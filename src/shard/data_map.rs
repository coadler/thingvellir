use std::hash::Hash;

use indexmap::map::Entry;
use indexmap::{IndexMap, IndexSet};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use smallvec::SmallVec;
use tokio::time::Instant;

use super::lru_candidates::LruCandidates;
use super::{PendingCallback, ShardConfig, TakeDataSender};
use crate::ServiceData;

type PendingCallbacks<Data> = SmallVec<[PendingCallback<Data>; 1]>;

enum InternalDataState<Key, Data> {
    // a SmallVec is used here, so we do not need to heap-allocate in the majority use-case,
    // which is there is going to be a load, and only one pending callback ever for it.
    Loading(PendingCallbacks<Data>, Option<TakeDataSender<Key, Data>>),
    Loaded(Data, u64),
}

pub(super) enum DataState<'a, 'b: 'a, Key: Eq + Hash + Clone, Data: ServiceData> {
    Loaded(DataGuard<'a, 'b, Key, Data>),
    MustLoad(Key, &'a mut PendingCallbacks<Data>),
    Loading(&'a mut PendingCallbacks<Data>),
}

impl<Key, Data> InternalDataState<Key, Data> {
    #[inline]
    fn is_loading(&self) -> bool {
        match self {
            InternalDataState::Loading(_, _) => true,
            InternalDataState::Loaded(_, _) => false,
        }
    }

    #[inline]
    fn is_loaded(&self) -> bool {
        match self {
            InternalDataState::Loaded(_, _) => true,
            InternalDataState::Loading(_, _) => false,
        }
    }

    fn into_pending_action(self) -> Option<PendingActions<Key, Data>> {
        match self {
            InternalDataState::Loaded(_, _) => None,
            InternalDataState::Loading(callbacks, take_data_sender) => Some(PendingActions {
                callbacks,
                take_data_sender,
            }),
        }
    }

    /// Gets a mutable reference to the pending callbacks vec.
    ///
    /// # Panics
    /// Panics if the internal data state is not loading.
    fn get_pending_callbacks_mut(&mut self) -> &mut PendingCallbacks<Data> {
        match self {
            InternalDataState::Loading(pending_callbacks, _) => pending_callbacks,
            InternalDataState::Loaded(_, _) => {
                panic!("invariant: `get_pending_callbacks_mut` called when data state is loaded.")
            }
        }
    }

    /// Gets a mutable reference to the data.
    ///
    /// # Panics
    /// Panics if the internal data state is not loaded.
    fn get_data_mut(&mut self) -> &mut Data {
        match self {
            InternalDataState::Loaded(data, _) => data,
            InternalDataState::Loading(_, _) => {
                panic!("invariant: `get_data_mut` called when data state is loading.")
            }
        }
    }
}

#[derive(Default)]
struct AccessOffset(u64);

impl AccessOffset {
    #[inline]
    fn next(&mut self) -> u64 {
        self.0 += 1;
        self.0
    }
}

pub(super) struct DataMap<Key, Data> {
    data: IndexMap<Key, InternalDataState<Key, Data>>,
    expiring_keys: IndexSet<Key>,
    lru_candidates: LruCandidates<Key>,
    lru_candidates_num_probes: u16,
    max_data_capacity: usize,
    rng: StdRng,
    access_offset: AccessOffset,
}

impl<Key: Hash + Eq + Clone, Data: ServiceData> DataMap<Key, Data> {
    pub(super) fn new(config: &ShardConfig) -> Self {
        let data = IndexMap::with_capacity(config.max_data_capacity);
        let expiring_keys = IndexSet::with_capacity(config.max_data_capacity);
        let lru_candidates = LruCandidates::default();

        Self {
            data,
            max_data_capacity: config.max_data_capacity,
            expiring_keys,
            lru_candidates,
            lru_candidates_num_probes: config.lru_candidates_num_probes,
            rng: StdRng::from_entropy(),
            access_offset: AccessOffset::default(),
        }
    }

    /// Checks if the data map is at capacity and will no longer accept any new keys.
    #[inline]
    pub(super) fn is_at_capacity(&self) -> bool {
        self.data.len() == self.max_data_capacity
    }

    /// Returns the number of keys stored by this data map, in the loading and loaded state.
    #[inline]
    pub(super) fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns the number of expiring keys stored by this data map.
    #[inline]
    pub(super) fn expiring_keys_len(&self) -> usize {
        self.expiring_keys.len()
    }

    /// Checks if the insertion of a given key would put the data map over capacity.
    #[inline]
    pub(super) fn would_exceed_capacity_if_inserted(&self, key: &Key) -> bool {
        self.is_at_capacity() && !self.data.contains_key(key)
    }

    /// Returns a DataGuard, that provides safe access to the underlying Data.
    ///
    /// Automatically will update LRU on access. Additionally, the expiring keys set is updated
    /// the guard is dropped and the data has been noticed to be mutated.
    #[inline]
    pub(super) fn get_loaded_data<'a, 'b: 'a>(
        &'a mut self,
        key: &'b Key,
    ) -> Option<DataGuard<'a, 'b, Key, Data>> {
        match self.data.get_mut(key) {
            Some(InternalDataState::Loaded(data, access_offset)) => {
                self.lru_candidates
                    .remove_candidate_by_access_offset(*access_offset);
                *access_offset = self.access_offset.next();
                Some(DataGuard {
                    data_ref: data,
                    key_ref: key,
                    prev_expires: PrevExpires::Unknown,
                    expiring_keys: &mut self.expiring_keys,
                })
            }
            _ => None,
        }
    }

    /// Removes loaded data from the main data map, returning None if the data is not loaded.
    ///
    /// # Panics
    ///
    /// Panics if the data is in the loading state.
    #[must_use = "you must do something with the loaded data."]
    pub(super) fn remove_loaded_data(&mut self, key: &Key) -> Option<(Key, Data)> {
        match self.data.swap_remove_full(key)? {
            (_, key, InternalDataState::Loaded(data, access_offset)) => {
                self.lru_candidates
                    .remove_candidate_by_access_offset(access_offset);

                if data.get_expires_at().is_some() {
                    self.expiring_keys.remove(&key);
                }

                Some((key, data))
            }

            (_, _, InternalDataState::Loading(_, _)) => {
                panic!("invariant: remove_loaded_data attempted to remove data which is loading.")
            }
        }
    }

    /// Probes the data-set, returning a key that is most likely to be the least recently used.
    ///
    /// Returns None if no keys are eligible for LRU. This can happen even if
    /// there is data in the map, because loading entries are not eligible
    /// for LRU eviction.
    pub(super) fn probe_and_take_best_lru_candidate(&mut self) -> Option<Key> {
        self.probe_lru_candidates();
        // Tries to grab the best candidate for eviction. This can be none, even after
        // update_lru_candidates has run, if for some weird reason no candidates
        // could be located, because most of the data is in the loading state,
        // or the data is simply empty.
        self.lru_candidates.take_best_candidate()
    }

    /// Probes the expiring key set, checking a random key to see if it has expired.
    ///
    /// If a key is returned, it must be removed, otherwise the state of the datamap will be inconsistent.
    ///
    /// Will return:
    ///   - `Err(ProbeEmpty)` if no probing is possible, because the expiring key set is empty.
    ///   - `Ok(None)` if the probe hit a key that did not expire (more probing is fine.)
    ///   - `Ok(Some(Key))` - if the probe hit a key that is expired (more probing is fine.)
    pub(super) fn probe_and_take_expiring_key(
        &mut self,
        now: &Instant,
    ) -> Result<Option<Key>, ProbeEmpty> {
        if self.expiring_keys.is_empty() {
            return Err(ProbeEmpty);
        }

        let idx = self.rng.gen_range(0, self.expiring_keys.len());
        let maybe_expiring_key = self
            .expiring_keys
            .get_index(idx)
            .expect("invariant: Tried to get_index on an invalid range.");

        let data = self
            .data
            .get(maybe_expiring_key)
            .expect("invariant: tried to get key that existed in `expiring_keys` but not `data`");

        let data = match data {
            // data is loading, and thus can't expire...
            InternalDataState::Loading(_, _) => return Ok(None),
            InternalDataState::Loaded(data, _) => data,
        };

        let expiry = match data.get_expires_at() {
            None => {
                panic!("invariant: a key was in `expiring_keys` that does not have an expiration.")
            }
            Some(expiry) => expiry,
        };

        if expiry > now {
            return Ok(None);
        }

        let expiring_key = self.expiring_keys
            .swap_remove_index(idx)
            .expect("invariant: tried to `swap_remove_index` on an index that no longer exists in `expiring_keys`");

        Ok(Some(expiring_key))
    }

    /// Swaps the Loading data for some Loaded data. Will return None if the data was already loaded,
    /// or if the data was not loaded. Will return a DataGuard containing the a reference to the swapped
    /// data, and also the pending actions that must be used.
    pub(super) fn swap_loaded_data<'a>(
        &'a mut self,
        key: Key,
        data: Data,
    ) -> Option<(DataGuard<'a, 'a, Key, Data>, PendingActions<Key, Data>)> {
        match self.data.get_full_mut(&key) {
            None | Some((_, _, InternalDataState::Loaded(_, _))) => None,
            Some((_idx, key_ref, entry @ InternalDataState::Loading(_, _))) => {
                let access_offset = self.access_offset.next();
                self.lru_candidates
                    .try_insert_candidate(key_ref, access_offset);

                let loading_data_state =
                    std::mem::replace(entry, InternalDataState::Loaded(data, access_offset));

                let pending_actions = loading_data_state
                    .into_pending_action()
                    .expect("invariant: data state was not loading");

                let data_ref = match entry {
                    InternalDataState::Loaded(data, _) => data,
                    // not reachable, because we just swapped it out.
                    InternalDataState::Loading(_, _) => unreachable!(),
                };

                let guard = DataGuard {
                    data_ref,
                    key_ref,
                    // This bit is important, as it will force a re-check once the guard is dropped,
                    // which will insert the key into expiring_keys, if the data has an expiry.
                    prev_expires: PrevExpires::Known(None),
                    expiring_keys: &mut self.expiring_keys,
                };

                Some((guard, pending_actions))
            }
        }
    }

    /// Removes the data state from the data map if it's in the Loading state, returning the pending actions, along with
    /// the owned key, if the caller needs it.
    ///
    /// If the data is in the loaded state, or does not exist in the map, None is returned, and the data map is not modified.
    pub(super) fn take_pending_actions(
        &mut self,
        key: Key,
    ) -> Option<PendingActionsWithKey<Key, Data>> {
        match self.data.entry(key) {
            Entry::Occupied(ent) if ent.get().is_loading() => {
                let (k, v) = ent.swap_remove_entry();
                Some(PendingActionsWithKey::new(k, v.into_pending_action()?))
            }
            _ => None,
        }
    }

    pub(super) fn get_or_insert_loaded_or_loading_data<F>(
        &mut self,
        key: Key,
        try_takeover: F,
    ) -> DataState<Key, Data>
    where
        F: FnOnce(&Key) -> Option<Data>,
    {
        match self.data.entry(key) {
            Entry::Vacant(ent) => {
                let maybe_data = (try_takeover)(ent.key());
                match maybe_data {
                    None => {
                        let key = ent.key().clone();
                        let data_state =
                            ent.insert(InternalDataState::Loading(SmallVec::new(), None));

                        DataState::MustLoad(key, data_state.get_pending_callbacks_mut())
                    }
                    Some(data) => {
                        let access_offset = self.access_offset.next();
                        let (key_ref, value) =
                            ent.insert_entry(InternalDataState::Loaded(data, access_offset));
                        self.lru_candidates
                            .try_insert_candidate(key_ref, access_offset);
                        DataState::Loaded(DataGuard {
                            data_ref: value.get_data_mut(),
                            key_ref,
                            // This bit is important, as it will force a re-check once the guard is dropped,
                            // which will insert the key into expiring_keys, if the data has an expiry.
                            prev_expires: PrevExpires::Known(None),
                            expiring_keys: &mut self.expiring_keys,
                        })
                    }
                }
            }
            Entry::Occupied(ent) => {
                let (key_ref, value) = ent.into_entry_mut();

                match value {
                    InternalDataState::Loading(callbacks, _) => DataState::Loading(callbacks),
                    InternalDataState::Loaded(data_ref, access_offset) => {
                        self.lru_candidates
                            .remove_candidate_by_access_offset(*access_offset);
                        *access_offset = self.access_offset.next();
                        DataState::Loaded(DataGuard {
                            data_ref,
                            key_ref,
                            // This bit is important, as it will force a re-check once the guard is dropped,
                            // which will insert the key into expiring_keys, if the data has an expiry.
                            prev_expires: PrevExpires::Known(None),
                            expiring_keys: &mut self.expiring_keys,
                        })
                    }
                }
            }
        }
    }

    pub(super) fn try_take(
        &mut self,
        key: Key,
        sender: TakeDataSender<Key, Data>,
    ) -> TryTakeResult<Key, Data> {
        let mut occupied_entry = match self.data.entry(key) {
            Entry::Vacant(ent) => return TryTakeResult::NotFound(ent.into_key(), sender),
            Entry::Occupied(ent) => ent,
        };

        match occupied_entry.get_mut() {
            InternalDataState::Loading(_pending_callbacks, existing_sender) => {
                if existing_sender.is_some() {
                    return TryTakeResult::TakeAlreadyEnqueued(sender);
                }

                // There is no enqueued take data sender, so we'll go and re-insert, with the result_tx, so when the data is loaded, we'll
                // have something to yank.
                *existing_sender = Some(sender);
                TryTakeResult::Enqueued
            }
            InternalDataState::Loaded(_, _) => {
                // Take ownership of the entry now that we know we're in the loaded state.
                let (key, data) = match occupied_entry.swap_remove_entry() {
                    (key, InternalDataState::Loaded(data, access_offset)) => {
                        // Remove from LRU candidates, and expiring keys.
                        self.lru_candidates
                            .remove_candidate_by_access_offset(access_offset);
                        if data.get_expires_at().is_some() {
                            self.expiring_keys.remove(&key);
                        }
                        (key, data)
                    }
                    // unreachable because we checked the data state's variant above.
                    _ => unreachable!(),
                };

                TryTakeResult::Taken(key, data, sender)
            }
        }
    }

    // Private methods:

    #[inline]
    fn probe_lru_candidates(&mut self) {
        if self.data.is_empty() {
            return;
        }

        for _ in 0..self.lru_candidates_num_probes {
            let index = self.rng.gen_range(0, self.data.len());
            match &mut self.data.get_index_mut(index) {
                Some((key, InternalDataState::Loaded(_data, access_offset))) => {
                    self.lru_candidates
                        .try_insert_candidate(&key, *access_offset);
                }
                Some(_) => {}
                None => break,
            };
        }
    }
}

enum PrevExpires {
    /// We have not checked the expiration status of the data.
    Unknown,
    /// We have checked the expiration status of the data, and it is now
    /// known.
    Known(Option<Instant>),
}

enum ExpirationChangeStatus {
    /// Expiration has not changed, no action required.
    Unchanged,
    /// Data is now expiring, which means we need to insert the key into the expiring keys.
    NowExpiring,
    /// Data is no longer expiring, which means we can remove it from the expiring keys.
    NoLongerExpiring,
}

impl PrevExpires {
    /// Updates the PrevExpires, setting to Known if previously Unknown. Call this before any expected
    /// mutation of the data might occur.
    #[inline]
    fn update_if_unknown<Data: ServiceData>(&mut self, data: &Data) {
        match self {
            PrevExpires::Unknown => {
                let _ = std::mem::replace(self, PrevExpires::Known(data.get_expires_at().cloned()));
            }
            PrevExpires::Known(_) => {}
        }
    }

    /// Gets the `ExpirationChangeStatus` of the given data. Returns whether the data's expiration
    /// status has remained unchanged, or needs to be inserted or removed from the expiring keys set.
    #[inline]
    fn get_change_status<Data: ServiceData>(&self, data: &Data) -> ExpirationChangeStatus {
        match self {
            PrevExpires::Unknown => ExpirationChangeStatus::Unchanged,
            PrevExpires::Known(prev_expires) => match (prev_expires, data.get_expires_at()) {
                (None, None) | (Some(_), Some(_)) => ExpirationChangeStatus::Unchanged,
                (Some(_), None) => ExpirationChangeStatus::NoLongerExpiring,
                (None, Some(_)) => ExpirationChangeStatus::NowExpiring,
            },
        }
    }
}

/// DataGuard holds a reference to the data, ensuring that all necessary state updates happen
/// once the data reference is dropped. Namely, this means updating the set of expiring keys
/// by checking to see if the data was mutated, and if it was, checking if the data's expiration
/// status has changed.
pub(super) struct DataGuard<'a, 'b: 'a, Key: Eq + Hash + Clone, Data: ServiceData> {
    key_ref: &'b Key,
    data_ref: &'a mut Data,
    expiring_keys: &'a mut IndexSet<Key>,
    prev_expires: PrevExpires,
}

impl<'a, 'b, Key: Eq + Hash + Clone, Data: ServiceData> DataGuard<'a, 'b, Key, Data> {
    #[inline]
    pub(super) fn as_ref(&self) -> &Data {
        self.data_ref
    }

    #[inline]
    pub(super) fn as_mut(&mut self) -> &mut Data {
        self.prev_expires.update_if_unknown(self.data_ref);
        self.data_ref
    }

    #[inline]
    pub(super) fn clone_key(&self) -> Key {
        self.key_ref.clone()
    }

    #[inline]
    pub(super) fn into_cloned_key(self) -> Key {
        self.clone_key()
    }
}

impl<'a, 'b, Key: Eq + Hash + Clone, Data: ServiceData> Drop for DataGuard<'a, 'b, Key, Data> {
    fn drop(&mut self) {
        match self.prev_expires.get_change_status(self.data_ref) {
            ExpirationChangeStatus::Unchanged => {}
            ExpirationChangeStatus::NoLongerExpiring => {
                self.expiring_keys.remove(self.key_ref);
            }
            ExpirationChangeStatus::NowExpiring => {
                self.expiring_keys.insert(self.clone_key());
            }
        }
    }
}

pub(super) enum TryTakeResult<Key, Data> {
    NotFound(Key, TakeDataSender<Key, Data>),
    Taken(Key, Data, TakeDataSender<Key, Data>),
    TakeAlreadyEnqueued(TakeDataSender<Key, Data>),
    Enqueued,
}

/// Returned as an error when no further probing work is possible. The probe
/// should not be called again until the data has potentially changed.
pub(super) struct ProbeEmpty;

pub(super) struct PendingActions<Key, Data> {
    pub(super) callbacks: PendingCallbacks<Data>,
    pub(super) take_data_sender: Option<TakeDataSender<Key, Data>>,
}

pub(super) struct PendingActionsWithKey<Key, Data> {
    pub(super) key: Key,
    pub(super) callbacks: PendingCallbacks<Data>,
    pub(super) take_data_sender: Option<TakeDataSender<Key, Data>>,
}

impl<Key, Data> PendingActionsWithKey<Key, Data> {
    fn new(key: Key, pending_actions: PendingActions<Key, Data>) -> Self {
        Self {
            key,
            callbacks: pending_actions.callbacks,
            take_data_sender: pending_actions.take_data_sender,
        }
    }
}
