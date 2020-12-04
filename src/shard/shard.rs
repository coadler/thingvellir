use std::fmt::Display;
use std::hash::Hash;

use log::{error, info, warn};

use tokio::sync::mpsc;
use tokio::time::{interval, Instant, Interval};

use super::commit_queue::{EnqueuedCommit, TakeOwnershipResult};
use super::{
    utils, AccumulatedCommitPolicy, DataMap, DataState, InternalMessage, PendingCallback,
    ServiceHandleMessage, ShardConfig, ShardStats, TakeDataSender, TakenData, TryTakeResult,
    UpstreamManager,
};
use crate::{LoadFromUpstream, ServiceData, ShardError, UpstreamError};

pub(super) struct Shard<Key, Data, Upstream> {
    running: bool,
    config: ShardConfig,

    shard_receiver: mpsc::Receiver<ServiceHandleMessage<Key, Data>>,

    internal_receiver: mpsc::UnboundedReceiver<InternalMessage<Key, Data>>,
    internal_sender: mpsc::UnboundedSender<InternalMessage<Key, Data>>,

    data_map: DataMap<Key, Data>,
    upstream_manager: UpstreamManager<Key, Data, Upstream>,
    stats: ShardStats,
    expedited_expiration_probe_enqueued: bool,
    cache_expiration_probe_interval: Interval,
}

impl<Key, Data, Upstream, MaybeMutableUpstream> Shard<Key, Data, MaybeMutableUpstream>
where
    Key: Clone + Hash + Eq + Display + Send + 'static,
    Data: ServiceData,
    Upstream: LoadFromUpstream<Key, Data>,
    MaybeMutableUpstream: super::marker::MaybeMutableUpstream<Key, Data, Immutable = Upstream>,
{
    pub(super) fn new(
        shard_receiver: mpsc::Receiver<ServiceHandleMessage<Key, Data>>,
        upstream: MaybeMutableUpstream,
        config: ShardConfig,
    ) -> Self {
        // we use an unbounded channel here, as we want the upstream implementation to be "fire and forget" which is to say,
        // the data loaded callback should not need to be awaited (which is the case with the regular Sender),
        // and we don't necessarily need back-pressure here, as the demand for loaded data originates from the external
        // shard message.
        let (internal_sender, internal_receiver) = mpsc::unbounded_channel();

        let cache_expiration_probe_interval = interval(config.cache_expiration_probe_interval);
        let data_map = DataMap::new(&config);
        let upstream_manager = UpstreamManager::new(internal_sender.clone(), upstream, &config);

        Self {
            config,
            shard_receiver,
            internal_receiver,
            internal_sender,
            cache_expiration_probe_interval,
            upstream_manager,
            data_map,
            stats: Default::default(),
            running: true,
            expedited_expiration_probe_enqueued: false,
        }
    }

    pub(super) async fn run(mut self) {
        let shard_id = self.config.shard_id;

        let _abort_on_panic =
            super::utils::AbortOnPanic::new(format!("shard {} panicked!", shard_id));

        info!("shard={} is starting", shard_id);

        if let Err(error) = self.shard_loop().await {
            eprintln!("Shard {} has failed with error: {:?}", shard_id, error);
            std::process::abort();
        }

        info!(
            "shard={} has stopped, flushing writes to storage.",
            shard_id
        );

        let persist_all_start = Instant::now();
        match self.persist_all().await {
            Ok(()) => {
                info!(
                    "shard={} has finished flushing, took {:?}.",
                    shard_id,
                    persist_all_start.elapsed()
                );
            }
            Err(err) => {
                error!(
                    "shard={} has failed to persist all with error: {:?}",
                    shard_id, err
                );
            }
        }
    }

    async fn shard_loop(&mut self) -> Result<(), anyhow::Error> {
        use tokio::stream::StreamExt;

        while self.running {
            // todo: some notes on stopping:
            // - we need to make sure that we stop handling any shard messages,
            //   but we need to continue to handle messages on the internal
            //   receiver, until all the send ends have been closed.
            //   (the tricky part here, however, is that we also hold an internal sender,
            //    which means it wont close, so we need to do ... something, about that?!)
            tokio::select! {
                Some(shard_message) = self.shard_receiver.next() => {
                    self.handle_shard_message(shard_message);
                }
                Some(internal_message) = self.internal_receiver.next() => {
                    self.handle_internal_message(internal_message)
                }
                _interval = self.cache_expiration_probe_interval.tick(), if !self.expedited_expiration_probe_enqueued => {
                    self.probe_expired_entries();
                }
                Ok(data_ref) = self.upstream_manager.poll_commit_queue_ready() => {
                    self.handle_commit_data(data_ref);
                }
                else => break,
            }
        }

        Ok(())
    }

    #[inline]
    fn handle_commit_data(&mut self, enqueued_commit: EnqueuedCommit<Key, Data>) {
        let (key, mut data) = enqueued_commit.into_inner();
        match &mut data {
            Some(data) => self.upstream_manager.do_commit_data(key, data),
            None => {
                let mut guard = self
                    .data_map
                    .get_loaded_data(&key)
                    .expect("invariant: referenced key from persist queue does not exist.");

                self.upstream_manager
                    .do_commit_data(key.clone(), guard.as_mut())
            }
        };
    }

    #[inline]
    fn handle_internal_message(&mut self, message: InternalMessage<Key, Data>) {
        match message {
            InternalMessage::DataLoadResult(key, result) => match result {
                Ok(data) => self.handle_data_load_result_success(key, data),
                Err(error) => self.handle_data_load_result_error(key, error.into()),
            },
            InternalMessage::DataCommitResult(key, result) => match result {
                Ok(()) => self.handle_data_commit_result_success(key),
                Err(error) => self.handle_data_commit_result_error(key, error),
            },
            InternalMessage::DoExpeditedExpirationProbe => {
                self.probe_expired_entries();
            }
        }
    }

    #[inline]
    fn handle_shard_message(&mut self, message: ServiceHandleMessage<Key, Data>) {
        match message {
            ServiceHandleMessage::Execute(key, func) => {
                self.execute_or_enqueue_load(key, PendingCallback::Execute(func), false);
            }
            ServiceHandleMessage::ExecuteMut(key, func) => {
                self.execute_or_enqueue_load(key, PendingCallback::ExecuteMut(func), false);
            }
            ServiceHandleMessage::ExecuteIfCached(key, func) => {
                self.execute_if_cached(key, func);
            }
            ServiceHandleMessage::GetStats(result_tx) => {
                self.stats.data_size = self.data_map.len();
                self.stats.expiring_keys = self.data_map.expiring_keys_len();
                result_tx.send(self.stats.clone()).ok();
            }
            ServiceHandleMessage::TakeData(key, result_tx) => {
                self.take_data(key, result_tx);
            }
            ServiceHandleMessage::Stop => {
                self.running = false;
                warn!("shard {} is stopping", self.config.shard_id);
            }
        }
    }

    #[inline]
    fn execute_if_cached(&mut self, key: Key, func: Box<dyn FnOnce(Option<&Data>) + Send>) {
        let expired = {
            let guard = self.data_map.get_loaded_data(&key);
            match guard {
                None => {
                    (func)(None);
                    None
                }
                Some(guard) => {
                    if utils::is_expired(guard.as_ref()) {
                        Some((guard.into_cloned_key(), func))
                    } else {
                        (func)(Some(guard.as_ref()));
                        None
                    }
                }
            }
        };

        if let Some((key, func)) = expired {
            self.evict_key(&key, EvictionReason::Ttl);
            self.upstream_manager.cancel_commit(&key);
            return self.execute_if_cached(key, func);
        }
    }

    fn handle_data_load_result_success(&mut self, key: Key, data: Data) {
        if utils::is_expired(&data) {
            self.handle_data_load_result_error(key, ShardError::DataImmediatelyExpired);
            return;
        }

        let (mut guard, pending_actions) = match self.data_map.swap_loaded_data(key, data) {
            // The demand for the data is gone? Wat.
            None => return,
            Some((guard, pending_actions)) => (guard, pending_actions),
        };

        let pending_executions_completed = pending_actions.callbacks.len();
        let default_commit_policy = self.config.default_commit_policy;

        let accumulated_commit_policy = pending_actions.callbacks.into_iter().fold(
            AccumulatedCommitPolicy::new(),
            |acc, pending_callback| {
                acc.accumulate(pending_callback.resolve(&mut guard, default_commit_policy))
            },
        );

        self.stats.record_load_complete(Ok(()));
        self.stats
            .record_pending_executions_completed(pending_executions_completed);

        // We have finished processing the data load, and performed all pending callbacks,
        // do we have a request to now take ownership of the data elsewhere? if so, let's
        // go ahead and respect that request.
        if let Some(take_data_sender) = pending_actions.take_data_sender {
            let key = guard.into_cloned_key();
            self.take_data(key, take_data_sender);
        } else if accumulated_commit_policy.did_mutate() {
            // todo: use the accumulated commit policy to act with delay.
            self.upstream_manager
                .enqueue_persist(&guard.into_cloned_key(), Instant::now());
        }
    }

    fn handle_data_load_result_error(&mut self, key: Key, error: ShardError) {
        let pending_actions = match self.data_map.take_pending_actions(key) {
            None => return,
            Some(pending_actions) => pending_actions,
        };

        let pending_executions_completed = pending_actions.callbacks.len();
        for pending_callback in pending_actions.callbacks {
            pending_callback.reject(error.clone(), self.config.default_commit_policy);
        }

        self.stats.record_load_complete(Err(&error));
        self.stats
            .record_pending_executions_completed(pending_executions_completed);

        // If the load failed, there's nothing to take.
        if let Some(take_data_sender) = pending_actions.take_data_sender {
            take_data_sender.send(None).ok();
        }
    }

    fn handle_data_commit_result_success(&mut self, key: Key) {
        self.upstream_manager.temp_mark_commit_complete(key);
        // todo!()
    }

    fn handle_data_commit_result_error(&mut self, key: Key, error: UpstreamError) {
        self.upstream_manager.temp_mark_commit_complete(key);
        // todo!()
    }

    #[inline]
    fn execute_or_enqueue_load(
        &mut self,
        key: Key,
        pending_callback: PendingCallback<Data>,
        is_expire_retry: bool,
    ) {
        // If we are at capacity, and we are unable to evict, and this isn't a known key,
        // we will reject the request. Future executions (once loads are complete),
        // should allow us to evict items.
        if self.data_map.would_exceed_capacity_if_inserted(&key) && !self.evict_lru() {
            pending_callback.reject(
                ShardError::ShardAtCapacity,
                self.config.default_commit_policy,
            );
            return;
        }

        let key_is_expired = {
            let upstream_manager = &mut self.upstream_manager;
            let data_state = self.data_map
                .get_or_insert_loaded_or_loading_data(key, |key_ref| {
                    let take_ownership_result =
                        upstream_manager.try_take_ownership_from_commit_queue(key_ref);

                    match take_ownership_result {
                        // The persist queue did not own the, we need to schedule it to be fetched from upstream.
                        TakeOwnershipResult::NotEnqueued => None,
                        // We have successfully taken the ownership of the data, so insert it into the
                        // data map.
                        TakeOwnershipResult::Transferred(data) => Some(data),
                        TakeOwnershipResult::NotOwned => panic!(
                            "invariant: tried to relinquish ownership of key that was already in the hash-map"
                        ),
                    }
                });

            match data_state {
                DataState::MustLoad(key, pending_callbacks) => {
                    pending_callbacks.push(pending_callback);
                    self.upstream_manager.do_load_data(key);

                    self.stats.loads_in_progress += 1;
                    self.stats.executions_pending += 1;
                    None
                }

                DataState::Loading(pending_callbacks) => {
                    pending_callbacks.push(pending_callback);
                    self.stats.executions_pending += 1;
                    None
                }

                DataState::Loaded(mut guard) => {
                    if utils::is_expired(guard.as_ref()) {
                        if is_expire_retry {
                            pending_callback.reject(
                                ShardError::DataImmediatelyExpired,
                                self.config.default_commit_policy,
                            );
                            None
                        } else {
                            Some((guard.into_cloned_key(), pending_callback))
                        }
                    } else {
                        let accumulated_commit_policy: AccumulatedCommitPolicy = pending_callback
                            .resolve(&mut guard, self.config.default_commit_policy)
                            .into();

                        if accumulated_commit_policy.did_mutate() {
                            // todo: use the accumulated commit policy to act with delay.
                            self.upstream_manager
                                .enqueue_persist(&guard.into_cloned_key(), Instant::now());
                        }
                        // todo: act upon the accumulated commit policy.
                        self.stats.executions_complete += 1;
                        None
                    }
                }
            }
        };

        // If the key is expired, we'll retry this operation, first evicting the key,
        // and then re-trying the execute enqueue or load.
        if let Some((key, pending_callback)) = key_is_expired {
            self.evict_key(&key, EvictionReason::Ttl);
            // If we're expired, let's go ahead and cancel the commit as well. This will prevent us from getting into a race.
            self.upstream_manager.cancel_commit(&key);
            self.execute_or_enqueue_load(key, pending_callback, true);
        }
    }

    fn take_data(&mut self, key: Key, sender: TakeDataSender<Key, Data>) {
        match self.data_map.try_take(key, sender) {
            TryTakeResult::NotFound(key, sender) => {
                let taken_data = self
                    .upstream_manager
                    .cancel_and_take_data_from_commit_queue(&key);

                if taken_data.is_some() {
                    self.stats.keys_taken += 1;
                }

                sender.send(taken_data).ok();
            }
            TryTakeResult::Taken(key, data, sender) => {
                // big todo: if we are persisting data due to an immediate write commit policy,
                // we need to ensure that take doesn't move the data before it's persisted.
                let was_enqueued_at = self
                    .upstream_manager
                    .cancel_commit(&key)
                    .map(|data| data.at());

                let taken_data = TakenData {
                    key,
                    data,
                    was_enqueued_at,
                };
                self.stats.keys_taken += 1;
                sender.send(Some(taken_data)).ok();
            }
            TryTakeResult::TakeAlreadyEnqueued(sender) => {
                // We already have an existing sender, so return `None` here.
                // todo: maybe an AlreadyTakenError?? none is fine for now tho.
                sender.send(None).ok();
            }
            TryTakeResult::Enqueued => {}
        }
    }

    /// Evicts what is "probably" the least recently used data from storage.
    ///
    /// Returns true if data was able to be successfully evicted, otherwise, false.
    fn evict_lru(&mut self) -> bool {
        match self.data_map.probe_and_take_best_lru_candidate() {
            Some(key_to_evict) => {
                self.evict_key(&key_to_evict, EvictionReason::Lru);
                true
            }
            None => false,
        }
    }

    fn evict_key(&mut self, key_to_evict: &Key, eviction_reason: EvictionReason) {
        let (key, data) = self
            .data_map
            .remove_loaded_data(key_to_evict)
            .expect("evict_key tried to evict a key that does not exist.");

        self.upstream_manager
            .give_ownership_to_commit_queue_if_enqueued(key, data);

        match eviction_reason {
            EvictionReason::Ttl => {
                self.stats.keys_ttl_evicted += 1;
            }
            EvictionReason::Lru => {
                self.stats.keys_lru_evicted += 1;
            }
        }
    }

    /// Probes the expiring keys to see if any have expired. We do this rather than maintain a rather (costly) delay queue of all the
    /// expiring keys, applying a probabilistic approach to key expiry.
    ///
    /// Every time this probe is called, we look at a random sample of N many keys within the expiring keys set and expire them. If the
    /// number of keys expired during the probe (X) exceed 25% of N, we immediately schedule another probe, until the % of expired keys
    /// is under 25% per probe, then we go back to probing at the regular tick interval.
    fn probe_expired_entries(&mut self) {
        let mut num_expired_keys = 0;
        let now = Instant::now();

        for _ in 0..self.config.cache_expiration_probe_keys_per_tick {
            match self.data_map.probe_and_take_expiring_key(&now) {
                // There are no keys to evict, so we have nothing left to do.
                Err(_probe_empty) => break,
                // The probe returned nothing.
                Ok(None) => continue,
                Ok(Some(expiring_key)) => {
                    self.evict_key(&expiring_key, EvictionReason::Ttl);
                    num_expired_keys += 1;
                }
            }
        }

        // Did we expire enough keys to make us enter the expedited loop?
        // The thought here is that if we've expired over 25% of the keys when we probed, we should expedite the next tick, hopefully,
        // to the next tick of the event loop for this shard, going until the number of expired keys is probably under 25%. Then we'll
        // go back to probing at the configured probe interval. We do this instead of looping, so we can avoid "blocking" the event
        // loop processing expiration probes for too long.
        let expedited_expiration_threshold = self.config.cache_expiration_probe_keys_per_tick / 4;
        self.expedited_expiration_probe_enqueued =
            num_expired_keys >= expedited_expiration_threshold;

        self.stats.expiration_probes_ran += 1;
        if self.expedited_expiration_probe_enqueued {
            self.stats.expiration_probes_expedited += 1;
            self.internal_sender
                .send(InternalMessage::DoExpeditedExpirationProbe)
                .ok();
        }
    }

    async fn persist_all(self) -> Result<(), ShardError> {
        // let (upstream, persist_queue) = match self.commit_queue {
        //     None => return Ok(()),
        //     Some(persist_queue) => (self.upstream, persist_queue),
        // };
        // let cancelled_commits = persist_queue.consume();

        todo!();
    }
}

enum EvictionReason {
    Lru,
    Ttl,
}
