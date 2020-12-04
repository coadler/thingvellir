use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::mem::replace;
use std::task::{Context, Poll};

use futures::future::poll_fn;
use tokio::time::delay_queue::Key as DelayQueueKey;
use tokio::time::{DelayQueue, Error, Instant};

/// A more compact form of `EnqueuedPersist`, not holding the key, as it's already held in
/// `PersistQueue.delays_by_key`.
enum InternalDataRef<Data> {
    // Data is boxed here, as in the 99% case, we will be using `References`, and
    // the size of the `EnqueuedPersist` should not be dependent on `Data`, this means that
    // if we need to own the data, we're okay spilling out onto the heap.
    Owned(Box<Data>),
    // We'll refer to the value stored within the service shard's hashmap.
    References,
}

/// Contains a either a reference to the data or the data.
///
/// The reference here is the `Key` that the data would be stored at within the
/// shard's hash-map.
pub(crate) enum EnqueuedCommit<Key, Data> {
    /// The data is owned by this DataRef.
    Owned(Key, Data, Instant),
    /// The data is stored at a given `Key` within the shard's hash-map.
    References(Key, Instant),
}

impl<Key, Data> EnqueuedCommit<Key, Data> {
    pub fn at(&self) -> Instant {
        match self {
            EnqueuedCommit::Owned(_, _, at) => *at,
            EnqueuedCommit::References(_, at) => *at,
        }
    }

    pub fn key(&self) -> &Key {
        match self {
            EnqueuedCommit::Owned(key, _, _) => key,
            EnqueuedCommit::References(key, _) => key,
        }
    }

    pub fn into_key(self) -> Key {
        match self {
            EnqueuedCommit::Owned(key, _, _) => key,
            EnqueuedCommit::References(key, _) => key,
        }
    }

    pub fn data(&self) -> Option<&Data> {
        match self {
            EnqueuedCommit::Owned(_, data, _) => Some(data),
            EnqueuedCommit::References(_, _) => None,
        }
    }

    pub fn into_data(self) -> Option<Data> {
        match self {
            EnqueuedCommit::Owned(_, data, _) => Some(data),
            EnqueuedCommit::References(_, _) => None,
        }
    }

    pub fn into_inner(self) -> (Key, Option<Data>) {
        match self {
            EnqueuedCommit::Owned(key, data, _) => (key, Some(data)),
            EnqueuedCommit::References(key, _) => (key, None),
        }
    }
}

struct InternalEnqueuedPersist<Data> {
    delay_queue_key: DelayQueueKey,
    internal_data_ref: InternalDataRef<Data>,
    at: Instant,
}

impl<Data> InternalEnqueuedPersist<Data> {
    #[inline]
    fn into_enqueued_persist<Key>(self, key: Key) -> EnqueuedCommit<Key, Data> {
        match self.internal_data_ref {
            InternalDataRef::References => EnqueuedCommit::References(key, self.at),
            InternalDataRef::Owned(data) => EnqueuedCommit::Owned(key, *data, self.at),
        }
    }
}

/// A `PersistQueue` is responsible for holding when a given piece of
/// data should be persisted to storage.
pub(crate) struct CommitQueue<Key, Data> {
    delay_queue: DelayQueue<Key>,
    delays_by_key: HashMap<Key, InternalEnqueuedPersist<Data>>,
}

/// Returned by `PersistQueue.enqueue_at`, describing operation
/// the enqueue decided to take.
pub(crate) enum PersistResult {
    /// The key was enqueued for persisting for the first time.
    Enqueued,
    /// The queue was re-enqueued for persisting sooner, as the
    /// deadline would not be met with the current enqueued persist.
    ReEnqueued(Instant),
    /// The current enqueued persist would meet the deadline requirement.
    AlreadyEnqueued(Instant),
}

pub(crate) enum GiveOwnershipResult<Key, Data> {
    /// The key is not enqueued.
    NotEnqueued(Key, Data),
    /// The inner data was already owned data. The data provided to the promote
    /// function was discarded.
    AlreadyOwned(Key, Data),
    /// The inner data has been promoted to owned data.
    Transferred,
}

pub(crate) enum TakeOwnershipResult<Data> {
    /// The key is not enqueued.
    NotEnqueued,
    /// The data has been demoted to a reference.
    Transferred(Data),
    /// The data was already demoted, we don't have the owned data here.
    NotOwned,
}

impl<Key: Clone + Hash + Eq, Data> CommitQueue<Key, Data> {
    /// Constructs a new persist queue, with no pre-allocated capacity. This will not cause
    /// a heap allocation until the commit queue has data enqueued.
    pub(crate) fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Constructs a persist queue with a given capacity, this will heap allocate
    /// such that `capacity` items can be enqueued before a re-allocation is required.
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            delay_queue: DelayQueue::with_capacity(capacity),
            delays_by_key: HashMap::with_capacity(capacity),
        }
    }

    /// Returns how many items are enqueued for a persist.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.delay_queue.len()
    }

    /// Returns when the given key is scheduled to be persisted, or None if it is not enqueued.
    #[inline]
    pub(crate) fn get_persist_at(&self, key: &Key) -> Option<Instant> {
        self.delays_by_key
            .get(key)
            .map(|enqueued_persist| enqueued_persist.at)
    }

    pub(crate) fn persist_at(&mut self, key: &Key, deadline: Instant) -> PersistResult {
        // Usage of `get_mut` here over `entry` is deliberate. The thought being that
        // most of the time we will be trying to enqueue already queued data, so we can
        // avoid cloning the `key`.
        match self.delays_by_key.get_mut(key) {
            // We are requesting a more "timely" persist, so we must enqueue
            // ourselves sooner.
            Some(internal_enqueued_persist) if deadline < internal_enqueued_persist.at => {
                self.delay_queue
                    .reset_at(&internal_enqueued_persist.delay_queue_key, deadline);
                let prev_deadline = internal_enqueued_persist.at;
                internal_enqueued_persist.at = deadline;

                PersistResult::ReEnqueued(prev_deadline)
            }
            // The persist is enqueued sooner than what we requested, which means we don't have
            // to do anything.
            Some(internal_enqueued_persist) => {
                PersistResult::AlreadyEnqueued(internal_enqueued_persist.at)
            }
            // The persist for this key is not yet enqueued, so let's enqueue it.
            None => {
                let delay_queue_key = self.delay_queue.insert_at(key.clone(), deadline);
                self.delays_by_key.insert(
                    key.clone(),
                    InternalEnqueuedPersist {
                        at: deadline,
                        delay_queue_key,
                        internal_data_ref: InternalDataRef::References,
                    },
                );
                PersistResult::Enqueued
            }
        }
    }

    /// Given a key and some data, if enqueued for persist, give ownership of the Data to the persist queue.
    ///
    /// The use here is if we are about to drop an item from the main hash map, and we want to ensure
    /// the data lives long enough to be persisted, we will stash it away here, so when it's time to
    /// persist, we'll have the data.
    pub(crate) fn give_ownership(
        &mut self,
        key: Key,
        data: Data,
    ) -> GiveOwnershipResult<Key, Data> {
        if let Some(enqueued_persist) = self.delays_by_key.get_mut(&key) {
            match &mut enqueued_persist.internal_data_ref {
                references @ InternalDataRef::References => {
                    *references = InternalDataRef::Owned(Box::new(data));
                    GiveOwnershipResult::Transferred
                }
                InternalDataRef::Owned(_) => GiveOwnershipResult::AlreadyOwned(key, data),
            }
        } else {
            GiveOwnershipResult::NotEnqueued(key, data)
        }
    }

    /// The inverse of `give_ownership`.
    ///
    /// Given a key that is enqueued for persist, relinquish ownership from the `DataRef`.
    /// of the data. It's assumed that the data will then be moved into the main hash-map within the shard,
    /// so that when the persist delay expires, the data will be able to be looked up from there.
    pub(crate) fn take_ownership(&mut self, key: &Key) -> TakeOwnershipResult<Data> {
        if let Some(enqueued_persist) = self.delays_by_key.get_mut(&key) {
            let prev_data_ref = replace(
                &mut enqueued_persist.internal_data_ref,
                InternalDataRef::References,
            );
            match prev_data_ref {
                InternalDataRef::Owned(data) => TakeOwnershipResult::Transferred(*data),
                InternalDataRef::References => TakeOwnershipResult::NotOwned,
            }
        } else {
            TakeOwnershipResult::NotEnqueued
        }
    }

    /// Given a `key`, cancel the persist, and return the enqueued persist, containing the data
    /// either by reference, or owned, and when the data was scheduled to be committed.
    ///
    /// If the persist queue owns the data for the key, this will relinquish ownership of the data.
    pub(crate) fn cancel(&mut self, key: &Key) -> Option<EnqueuedCommit<Key, Data>> {
        match self.delays_by_key.remove_entry(key) {
            None => None,
            Some((key, internal_enqueued_persist)) => {
                self.delay_queue
                    .remove(&internal_enqueued_persist.delay_queue_key);
                Some(internal_enqueued_persist.into_enqueued_persist(key))
            }
        }
    }

    /// Given a key, demotes the data from being owned, returning it.
    /// Polls the persist queue, returning a DataRef for data that should be persisted.
    ///
    /// Will return Ready(Result) if there is more data that is ready to be persisted, or NotReady
    /// if nothing in the queue is ready for persisting.
    #[inline]
    pub(crate) fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<EnqueuedCommit<Key, Data>, Error>> {
        match self.delay_queue.poll_expired(cx) {
            Poll::Ready(Some(entry)) => {
                let key = entry?.into_inner();
                let internal_enqueued_persist = self.delays_by_key.remove(&key).expect(
                    "data inconsistency: poll_ready returned a key not contained in delays_by_key",
                );

                Poll::Ready(Ok(internal_enqueued_persist.into_enqueued_persist(key)))
            }
            Poll::Ready(None) => Poll::Pending,
            Poll::Pending => Poll::Pending,
        }
    }

    /// Returns a future that will resolve when the next item in the queue is ready to be persisted.
    #[inline]
    pub(crate) fn next<'a>(
        &'a mut self,
    ) -> impl Future<Output = Result<EnqueuedCommit<Key, Data>, Error>> + 'a {
        poll_fn(move |cx| self.poll_ready(cx))
    }

    /// Consumes the persist queue, returning all persists that have been scheduled as
    /// `EnqueuedPersist`s. The returned vector is in no particular order, and it's
    /// up to the caller to sort it as they please.
    pub(crate) fn consume(self) -> Vec<EnqueuedCommit<Key, Data>> {
        self.delays_by_key
            .into_iter()
            .map(|(key, internal_enqueued_persist)| {
                internal_enqueued_persist.into_enqueued_persist(key)
            })
            .collect()
    }
}

#[inline]
pub(crate) fn poll_option_ready<'a, Key: Clone + Hash + Eq, Data>(
    mut commit_queue: Option<&'a mut CommitQueue<Key, Data>>,
) -> impl Future<Output = Result<EnqueuedCommit<Key, Data>, Error>> + 'a {
    poll_fn(move |cx| {
        commit_queue
            .as_mut()
            .map(|queue| queue.poll_ready(cx))
            .unwrap_or(Poll::Pending)
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio::time::{Duration, Instant};
    struct Data;

    #[tokio::test]
    async fn test_persist_queue_persist_at() {
        let mut queue: CommitQueue<u32, Data> = CommitQueue::new();
        let now = Instant::now();

        match queue.persist_at(&1, now + Duration::from_secs(2)) {
            PersistResult::Enqueued => {}
            _ => unreachable!(),
        }
        assert_eq!(queue.get_persist_at(&1), Some(now + Duration::from_secs(2)));

        match queue.persist_at(&1, now + Duration::from_secs(3)) {
            PersistResult::AlreadyEnqueued(when) => {
                assert_eq!(when, now + Duration::from_secs(2));
            }
            _ => unreachable!(),
        }
        assert_eq!(queue.get_persist_at(&1), Some(now + Duration::from_secs(2)));

        match queue.persist_at(&1, now + Duration::from_secs(1)) {
            PersistResult::ReEnqueued(when) => {
                assert_eq!(when, now + Duration::from_secs(2));
            }
            _ => unreachable!(),
        }
        assert_eq!(queue.get_persist_at(&1), Some(now + Duration::from_secs(1)));
        assert_eq!(queue.len(), 1);

        let data_ref = queue.cancel(&1).unwrap();
        assert_eq!(data_ref.at(), now + Duration::from_secs(1));
        assert!(queue.cancel(&1).is_none());
        assert!(queue.get_persist_at(&1).is_none());
        assert_eq!(queue.len(), 0);
    }

    #[tokio::test]
    async fn test_next() {
        let mut queue: CommitQueue<u32, Data> = CommitQueue::new();
        let now = Instant::now();
        match queue.persist_at(&1, now + Duration::from_millis(50)) {
            PersistResult::Enqueued => {}
            _ => unreachable!(),
        }
        match queue.persist_at(&3, now + Duration::from_millis(150)) {
            PersistResult::Enqueued => {}
            _ => unreachable!(),
        }
        match queue.persist_at(&2, now + Duration::from_millis(100)) {
            PersistResult::Enqueued => {}
            _ => unreachable!(),
        }

        assert_eq!(queue.len(), 3);
        assert_eq!(queue.next().await.unwrap().into_key(), 1);
        assert_eq!(queue.len(), 2);
        assert_eq!(queue.next().await.unwrap().into_key(), 2);
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.next().await.unwrap().into_key(), 3);
        assert_eq!(queue.len(), 0);

        let mut task = tokio_test::task::spawn(queue.next());
        match task.poll() {
            std::task::Poll::Pending => {}
            _ => panic!("poll was ready?"),
        };
    }

    #[tokio::test]
    async fn test_consume() {
        let mut queue: CommitQueue<u32, Data> = CommitQueue::new();
        let now = Instant::now();
        match queue.persist_at(&1, now + Duration::from_millis(50)) {
            PersistResult::Enqueued => {}
            _ => unreachable!(),
        }
        match queue.persist_at(&3, now + Duration::from_millis(150)) {
            PersistResult::Enqueued => {}
            _ => unreachable!(),
        }
        match queue.persist_at(&2, now + Duration::from_millis(100)) {
            PersistResult::Enqueued => {}
            _ => unreachable!(),
        }

        let mut consumed = queue.consume();
        assert_eq!(consumed.len(), 3);
        consumed.sort_by_key(|k| k.at().into_std());
        assert_eq!(
            consumed.iter().map(|k| k.at()).collect::<Vec<_>>(),
            vec![
                now + Duration::from_millis(50),
                now + Duration::from_millis(100),
                now + Duration::from_millis(150)
            ]
        );
        assert_eq!(
            consumed
                .into_iter()
                .map(|k| k.into_key())
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    #[tokio::test]
    async fn test_ownership() {
        let mut queue: CommitQueue<u32, Data> = CommitQueue::new();
        // Can't give ownership if the data isn't enqueued.
        match queue.give_ownership(1, Data {}) {
            GiveOwnershipResult::NotEnqueued(_, _) => {}
            _ => unreachable!(),
        }

        // Can't take ownership if the data isn't enqueued.
        match queue.take_ownership(&1) {
            TakeOwnershipResult::NotEnqueued => {}
            _ => unreachable!(),
        }

        // We now are scheduling a persist.
        let now = Instant::now();
        match queue.persist_at(&1, now + Duration::from_millis(50)) {
            PersistResult::Enqueued => {}
            _ => unreachable!(),
        }

        // Can't take ownership, the data isn't owned.
        match queue.take_ownership(&1) {
            TakeOwnershipResult::NotOwned => {}
            _ => unreachable!(),
        }

        // Give ownership should succeed.
        match queue.give_ownership(1, Data {}) {
            GiveOwnershipResult::Transferred => {}
            _ => unreachable!(),
        }

        // We already own the data for that key?!
        match queue.give_ownership(1, Data {}) {
            GiveOwnershipResult::AlreadyOwned(_, _) => {}
            _ => unreachable!(),
        }

        // First take succeeds.
        match queue.take_ownership(&1) {
            TakeOwnershipResult::Transferred(_) => {}
            _ => unreachable!(),
        }

        // Second take does not.
        match queue.take_ownership(&1) {
            TakeOwnershipResult::NotOwned => {}
            _ => unreachable!(),
        }

        // Take the item, no data should be present.
        let item = queue.next().await.unwrap();
        assert_eq!(item.key(), &1);
        assert!(item.data().is_none());

        // - Enqueue another item, and give ownership.
        match queue.persist_at(&2, now + Duration::from_millis(50)) {
            PersistResult::Enqueued => {}
            _ => unreachable!(),
        }

        // Give ownership should succeed.
        match queue.give_ownership(2, Data {}) {
            GiveOwnershipResult::Transferred => {}
            _ => unreachable!(),
        }

        // Take the item, data should be present.
        let item = queue.next().await.unwrap();
        assert_eq!(item.key(), &2);
        assert!(item.data().is_some());
    }
}
