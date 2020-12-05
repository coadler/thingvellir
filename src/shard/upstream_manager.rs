use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;

use smallvec::SmallVec;

use tokio::sync::mpsc;
use tokio::time::Instant;

use super::commit_queue::{
    poll_option_ready, CommitQueue, EnqueuedCommit, PersistResult, TakeOwnershipResult,
};
use super::{ImmediateCommitWaiters, InternalMessage, ShardConfig, TakeDataSender, TakenData};
use crate::{CommitToUpstream, DataCommitRequest, DataLoadRequest, LoadFromUpstream};

pub(super) struct UpstreamManager<Key, Data, Upstream> {
    upstream: Upstream,
    internal_sender: mpsc::UnboundedSender<InternalMessage<Key, Data>>,
    in_progress_commits: HashMap<Key, InProgressCommit<Key, Data>>,
    commit_queue: Option<CommitQueue<Key, Data>>,
}

impl<
    Key: Hash + Eq + Clone,
    Data,
    Upstream: LoadFromUpstream<Key, Data>,
    MaybeMutableUpstream: super::marker::MaybeMutableUpstream<Key, Data, Immutable = Upstream>,
> UpstreamManager<Key, Data, MaybeMutableUpstream>
{
    pub(super) fn new(
        internal_sender: mpsc::UnboundedSender<InternalMessage<Key, Data>>,
        mut upstream: MaybeMutableUpstream,
        config: &ShardConfig,
    ) -> Self {
        // todo: should we have this be a config option? the thought is to pre-size this large enough to avoid
        // re-allocations, but small enough so it's memory footprint is irrelevant relative to the rest of the
        // shard.
        let in_progress_commits =
            HashMap::with_capacity(std::cmp::min(config.max_data_capacity, 1 << 14));
        let commit_queue = if upstream.as_mutable().is_some() {
            Some(CommitQueue::with_capacity(config.persist_queue_capacity))
        } else {
            None
        };

        Self {
            upstream,
            internal_sender,
            in_progress_commits,
            commit_queue,
        }
    }

    pub(super) fn try_take_ownership_from_commit_queue(
        &mut self,
        key: &Key,
    ) -> TakeOwnershipResult<Data> {
        self.commit_queue
            .as_mut()
            .map(|queue| queue.take_ownership(key))
            .unwrap_or(TakeOwnershipResult::NotEnqueued)
    }

    /// Enqueues the persist operation for a given `key`, at the given `deadline`
    pub(super) fn enqueue_persist(
        &mut self,
        key: &Key,
        deadline: Instant,
    ) -> Option<PersistResult> {
        self.commit_queue
            .as_mut()
            .map(|queue| queue.persist_at(key, deadline))
    }

    /// Given an owned key + data, give ownership of that to the commit queue, only if the commit for a given key
    /// is enqueued. Otherwise the key and data are dropped.
    #[inline]
    pub(super) fn give_ownership_to_commit_queue_if_enqueued(&mut self, key: Key, data: Data) {
        if let Some(persist_queue) = &mut self.commit_queue {
            persist_queue.give_ownership(key, data);
        }
    }

    /// Cancels a given commit from the commit queue. If the upstream manager is running in immutable
    /// mode this will always return None.
    pub(super) fn cancel_commit(&mut self, key: &Key) -> Option<EnqueuedCommit<Key, Data>> {
        self.commit_queue
            .as_mut()
            .and_then(|queue| queue.cancel(&key))
    }

    /// Cancels a commit, and takes data from the commit queue.
    ///
    /// # Panics
    ///
    /// Panics if the data is in the commit queue, but is not owned by the commit queue.
    pub(super) fn cancel_and_take_data_from_commit_queue(
        &mut self,
        key: &Key,
    ) -> Option<TakenData<Key, Data>> {
        self.commit_queue
            .as_mut()
            .and_then(|queue| match queue.cancel(key) {
                Some(EnqueuedCommit::Owned(key, data, was_enqueued_at)) => Some(TakenData {
                    key,
                    data,
                    was_enqueued_at: Some(was_enqueued_at),
                }),
                Some(EnqueuedCommit::References(_, _)) => panic!("invariant: called cancel_and_take_data_from_commit_queue when commit queue did not own data."),
                None => None,
            })
    }

    /// Polls the commit queue if it's ready. If the upstream manager is running in immutable mode, this will
    /// perpetually return a pending future.
    #[inline]
    pub(super) fn poll_commit_queue_ready<'a>(
        &'a mut self,
    ) -> impl Future<Output = Result<EnqueuedCommit<Key, Data>, tokio::time::Error>> + 'a {
        poll_option_ready(self.commit_queue.as_mut())
    }

    /// Performs the commit of data.
    ///
    /// # Panics
    ///
    /// Panics if data commit was already in progress for a given key, or if the upstream does not support commits.
    pub(super) fn do_commit_data(&mut self, key: Key, data: &mut Data) {
        if self
            .in_progress_commits
            .insert(key.clone(), InProgressCommit::new())
            .is_some()
        {
            // todo: we should panic here.
            // println!("warning: in progress commit already exists");
            return;
        }

        let data_commit_request = DataCommitRequest::new(self.internal_sender.clone(), key, data);
        self.upstream
            .as_mutable()
            .expect("invariant: commit data attempted with no mutable upstream.")
            .commit(data_commit_request);
    }

    pub(super) fn temp_mark_commit_complete(&mut self, key: Key) {
        self.in_progress_commits.remove(&key);
    }

    pub(super) fn do_load_data(&mut self, key: Key) {
        let result_channel = DataLoadRequest::new(self.internal_sender.clone(), key);
        self.upstream.as_immutable().load(result_channel);
    }
}

enum AfterCommitAction<Key, Data> {
    CommitImmediately(ImmediateCommitWaiters),
    CommitWithin(Instant),
    FinishTakeOver(TakeDataSender<Key, Data>),
}
struct InProgressCommitData<Key, Data> {
    commit_waiters: ImmediateCommitWaiters,
    next_deadline: Option<AfterCommitAction<Key, Data>>,
}

impl<Key, Data> Default for InProgressCommitData<Key, Data> {
    fn default() -> Self {
        Self {
            commit_waiters: SmallVec::new(),
            next_deadline: None,
        }
    }
}

#[derive(Default)]
struct InProgressCommit<Key, Data> {
    inner: Option<Box<InProgressCommitData<Key, Data>>>,
}

struct TakeDataPending {}

impl<Key, Data> InProgressCommit<Key, Data> {
    /// Creates an `InProgressCommit` with the given immediate commit waiters. The waiters will be called
    /// once the commit is finished, with the result.
    fn with_immediate_waiters(waiters: ImmediateCommitWaiters) -> Self {
        Self {
            inner: Some(Box::new(InProgressCommitData {
                commit_waiters: waiters,
                ..Default::default()
            })),
        }
    }

    fn new() -> Self {
        Self { inner: None }
    }

    /// Marks the in-progress commit as requiring an immediate commit after the in-flight one finishes.
    fn add_after_commit_immediate_waiters(
        &mut self,
        waiters: ImmediateCommitWaiters,
    ) -> Result<(), TakeDataPending> {
        match self
            .inner
            .get_or_insert_with(Default::default)
            .next_deadline
            .get_or_insert_with(|| AfterCommitAction::CommitImmediately(SmallVec::new()))
        {
            AfterCommitAction::CommitImmediately(existing_waiters) => {
                existing_waiters.extend(waiters);
                Ok(())
            }
            otherwise @ AfterCommitAction::CommitWithin(_) => {
                let _ = std::mem::replace(otherwise, AfterCommitAction::CommitImmediately(waiters));
                Ok(())
            }
            AfterCommitAction::FinishTakeOver(_) => Err(TakeDataPending {}),
        }
    }

    /// Marks the in-progress commit as requiring being scheduled again once it finishes.
    fn set_next_deadline(&mut self, deadline: Instant) {
        match self
            .inner
            .get_or_insert_with(Default::default)
            .next_deadline
            .get_or_insert_with(|| AfterCommitAction::CommitWithin(deadline))
        {
            // immediate always wins,
            AfterCommitAction::CommitWithin(x) if *x > deadline => {
                let _ = std::mem::replace(x, deadline);
            }
            AfterCommitAction::CommitWithin(_)
            | AfterCommitAction::CommitImmediately(_)
            | AfterCommitAction::FinishTakeOver(_) => {}
        }
    }
}

#[cfg(test)]
mod test {
    use super::InProgressCommit;

    #[test]
    fn test_sizeof_in_progress_commit() {
        assert_eq!(std::mem::size_of::<InProgressCommit<u32, u32>>(), 8);
    }
}
