use tokio::time::Duration;

use crate::UpstreamError;
use smallvec::SmallVec;

pub(crate) enum InnerCommitPolicy {
    Noop,
    Immediate(ImmediateCommitWaiter),
    Within(Duration),
}

#[derive(Clone, Copy)]
pub enum DefaultCommitPolicy {
    /// The data must be persisted successfully before the execution returns.
    Immediate,
    /// The data should try to be persisted with at most a `Duration` delay.
    Within(Duration),
}

pub(super) enum CommitPolicy {
    /// The data should persist with the default persistence policy of the service.
    Default,
    /// The mutation was a no-op, and no additional persistence should be enqueued.
    Noop,
    /// The data must be persisted successfully before the execution returns.
    Immediate,
    /// The data should try to be persisted with at most a `Duration` delay.
    Within(Duration),
}

pub(super) enum CommitPolicyNoDefault {
    Noop,
    Immediate,
    Within(Duration),
}

impl CommitPolicy {
    pub(super) fn apply_default(
        self,
        default_commit_policy: DefaultCommitPolicy,
    ) -> CommitPolicyNoDefault {
        match self {
            CommitPolicy::Default => default_commit_policy.into(),
            CommitPolicy::Noop => CommitPolicyNoDefault::Noop,
            CommitPolicy::Within(duration) => CommitPolicyNoDefault::Within(duration),
            CommitPolicy::Immediate => CommitPolicyNoDefault::Immediate,
        }
    }
}

/// Specifies how the `execute_mut` should be committed to the upstream.
///
/// This allows you to control the durability of your data mutation.
///
/// [`execute_mut`]: ServiceHandle::execute_mut
pub struct Commit<Data>(CommitPolicy, Data);

impl<Data> Commit<Data> {
    /// Persist the item immediately. The execution future will not resolve until your write has been been
    /// acknowledged by the upstream layer.
    pub fn immediately(data: Data) -> Self {
        Self(CommitPolicy::Immediate, data)
    }

    /// Persist the data, using the default persistence policy for the service.
    pub fn default(data: Data) -> Self {
        Self(CommitPolicy::Default, data)
    }

    /// Ensure that the data is persisted within a given duration.
    pub fn within(data: Data, duration: Duration) -> Self {
        Self(CommitPolicy::Within(duration), data)
    }

    /// The mutation resulted in a no-op. Do not try to persist the data in any way.
    ///
    /// # Safety
    ///
    /// This function is unsafe, as it's up to you to make sure to only call this *if and only if*
    /// the data truly did not change. Since you're getting a `&mut` to the data, this contract
    /// cannot be statically enforced. So, it's up to you to use correctly.
    pub unsafe fn noop(data: Data) -> Self {
        Self(CommitPolicy::Noop, data)
    }

    pub(super) fn into_inner(self) -> (CommitPolicy, Data) {
        (self.0, self.1)
    }
}

impl Into<CommitPolicyNoDefault> for DefaultCommitPolicy {
    fn into(self) -> CommitPolicyNoDefault {
        match self {
            DefaultCommitPolicy::Within(duration) => CommitPolicyNoDefault::Within(duration),
            DefaultCommitPolicy::Immediate => CommitPolicyNoDefault::Immediate,
        }
    }
}

pub(super) type ImmediateCommitWaiter = Box<dyn FnOnce(Result<(), UpstreamError>) + Send + 'static>;

pub(super) type ImmediateCommitWaiters = SmallVec<[ImmediateCommitWaiter; 1]>;

pub(super) enum AccumulatedCommitPolicy {
    Noop,
    Within(Duration),
    Immediate(ImmediateCommitWaiters),
}

impl From<InnerCommitPolicy> for AccumulatedCommitPolicy {
    fn from(commit_policy: InnerCommitPolicy) -> Self {
        match commit_policy {
            InnerCommitPolicy::Within(duration) => AccumulatedCommitPolicy::Within(duration),
            InnerCommitPolicy::Immediate(callback) => callback.into(),
            InnerCommitPolicy::Noop => AccumulatedCommitPolicy::Noop,
        }
    }
}

impl From<ImmediateCommitWaiter> for AccumulatedCommitPolicy {
    fn from(waiter: ImmediateCommitWaiter) -> Self {
        let mut vec = SmallVec::new();
        vec.push(waiter);
        AccumulatedCommitPolicy::Immediate(vec)
    }
}

impl AccumulatedCommitPolicy {
    pub(super) fn new() -> Self {
        AccumulatedCommitPolicy::Noop
    }

    pub(super) fn did_mutate(&self) -> bool {
        match self {
            AccumulatedCommitPolicy::Noop => false,
            AccumulatedCommitPolicy::Within(_) | AccumulatedCommitPolicy::Immediate(_) => true,
        }
    }

    pub(super) fn accumulate(self, commit_policy: InnerCommitPolicy) -> Self {
        match commit_policy {
            InnerCommitPolicy::Within(duration) => self.accumulate_commit_within(duration),
            InnerCommitPolicy::Immediate(waiter) => self.accumulate_commit_immediate(waiter),
            InnerCommitPolicy::Noop => self,
        }
    }

    fn accumulate_commit_within(self, duration: Duration) -> Self {
        match self {
            AccumulatedCommitPolicy::Noop => AccumulatedCommitPolicy::Within(duration),
            AccumulatedCommitPolicy::Within(existing_duration) => {
                // We want to keep the minimum of the two durations.
                AccumulatedCommitPolicy::Within(std::cmp::min(existing_duration, duration))
            }
            // There's nothing faster than immediate.
            AccumulatedCommitPolicy::Immediate(_) => self,
        }
    }

    fn accumulate_commit_immediate(self, waiter: ImmediateCommitWaiter) -> Self {
        match self {
            AccumulatedCommitPolicy::Noop | AccumulatedCommitPolicy::Within(_) => waiter.into(),
            AccumulatedCommitPolicy::Immediate(mut waiters) => {
                waiters.push(waiter);
                AccumulatedCommitPolicy::Immediate(waiters)
            }
        }
    }
}
