use std::hash::Hash;

use super::{DataGuard, DefaultCommitPolicy, InnerCommitPolicy};
use crate::error::ShardError;
use crate::ServiceData;

pub(super) enum PendingCallback<Data> {
    ExecuteMut(
        Box<
            dyn FnOnce(Result<&mut Data, ShardError>, DefaultCommitPolicy) -> InnerCommitPolicy
                + Send,
        >,
    ),
    Execute(Box<dyn FnOnce(Result<&Data, ShardError>) + Send>),
}

impl<Data: ServiceData> PendingCallback<Data> {
    pub(super) fn reject(
        self,
        error: ShardError,
        default_commit_policy: Option<DefaultCommitPolicy>,
    ) {
        match self {
            PendingCallback::ExecuteMut(callback) => {
                (callback)(
                    Err(error),
                    default_commit_policy
                        .expect("invariant: mut operation with no default commit policy"),
                );
            }
            PendingCallback::Execute(callback) => {
                (callback)(Err(error));
            }
        }
    }

    pub(super) fn resolve<Key: Hash + Clone + Eq>(
        self,
        guard: &mut DataGuard<Key, Data>,
        default_commit_policy: Option<DefaultCommitPolicy>,
    ) -> InnerCommitPolicy {
        match self {
            PendingCallback::ExecuteMut(callback) => (callback)(
                Ok(guard.as_mut()),
                default_commit_policy
                    .expect("invariant: mut operation with no default commit policy"),
            ),
            PendingCallback::Execute(callback) => {
                (callback)(Ok(guard.as_ref()));
                InnerCommitPolicy::Noop
            }
        }
    }
}
