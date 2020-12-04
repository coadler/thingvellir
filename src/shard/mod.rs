mod builder;
mod commit_policy;
mod commit_queue;
mod config;
mod data_map;
mod handle;
mod lru_candidates;
mod marker;
mod messages;
mod pending_callback;
mod sender;
#[allow(clippy::module_inception)]
mod shard;
mod stats;
mod upstream_manager;
mod utils;

use self::commit_policy::{
    AccumulatedCommitPolicy, CommitPolicy, CommitPolicyNoDefault, ImmediateCommitWaiters,
    InnerCommitPolicy,
};
use self::config::ShardConfig;
use self::data_map::{DataGuard, DataMap, DataState, TryTakeResult};
use self::messages::{ServiceHandleMessage, TakeDataSender};
use self::pending_callback::PendingCallback;
use self::sender::ServiceHandleShardSender;
use self::shard::Shard;
use self::upstream_manager::UpstreamManager;

pub(crate) use self::messages::InternalMessage;

pub use self::builder::{service_builder, ServiceBuilder};
pub use self::commit_policy::{Commit, DefaultCommitPolicy};
pub use self::handle::{MutableServiceHandle, ServiceHandle};
pub use self::sender::TakenData;
pub use self::stats::ShardStats;
