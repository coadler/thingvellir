// TODO - Seems like it would be nice to clean these things up, rather than ignoring them
#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

mod error;
mod shard;
mod upstream;
pub mod upstreams;

pub use self::error::{ShardError, UpstreamError};
pub use self::shard::{
    service_builder, Commit, DefaultCommitPolicy, MutableServiceHandle, ServiceBuilder,
    ServiceHandle, ShardStats,
};
pub use self::upstream::{
    CommitToUpstream, DataCommitRequest, DataLoadRequest, LoadFromUpstream, MutableUpstreamFactory,
    OwnedDataCommitRequest, ProcessingDataCommitRequest, ServiceData, UpstreamFactory,
};
