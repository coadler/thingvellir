use tokio::sync::oneshot;

use super::{DefaultCommitPolicy, InnerCommitPolicy, TakenData};
use crate::{ShardError, ShardStats, UpstreamError};

pub(crate) enum InternalMessage<Key, Data> {
    DataLoadResult(Key, Result<Data, UpstreamError>),
    DataCommitResult(Key, Result<(), UpstreamError>),
    DoExpeditedExpirationProbe,
}

pub(super) enum ServiceHandleMessage<Key, Data> {
    Execute(Key, Box<dyn FnOnce(Result<&Data, ShardError>) + Send>),
    ExecuteIfCached(Key, Box<dyn FnOnce(Option<&Data>) + Send>),
    ExecuteMut(
        Key,
        Box<
            dyn FnOnce(Result<&mut Data, ShardError>, DefaultCommitPolicy) -> InnerCommitPolicy
                + Send,
        >,
    ),
    GetStats(oneshot::Sender<ShardStats>),
    TakeData(Key, TakeDataSender<Key, Data>),
    Stop,
}

pub(super) type TakeDataSender<Key, Data> = oneshot::Sender<Option<TakenData<Key, Data>>>;
