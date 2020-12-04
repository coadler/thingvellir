use std::future::Future;

use tokio::sync::mpsc;

use super::{DataCommitRequest, DataLoadRequest};
use crate::shard::InternalMessage;
use crate::UpstreamError;

pub(crate) fn data_load_request_pair<Key, Data>(
    key: Key,
) -> (
    DataLoadRequest<Key, Data>,
    impl Future<Output = Result<Data, UpstreamError>>,
) {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let data_load_request = DataLoadRequest::new(sender, key);
    let result_future = async move {
        match receiver.recv().await {
            Some(InternalMessage::DataLoadResult(_key, result)) => result,
            _ => unreachable!(),
        }
    };

    (data_load_request, result_future)
}

pub(crate) fn data_commit_request_pair<'a, Key, Data>(
    key: Key,
    data: &'a mut Data,
) -> (
    DataCommitRequest<'a, Key, Data>,
    impl Future<Output = Result<(), UpstreamError>>,
) {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let data_commit_request = DataCommitRequest::new(sender, key, data);
    let result_future = async move {
        match receiver.recv().await {
            Some(InternalMessage::DataCommitResult(_key, result)) => result,
            _ => unreachable!(),
        }
    };

    (data_commit_request, result_future)
}
