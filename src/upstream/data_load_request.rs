use std::future::Future;

use tokio::sync::mpsc;

use crate::shard::InternalMessage;
use crate::{ServiceData, UpstreamError};

/// The data load request is passed to [`LoadFromUpstream::load`] handler.
///
/// It contains methods to notify the shard that the data has finished loading,
/// or has failed to load.
///
/// If the DataLoadRequest is dropped, without sending a result, the shard will
/// receive an `UpstreamError::OperationAborted` for the given key being loaded.
/// This prevents leaking load requests due to code error, where a code-path did
/// not use the data load request.
#[must_use = "the data load request must be resolved or rejected, otherwise the operation will be considered aborted."]
pub struct DataLoadRequest<Key, Data> {
    key: Option<Key>,
    sender: mpsc::UnboundedSender<InternalMessage<Key, Data>>,
}

impl<Key, Data> DataLoadRequest<Key, Data> {
    pub(crate) fn new(sender: mpsc::UnboundedSender<InternalMessage<Key, Data>>, key: Key) -> Self {
        Self {
            key: Some(key),
            sender,
        }
    }

    /// Returns a reference to the key that is currently being loaded.
    pub fn key(&self) -> &Key {
        self.key
            .as_ref()
            .expect("invariant: key must be present, unless dropped.")
    }

    /// Resolves the data load request with the given data.
    pub fn resolve(mut self, data: Data) {
        let key = self
            .key
            .take()
            .expect("invariant: key must be present, unless dropped.");
        self.sender
            .send(InternalMessage::DataLoadResult(key, Ok(data)))
            .ok();
    }

    /// Rejects the data load request with a given error. The error must be able to
    /// be converted into an [`UpstreamError`].
    pub fn reject<E: Into<UpstreamError>>(mut self, error: E) {
        let key = self
            .key
            .take()
            .expect("invariant: key must be present, unless dropped.");
        self.sender
            .send(InternalMessage::DataLoadResult(key, Err(error.into())))
            .ok();
    }
}

impl<Key: Send + 'static, Data: ServiceData> DataLoadRequest<Key, Data> {
    /// Convenience method to spawn a task to drive a future to completion, and capture that future's result in order
    /// to reject or resolve the data load request.
    pub fn spawn<F: Future<Output = Result<Data, UpstreamError>> + Send + 'static>(self, fut: F) {
        tokio::spawn(async move {
            match fut.await {
                Ok(data) => self.resolve(data),
                Err(err) => self.reject(err),
            };
        });
    }
}

impl<Key: Send + 'static, Data: ServiceData + Default> DataLoadRequest<Key, Data> {
    /// Similar to `spawn`, however, in event of encountering a [`UpstreamError::KeyNotFound`], error
    /// will resolve with the value of `Data::default`.
    ///
    /// [`spawn`]: `DataLoadRequest::spawn`
    pub fn spawn_default<F: Future<Output = Result<Data, UpstreamError>> + Send + 'static>(
        self,
        fut: F,
    ) {
        tokio::spawn(async move {
            match fut.await {
                Ok(data) => self.resolve(data),
                Err(UpstreamError::KeyNotFound) => self.resolve(Data::default()),
                Err(err) => self.reject(err),
            };
        });
    }
}

impl<Key, Data> Drop for DataLoadRequest<Key, Data> {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            // We are being dropped without calling `send_data`, this means that we dropped this request somehow.
            // This is probably a bug, but we don't want to "leak" the future indefinitely.
            self.sender
                .send(InternalMessage::DataLoadResult(
                    key,
                    Err(UpstreamError::OperationAborted),
                ))
                .ok();
        }
    }
}
