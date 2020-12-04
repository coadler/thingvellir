use std::future::Future;
use tokio::sync::mpsc;

use crate::shard::InternalMessage;
use crate::{ServiceData, UpstreamError};

struct DataCommitGuard<Key, Data> {
    key: Option<Key>,
    sender: mpsc::UnboundedSender<InternalMessage<Key, Data>>,
}
#[must_use = "the data commit request must be resolved or rejected, otherwise the operation will be considered aborted."]
pub struct DataCommitRequest<'a, Key, Data> {
    data: &'a mut Data,
    commit_guard: DataCommitGuard<Key, Data>,
}

impl<'a, Key, Data> DataCommitRequest<'a, Key, Data> {
    pub(crate) fn new(
        sender: mpsc::UnboundedSender<InternalMessage<Key, Data>>,
        key: Key,
        data: &'a mut Data,
    ) -> Self {
        Self {
            commit_guard: DataCommitGuard {
                key: Some(key),
                sender,
            },
            data,
        }
    }

    pub fn data_mut(&mut self) -> &mut Data {
        self.data
    }

    pub fn data(&self) -> &Data {
        self.data
    }

    pub fn key(&self) -> &Key {
        self.commit_guard.key()
    }

    pub fn into_processing(self) -> ProcessingDataCommitRequest<Key, Data> {
        ProcessingDataCommitRequest {
            drop_guard: self.commit_guard,
        }
    }

    pub fn resolve(self) {
        self.into_processing().resolve();
    }

    pub fn reject<E: Into<UpstreamError>>(self, error: E) {
        self.into_processing().reject(error);
    }
}

impl<Key: Send + 'static, Data: ServiceData> DataCommitRequest<'_, Key, Data> {
    pub fn spawn<F: Future<Output = Result<(), UpstreamError>> + Send + 'static>(self, fut: F) {
        self.into_processing().spawn(fut);
    }
}

impl<Key, Data: Clone> DataCommitRequest<'_, Key, Data> {
    pub fn into_owned(self) -> OwnedDataCommitRequest<Key, Data> {
        OwnedDataCommitRequest {
            data: self.data.clone(),
            commit_guard: self.commit_guard,
        }
    }
}

#[must_use = "the data commit request must be resolved or rejected, otherwise the operation will be considered aborted."]
pub struct OwnedDataCommitRequest<Key, Data> {
    data: Data,
    commit_guard: DataCommitGuard<Key, Data>,
}

impl<Key, Data> OwnedDataCommitRequest<Key, Data> {
    pub fn into_inner(self) -> (Data, ProcessingDataCommitRequest<Key, Data>) {
        (
            self.data,
            ProcessingDataCommitRequest {
                drop_guard: self.commit_guard,
            },
        )
    }

    pub fn data(&self) -> &Data {
        &self.data
    }

    pub fn resolve(self) {
        self.commit_guard.resolve();
    }

    pub fn reject<E: Into<UpstreamError>>(self, error: E) {
        self.commit_guard.reject(error);
    }
}

impl<Key: Send + 'static, Data: ServiceData> OwnedDataCommitRequest<Key, Data> {
    pub fn spawn<
        R: Future<Output = Result<(), UpstreamError>> + Send + 'static,
        F: FnOnce(&Key, Data) -> R + Send + 'static,
    >(
        self,
        func: F,
    ) {
        tokio::spawn(async move {
            let drop_guard = self.commit_guard;
            let fut = (func)(drop_guard.key.as_ref().unwrap(), self.data);
            match fut.await {
                Ok(()) => drop_guard.resolve(),
                Err(err) => drop_guard.reject(err),
            };
        });
    }
}

#[must_use = "the data commit request must be resolved or rejected, otherwise the operation will be considered aborted."]
pub struct ProcessingDataCommitRequest<Key, Data> {
    drop_guard: DataCommitGuard<Key, Data>,
}

impl<Key, Data> ProcessingDataCommitRequest<Key, Data> {
    pub fn resolve(self) {
        self.drop_guard.resolve();
    }

    pub fn reject<E: Into<UpstreamError>>(self, error: E) {
        self.drop_guard.reject(error);
    }
}

impl<Key: Send + 'static, Data: ServiceData> ProcessingDataCommitRequest<Key, Data> {
    pub fn spawn<F: Future<Output = Result<(), UpstreamError>> + Send + 'static>(self, fut: F) {
        tokio::spawn(async move {
            match fut.await {
                Ok(()) => self.resolve(),
                Err(err) => self.reject(err),
            };
        });
    }
}

impl<Key, Data> DataCommitGuard<Key, Data> {
    fn key(&self) -> &Key {
        self.key.as_ref().unwrap()
    }

    fn resolve(mut self) {
        let key = self
            .key
            .take()
            .expect("invariant: key must be present, unless dropped.");
        self.sender
            .send(InternalMessage::DataCommitResult(key, Ok(())))
            .ok();
    }

    fn reject<E: Into<UpstreamError>>(mut self, error: E) {
        let key = self
            .key
            .take()
            .expect("invariant: key must be present, unless dropped.");
        self.sender
            .send(InternalMessage::DataCommitResult(key, Err(error.into())))
            .ok();
    }
}

impl<Key, Data> Drop for DataCommitGuard<Key, Data> {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.sender
                .send(InternalMessage::DataCommitResult(
                    key,
                    Err(UpstreamError::OperationAborted),
                ))
                .ok();
        }
    }
}
