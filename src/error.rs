use std::sync::{Arc, Mutex};
use thiserror::Error;

use tokio::sync::mpsc::error::SendError as MpscSendError;
use tokio::sync::oneshot::error::RecvError as OneShotRecvError;

#[derive(Debug, Clone, Error)]
pub enum ShardError {
    #[error("shard is at capacity, and is unable to evict any records")]
    ShardAtCapacity,

    #[error("data returned from upstream is immediately expired.")]
    DataImmediatelyExpired,

    #[error("upstream error: {error:?}")]
    UpstreamError { error: UpstreamError },

    #[error("the shard is no longer running")]
    ShardGone,
}

impl From<OneShotRecvError> for ShardError {
    fn from(_error: OneShotRecvError) -> Self {
        ShardError::ShardGone
    }
}

impl<T> From<MpscSendError<T>> for ShardError {
    fn from(_error: MpscSendError<T>) -> Self {
        ShardError::ShardGone
    }
}

#[derive(Debug, Error, Clone)]
pub enum UpstreamError {
    #[error("operation aborted")]
    OperationAborted,

    #[error("key not found")]
    KeyNotFound,

    #[error("driver error: {error:?}")]
    DriverError { error: Arc<anyhow::Error> },

    #[error("serialization error: {error:?}")]
    SerializationError { error: Arc<anyhow::Error> },
}

impl UpstreamError {
    pub fn is_not_found(&self) -> bool {
        match self {
            UpstreamError::KeyNotFound => true,
            _ => false,
        }
    }

    pub fn serialization_error<E>(error: E) -> Self
    where
        E: std::error::Error + Send + 'static,
    {
        UpstreamError::SerializationError {
            error: Arc::new(SyncError::new(error).into()),
        }
    }
}

impl From<anyhow::Error> for UpstreamError {
    fn from(error: anyhow::Error) -> Self {
        UpstreamError::DriverError {
            error: Arc::new(error),
        }
    }
}

#[cfg(feature = "cassandra")]
impl From<cassandra_cpp::Error> for UpstreamError {
    fn from(error: cassandra_cpp::Error) -> Self {
        UpstreamError::DriverError {
            error: Arc::new(SyncError::new(error).into()),
        }
    }
}

#[cfg(feature = "cassandra")]
impl From<cassandra_cpp::Error> for ShardError {
    fn from(error: cassandra_cpp::Error) -> Self {
        let upstream_error: UpstreamError = error.into();
        upstream_error.into()
    }
}

impl From<foundationdb::FdbError> for UpstreamError {
    fn from(error: foundationdb::FdbError) -> Self {
        UpstreamError::DriverError {
            error: Arc::new(error.into()),
        }
    }
}

impl From<foundationdb::FdbError> for ShardError {
    fn from(error: foundationdb::FdbError) -> Self {
        let upstream_error: UpstreamError = error.into();
        upstream_error.into()
    }
}

impl From<UpstreamError> for ShardError {
    fn from(error: UpstreamError) -> Self {
        ShardError::UpstreamError { error }
    }
}

/// Similar to SyncFailure from the failure crate, makes an error Sync, if it does not
/// implement it, by wrapping it in a `Mutex<_>` and acquiring the lock for `std::fmt::Display`
/// and `std::fmt::Debug`.
pub struct SyncError<E> {
    inner: Mutex<E>,
}

impl<E: std::error::Error + Send + 'static> SyncError<E> {
    pub fn new(err: E) -> Self {
        SyncError {
            inner: Mutex::new(err),
        }
    }
}

impl<T> std::fmt::Display for SyncError<T>
where
    T: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.inner.lock().unwrap().fmt(f)
    }
}

impl<T> std::fmt::Debug for SyncError<T>
where
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        (*self.inner.lock().unwrap()).fmt(f)
    }
}

impl<E: std::error::Error + Send + 'static> std::error::Error for SyncError<E> {}
