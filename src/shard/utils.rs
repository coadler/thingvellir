use crate::ServiceData;
use tokio::time::Instant;

/// Checks if the data is expired, because it's elapsed the TTL.
#[inline]
pub(super) fn is_expired<Data: ServiceData>(data: &Data) -> bool {
    match data.get_expires_at() {
        None => false,
        Some(expires_at) => *expires_at <= Instant::now(),
    }
}

pub(super) struct AbortOnPanic(String);

impl AbortOnPanic {
    pub(super) fn new(reason: String) -> Self {
        Self(reason)
    }
}

impl Drop for AbortOnPanic {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("Process aborted, because: {}", self.0);
            std::process::abort();
        }
    }
}
