use tokio::time::Instant;

use super::{DataCommitRequest, DataLoadRequest};

pub trait LoadFromUpstream<Key, Data>: Send + 'static {
    fn load(&mut self, request: DataLoadRequest<Key, Data>);
}

pub trait CommitToUpstream<Key, Data>: Send + 'static {
    fn commit<'a>(&mut self, request: DataCommitRequest<'a, Key, Data>);
}

pub trait UpstreamFactory<Key, Data: ServiceData> {
    type Upstream: LoadFromUpstream<Key, Data>;

    fn create(&mut self) -> Self::Upstream;
}

impl<Key, Data, T> UpstreamFactory<Key, Data> for T
where
    Data: ServiceData,
    T: Clone + LoadFromUpstream<Key, Data>,
{
    type Upstream = Self;

    fn create(&mut self) -> Self::Upstream {
        self.clone()
    }
}

pub trait MutableUpstreamFactory<Key, Data: ServiceData> {
    type Upstream: LoadFromUpstream<Key, Data> + CommitToUpstream<Key, Data>;

    fn create(&mut self) -> Self::Upstream;
}

impl<Key, Data, T> MutableUpstreamFactory<Key, Data> for T
where
    Data: ServiceData,
    T: Clone + LoadFromUpstream<Key, Data> + CommitToUpstream<Key, Data>,
{
    type Upstream = Self;

    fn create(&mut self) -> Self::Upstream {
        self.clone()
    }
}

pub trait ServiceData: Send + 'static {
    fn should_persist(&self) -> bool {
        true
    }

    fn get_expires_at(&self) -> Option<&Instant> {
        None
    }
}

impl<T: ServiceData> ServiceData for Option<T> {
    fn should_persist(&self) -> bool {
        self.is_some()
    }

    fn get_expires_at(&self) -> Option<&Instant> {
        self.as_ref().and_then(|x| x.get_expires_at())
    }
}
