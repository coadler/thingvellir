use std::marker::PhantomData;

use crate::{CommitToUpstream, DataCommitRequest, LoadFromUpstream, ServiceData};

pub struct NotMutable<Key, Data>(pub PhantomData<(Key, Data)>);
impl<Key, Data> Default for NotMutable<Key, Data> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

pub struct Immutable<Key, Data, Upstream>(Upstream, NotMutable<Key, Data>);

impl<Key, Data, Upstream> Immutable<Key, Data, Upstream> {
    pub fn new(upstream: Upstream) -> Self {
        Self(upstream, NotMutable::default())
    }
}
pub struct Mutable<Key, Data, Upstream>(Upstream, PhantomData<(Key, Data)>);

impl<Key, Data, Upstream> Mutable<Key, Data, Upstream> {
    pub fn new(upstream: Upstream) -> Self {
        Self(upstream, PhantomData)
    }
}

pub trait MaybeMutableUpstream<Key, Data> {
    type Immutable: LoadFromUpstream<Key, Data>;
    type Mutable: CommitToUpstream<Key, Data>;

    fn as_immutable(&mut self) -> &mut Self::Immutable;
    fn as_mutable(&mut self) -> Option<&mut Self::Mutable>;
}

impl<Key: Send + 'static, Data: ServiceData> CommitToUpstream<Key, Data> for NotMutable<Key, Data> {
    fn commit<'a>(&mut self, data_commit_request: DataCommitRequest<'a, Key, Data>) {
        panic!("invariant: tried to mutate, with an immutable upstream.");
    }
}

impl<Key: Send + 'static, Data: ServiceData, Upstream: LoadFromUpstream<Key, Data>>
    MaybeMutableUpstream<Key, Data> for Immutable<Key, Data, Upstream>
{
    type Immutable = Upstream;
    type Mutable = NotMutable<Key, Data>;

    fn as_immutable(&mut self) -> &mut Self::Immutable {
        &mut self.0
    }

    fn as_mutable(&mut self) -> Option<&mut Self::Mutable> {
        None
    }
}

impl<
        Key: Send + 'static,
        Data: ServiceData,
        Upstream: LoadFromUpstream<Key, Data> + CommitToUpstream<Key, Data>,
    > MaybeMutableUpstream<Key, Data> for Mutable<Key, Data, Upstream>
{
    type Immutable = Upstream;
    type Mutable = Upstream;

    fn as_immutable(&mut self) -> &mut Self::Immutable {
        &mut self.0
    }

    fn as_mutable(&mut self) -> Option<&mut Self::Mutable> {
        Some(&mut self.0)
    }
}
