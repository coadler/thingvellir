use std::future::Future;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use tokio::sync::mpsc;

use dynamic_pool::{DynamicPool, DynamicPoolItem, DynamicReset};
use futures::future::try_join_all;

use super::{Commit, ServiceHandleMessage, ServiceHandleShardSender, ShardStats, TakenData};

use crate::ShardError;

struct ServiceHandleShardSenderVec<Key, Data>(Vec<ServiceHandleShardSender<Key, Data>>);
impl<Key, Data> DynamicReset for ServiceHandleShardSenderVec<Key, Data> {
    fn reset(&mut self) {}
}

impl<Key, Data> Deref for ServiceHandleShardSenderVec<Key, Data> {
    type Target = Vec<ServiceHandleShardSender<Key, Data>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<Key, Data> DerefMut for ServiceHandleShardSenderVec<Key, Data> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub struct ServiceHandle<Key, Data> {
    pool: DynamicPool<ServiceHandleShardSenderVec<Key, Data>>,
    shards: DynamicPoolItem<ServiceHandleShardSenderVec<Key, Data>>,
}

impl<Key: Send + 'static, Data: Send + 'static> ServiceHandle<Key, Data> {
    pub(super) fn with_senders_and_pool_capacity(
        senders: Vec<mpsc::Sender<ServiceHandleMessage<Key, Data>>>,
        handle_pool_capacity: usize,
    ) -> Self {
        let shards: Vec<_> = senders
            .into_iter()
            .map(ServiceHandleShardSender::from_sender)
            .collect();

        assert!(
            !shards.is_empty(),
            "Somehow, a ServiceHandle was tried to be constructed that holds 0 shards."
        );

        let pool = DynamicPool::new(handle_pool_capacity, handle_pool_capacity, move || {
            ServiceHandleShardSenderVec(shards.clone())
        });
        let shards = pool.take();

        ServiceHandle { pool, shards }
    }
}

impl<Key: Send + Hash, Data> ServiceHandle<Key, Data> {
    #[inline]
    pub fn handle(&self) -> Self {
        self.clone()
    }

    #[inline]
    fn select_shard(&mut self, key: &Key) -> &mut ServiceHandleShardSender<Key, Data> {
        let shard_len = self.shards.len();
        // When operating in single shard mode, we can circumvent having to hash the key,
        // as there is only one shard that will be able to service it anyways.
        let shard_idx = if shard_len == 1 {
            0
        } else {
            let key_hash = fxhash::hash(&key);
            key_hash % shard_len
        };

        // safety: index is bounds checked above.
        unsafe { self.shards.get_unchecked_mut(shard_idx) }
    }

    #[inline]
    pub fn execute<'a, F, T>(
        &'a mut self,
        key: Key,
        func: F,
    ) -> impl Future<Output = Result<T, ShardError>> + 'a
    where
        F: FnOnce(&Data) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.select_shard(&key).execute(key, func)
    }

    #[inline]
    pub fn execute_if_cached<'a, F, T>(
        &'a mut self,
        key: Key,
        func: F,
    ) -> impl Future<Output = Result<Option<T>, ShardError>> + 'a
    where
        F: FnOnce(&Data) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.select_shard(&key).execute_if_cached(key, func)
    }

    #[inline]
    pub fn take_data<'a>(
        &'a mut self,
        key: Key,
    ) -> impl Future<Output = Result<Option<TakenData<Key, Data>>, ShardError>> + 'a {
        self.select_shard(&key).take_data(key)
    }

    pub async fn get_shard_stats(&mut self) -> Result<Vec<ShardStats>, ShardError> {
        try_join_all(
            self.shards
                .iter_mut()
                .map(|s| s.get_shard_stats())
                .collect::<Vec<_>>(),
        )
        .await
    }
}

impl<Key, Data> Clone for ServiceHandle<Key, Data> {
    fn clone(&self) -> Self {
        let shards = self.pool.take();
        let pool = self.pool.clone();
        Self { shards, pool }
    }
}

pub struct MutableServiceHandle<Key, Data>(ServiceHandle<Key, Data>);

impl<Key: Send + Hash, Data> MutableServiceHandle<Key, Data> {
    pub(super) fn from_service_handle(service_handle: ServiceHandle<Key, Data>) -> Self {
        Self(service_handle)
    }

    pub fn into_immutable_handle(self) -> ServiceHandle<Key, Data> {
        self.0
    }

    #[inline]
    pub fn handle(&self) -> Self {
        self.clone()
    }

    #[inline]
    pub fn execute_mut<'a, F, T>(
        &'a mut self,
        key: Key,
        func: F,
    ) -> impl Future<Output = Result<T, ShardError>> + 'a
    where
        F: FnOnce(&mut Data) -> Commit<T> + Send + 'static,
        T: Send + 'static,
    {
        self.0.select_shard(&key).execute_mut(key, func)
    }
}

impl<Key, Data> Clone for MutableServiceHandle<Key, Data> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<Key, Data> Deref for MutableServiceHandle<Key, Data> {
    type Target = ServiceHandle<Key, Data>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<Key, Data> DerefMut for MutableServiceHandle<Key, Data> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
