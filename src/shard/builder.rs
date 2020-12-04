use std::fmt::Display;
use std::hash::Hash;
use std::marker::PhantomData;

use tokio::sync::mpsc;
use tokio::time::Duration;

use super::{MutableServiceHandle, ServiceHandle, Shard, ShardConfig};

use crate::{DefaultCommitPolicy, MutableUpstreamFactory, ServiceData, UpstreamFactory};

mod defaults {
    use tokio::time::Duration;

    pub const PERSIST_QUEUE_CAPACITY: usize = 0;
    pub const SHARD_SENDER_CHANNEL_BUFFER_SIZE: usize = 4096;
    pub const LRU_CANDIDATES_NUM_PROBES: u16 = 5;
    pub const HANDLE_POOL_CAPACITY: usize = 128;
    pub const CACHE_EXPIRATION_PROBE_INTERVAL: Duration = Duration::from_millis(100);
    pub const CACHE_EXPIRATION_PROBE_KEYS_PER_TICK: usize = 25;
}

pub struct ServiceBuilder<Key, Data> {
    max_data_capacity: usize,
    num_shards: Option<u8>,
    shard_persist_queue_capacity: Option<usize>,
    shard_sender_channel_buffer_size: Option<usize>,
    lru_candidates_num_probes: Option<u16>,
    handle_pool_capacity: Option<usize>,
    cache_expiration_probe_interval: Option<Duration>,
    cache_expiration_probe_keys_per_tick: Option<usize>,
    _phantom: PhantomData<(Key, Data)>,
}

pub fn service_builder<Key: Send + Clone + Hash + Eq + Display + 'static, Data: ServiceData>(
    max_data_capacity: usize,
) -> ServiceBuilder<Key, Data> {
    ServiceBuilder::new(max_data_capacity)
}

impl<Key: Send + Clone + Hash + Eq + Display + 'static, Data: ServiceData>
    ServiceBuilder<Key, Data>
{
    fn new(max_data_capacity: usize) -> Self {
        assert!(max_data_capacity > 0, "max_data_capacity must be > 0");

        Self {
            max_data_capacity,
            num_shards: None,
            shard_persist_queue_capacity: None,
            shard_sender_channel_buffer_size: None,
            lru_candidates_num_probes: None,
            handle_pool_capacity: None,
            cache_expiration_probe_interval: None,
            cache_expiration_probe_keys_per_tick: None,
            _phantom: PhantomData,
        }
    }

    pub fn num_shards(mut self, num_shards: u8) -> Self {
        assert!(num_shards > 0, "num_shards must be > 0");
        self.num_shards = Some(num_shards);
        self
    }

    pub fn shard_persist_queue_capacity(mut self, shard_persist_queue_capacity: usize) -> Self {
        self.shard_persist_queue_capacity = Some(shard_persist_queue_capacity);
        self
    }

    pub fn lru_candidates_num_probes(mut self, lru_candidates_num_probes: u16) -> Self {
        self.lru_candidates_num_probes = Some(lru_candidates_num_probes);
        self
    }

    pub fn shard_sender_channel_buffer_size(
        mut self,
        shard_sender_channel_buffer_size: usize,
    ) -> Self {
        self.shard_sender_channel_buffer_size = Some(shard_sender_channel_buffer_size);
        self
    }

    pub fn cache_expiration_probe_interval(
        mut self,
        cache_expiration_probe_interval: Duration,
    ) -> Self {
        self.cache_expiration_probe_interval = Some(cache_expiration_probe_interval);
        self
    }
    pub fn cache_expiration_probe_keys_per_tick(
        mut self,
        cache_expiration_probe_keys_per_tick: usize,
    ) -> Self {
        self.cache_expiration_probe_keys_per_tick = Some(cache_expiration_probe_keys_per_tick);
        self
    }

    pub fn build<UpstreamFactoryT: UpstreamFactory<Key, Data>>(
        self,
        mut upstream_factory: UpstreamFactoryT,
    ) -> ServiceHandle<Key, Data> {
        let num_shards = self.get_num_shards();
        let shard_sender_channel_buffer_size = self.get_shard_sender_buffer_size();

        let mut shard_senders = Vec::with_capacity(num_shards as usize);

        for shard_id in 0..num_shards {
            let (sender, receiver) = mpsc::channel(shard_sender_channel_buffer_size);
            let upstream = upstream_factory.create();
            let shard_config = self.get_shard_config(shard_id, None);
            let shard = Shard::new(
                receiver,
                super::marker::Immutable::new(upstream),
                shard_config,
            );
            tokio::spawn(shard.run());
            shard_senders.push(sender);
        }

        ServiceHandle::with_senders_and_pool_capacity(
            shard_senders,
            self.get_handle_pool_capacity(),
        )
    }

    pub fn build_mutable<UpstreamFactoryT: MutableUpstreamFactory<Key, Data>>(
        self,
        mut upstream_factory: UpstreamFactoryT,
        default_commit_policy: DefaultCommitPolicy,
    ) -> MutableServiceHandle<Key, Data> {
        let num_shards = self.get_num_shards();
        let shard_sender_channel_buffer_size = self.get_shard_sender_buffer_size();

        let mut shard_senders = Vec::with_capacity(num_shards as usize);

        for shard_id in 0..num_shards {
            let (sender, receiver) = mpsc::channel(shard_sender_channel_buffer_size);
            let upstream = upstream_factory.create();
            let shard_config = self.get_shard_config(shard_id, Some(default_commit_policy));
            let shard = Shard::new(
                receiver,
                super::marker::Mutable::new(upstream),
                shard_config,
            );
            tokio::spawn(shard.run());
            shard_senders.push(sender);
        }

        MutableServiceHandle::from_service_handle(ServiceHandle::with_senders_and_pool_capacity(
            shard_senders,
            self.get_handle_pool_capacity(),
        ))
    }

    fn get_num_shards(&self) -> u8 {
        self.num_shards.unwrap_or_else(|| num_cpus::get() as u8)
    }

    fn get_handle_pool_capacity(&self) -> usize {
        self.handle_pool_capacity
            .unwrap_or(defaults::HANDLE_POOL_CAPACITY)
    }

    fn get_shard_sender_buffer_size(&self) -> usize {
        self.shard_sender_channel_buffer_size
            .unwrap_or(defaults::SHARD_SENDER_CHANNEL_BUFFER_SIZE)
    }

    fn get_shard_config(
        &self,
        shard_id: u8,
        default_commit_policy: Option<DefaultCommitPolicy>,
    ) -> ShardConfig {
        let shard_persist_queue_capacity = self
            .shard_persist_queue_capacity
            .unwrap_or(defaults::PERSIST_QUEUE_CAPACITY);
        let max_data_capacity = self.max_data_capacity / (self.get_num_shards() as usize);
        let lru_candidates_num_probes = self
            .lru_candidates_num_probes
            .unwrap_or(defaults::LRU_CANDIDATES_NUM_PROBES);
        let cache_expiration_probe_interval = self
            .cache_expiration_probe_interval
            .unwrap_or(defaults::CACHE_EXPIRATION_PROBE_INTERVAL);
        let cache_expiration_probe_keys_per_tick = self
            .cache_expiration_probe_keys_per_tick
            .unwrap_or(defaults::CACHE_EXPIRATION_PROBE_KEYS_PER_TICK);

        ShardConfig {
            shard_id,
            max_data_capacity,
            persist_queue_capacity: shard_persist_queue_capacity,
            lru_candidates_num_probes,
            default_commit_policy,
            cache_expiration_probe_interval,
            cache_expiration_probe_keys_per_tick,
        }
    }
}
