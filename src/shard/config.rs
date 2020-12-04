use tokio::time::Duration;

use crate::DefaultCommitPolicy;

pub(super) struct ShardConfig {
    pub shard_id: u8,
    pub lru_candidates_num_probes: u16,
    pub max_data_capacity: usize,
    pub persist_queue_capacity: usize,
    pub default_commit_policy: Option<DefaultCommitPolicy>,
    pub cache_expiration_probe_interval: Duration,
    pub cache_expiration_probe_keys_per_tick: usize,
}
