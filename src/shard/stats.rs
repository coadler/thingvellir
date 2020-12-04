// todo: split between inner and dynamic shard stats.

/// Stat counters for a single shard, holding various counters
/// that may be of use to report metrics for. A convenience method
/// is provided to sum all the counters via `ShardStats::merge_stats(..)`.
#[derive(Debug, Default, Clone)]
pub struct ShardStats {
    /// The number of executions that a given shard has completed.
    pub executions_complete: u64,
    /// The number of executions that completed as result of a coalesced load from upstream.
    pub executions_coalesced: u64,
    /// The number of executions that are currently pending data being loaded from the upstream.
    pub executions_pending: usize,
    /// The number of keys that have been evicted due to capacity constraints that caused an LRU key
    /// to be removed.
    pub keys_lru_evicted: u64,
    /// The number of keys that have been evicted due to TTLing out.
    pub keys_ttl_evicted: u64,
    /// The number of keys that have been removed via the `.take_data()` method.
    pub keys_taken: u64,
    /// The number of load operations that are currently in progress.
    pub loads_in_progress: usize,
    /// The number of load operations that have failed (not including those that have failed due to not_found.)
    pub loads_failed: u64,
    /// The number of load operations that have failed due to the upstream reporting that the key was not found.
    pub loads_not_found: u64,
    /// The number of load operations that have completed successfully.
    pub loads_complete: u64,
    /// The number of keys that the shard is currently holding in memory.
    pub data_size: usize,
    /// The number of keys that the shard is holding, that has an expiration.
    pub expiring_keys: usize,
    /// The number of expiration probes that the shard has run.
    pub expiration_probes_ran: u64,
    /// The number of expedited expiration probes that the shard has run.
    pub expiration_probes_expedited: u64,
}

impl ShardStats {
    #[inline]
    pub(super) fn record_pending_executions_completed(
        &mut self,
        pending_executions_completed: usize,
    ) {
        self.executions_pending -= pending_executions_completed;
        self.executions_complete += pending_executions_completed as u64;
        if pending_executions_completed > 1 {
            self.executions_coalesced += (pending_executions_completed - 1) as u64;
        }
    }

    #[inline]
    pub(super) fn record_load_complete(&mut self, result: Result<(), &crate::ShardError>) {
        self.loads_in_progress -= 1;

        match result {
            Ok(()) => {
                self.loads_complete += 1;
            }
            Err(crate::ShardError::UpstreamError { error }) => {
                if error.is_not_found() {
                    self.loads_not_found += 1;
                } else {
                    self.loads_failed += 1;
                }
            }
            Err(_err) => {
                self.loads_failed += 1;
            }
        }
    }

    /// Merges a bunch of `ShardStats` into a singular shard stat that has all the counters
    /// summed up.
    pub fn merge_stats<S: IntoIterator<Item = ShardStats>>(stats: S) -> Self {
        stats
            .into_iter()
            .fold(ShardStats::default(), |s, d| s.merge(d))
    }

    fn merge(self, other: ShardStats) -> Self {
        Self {
            executions_complete: self.executions_complete + other.executions_complete,
            executions_coalesced: self.executions_coalesced + other.executions_coalesced,
            executions_pending: self.executions_pending + other.executions_pending,
            loads_in_progress: self.loads_in_progress + other.loads_in_progress,
            data_size: self.data_size + other.data_size,
            keys_lru_evicted: self.keys_lru_evicted + other.keys_lru_evicted,
            keys_ttl_evicted: self.keys_ttl_evicted + other.keys_ttl_evicted,
            keys_taken: self.keys_taken + other.keys_taken,
            expiring_keys: self.expiring_keys + other.expiring_keys,
            loads_failed: self.loads_failed + other.loads_failed,
            loads_not_found: self.loads_not_found + other.loads_not_found,
            loads_complete: self.loads_complete + other.loads_complete,
            expiration_probes_ran: self.expiration_probes_ran + other.expiration_probes_ran,
            expiration_probes_expedited: self.expiration_probes_expedited
                + other.expiration_probes_expedited,
        }
    }
}
