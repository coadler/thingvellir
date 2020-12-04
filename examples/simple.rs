use std::fmt::Display;

use rand::{thread_rng, Rng};
use thingvellir::{
    service_builder, Commit, CommitToUpstream, DataCommitRequest, DataLoadRequest,
    DefaultCommitPolicy, LoadFromUpstream, ServiceData, ShardStats,
};
use tokio::time::{delay_for, Duration};

/// This really dumb upstream just simply creates structs using
/// their default impl when requested.
#[derive(Clone, Default)]
struct InMemoryUpstream {}

impl<Key: Display, Data: Default> LoadFromUpstream<Key, Data> for InMemoryUpstream {
    fn load(&mut self, request: DataLoadRequest<Key, Data>) {
        // println!("loading {}", request.key());
        request.resolve(Default::default());
    }
}

impl<Key: Display, Data: Default> CommitToUpstream<Key, Data> for InMemoryUpstream {
    fn commit<'a>(&mut self, request: DataCommitRequest<'a, Key, Data>) {
        // println!("committing {}", request.key());
        request.resolve();
    }
}

// This is our simple data that the service will manage. There are many instances of the data. Service is an abstraction of an in-memory
// mapping of Key -> Data (Counter in this case is the Data). The service abstraction here, handles loading and committing data to the upstream
// and lets you execute mutations of the data in a simple, easy-to-use abstraction. See `handle.execute_mut(..)` below.

// Depending on your Upstream implementation, this struct may be serialized with serde, or profobuf, or really,
// whatever you want.
#[derive(Default, Clone)]
struct Counter {
    s: u64,
}

impl Counter {
    #[allow(dead_code)]
    fn incr(&mut self) -> u64 {
        self.s += 1;
        self.s
    }

    fn get(&self) -> u64 {
        self.s
    }
}

// This will eventually implement concepts that are required in order to handle data expiration, e.g. this will let us communicate to the
// service & upstream layer how long until the data should TTL out.
impl ServiceData for Counter {}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The service, creating this spawns all shards. The service and all shards will live as long as the service, or any handles of
    // the service are in-scope. Data is sharded by key, using fx_hash, in order to distribute data across all shards.
    // concepts:
    //  - `InMemoryUpstream` - handles all "load" requests by creating a new instance of the `Data` using `Default::default()`.
    //  - `DefaultCommitPolicy` - specifies the data durability policy for when the data is committed from the in-memory
    //    write buffer ("PersistQueue") to the upstream.
    //  - `max_data_capacity` - the maximum amount of data (not in size, but in # of keys) to hold in memory at once, this number
    //    tunable based on your machine type, and how big your structs are. Once this value is reached, items will be evicted from
    //    memory using a probabilistic LRU algorithm (see: lru_candidates.rs)
    let mut service = service_builder::<u64, Counter>(25000).build_mutable(
        InMemoryUpstream::default(),
        DefaultCommitPolicy::Within(Duration::from_secs(30)),
    );

    let _result = service
        // Persist::default(T) -> persist the mutation using the default commit policy
        // specified when the service is constructed.
        .execute_mut(1, |n| Commit::immediately(n.get()))
        // `execute_mut` submits the mutation to the shard, and returns a future which will resolve
        // when the data mutation is complete, and depending on commit policy, when the data
        // has been persisted by the storage layer.
        .await
        .unwrap();

    // spawns some workers to spam requests to the data.
    for _ in 0..32 {
        let mut handle = service.handle();
        tokio::spawn(async move {
            loop {
                let n = thread_rng().gen_range(0, 5000);
                // handle.execute takes a key (in this case, we are randomly generating a key,)
                // and a FnOnce. The FnOnce is called once the data is loaded.
                // the data loading is coalesced and cached.

                // execute vs execute mut:
                // - execute: executes it in an immutable context, we can make sure we don't need to
                //   deal with persisting the data, since we never gave out a mutable reference.
                // - execute_mut: executes with the intent of mutating the data, we need to handle
                //   persistence here.

                // note: this operation is cancellable, simply drop the future returned by `service.execute(...)` and the shard
                // will not execute the mutation if the the future was dropped before the shard handled the mutation.
                let _result = handle
                    // Persist::default(T) -> persist the mutation using the default commit policy
                    // specified when the service is constructed.
                    .execute_mut(n, |n| Commit::immediately(n.get()))
                    // `execute_mut` submits the mutation to the shard, and returns a future which will resolve
                    // when the data mutation is complete, and depending on commit policy, when the data
                    // has been persisted by the storage layer.
                    .await
                    .unwrap();
                // _result here is the result of `n.incr()`.
            }
        });
    }

    // A simple example of doing a non-mutable execution:
    // - This submits the read operation to the shard, and resolves once the shard has processed the read operation.
    // - Like execute_mut, this can be canceled by dropping the future, to specify a timeout,
    //   simply use: `tokio::time::timeout(service.execute(...)).await`.
    let _result = service.execute(100, |n| n.get()).await.unwrap();
    let _result = service.execute_if_cached(100, |n| n.get()).await.unwrap();

    // this is just stats reporting to stdout, a quick and easy way to "benchmark" the code at a high level.
    let mut prev_combined_hits = 0;
    loop {
        delay_for(Duration::from_secs(1)).await;
        let now = std::time::Instant::now();
        let stats = service.get_shard_stats().await.unwrap();
        let elapsed = now.elapsed();
        println!("per shard stats (latency: {:?}): {:?}", elapsed, stats);
        let merged = ShardStats::merge_stats(stats);
        let delta = merged.executions_complete - prev_combined_hits;
        prev_combined_hits = merged.executions_complete;
        println!(
            "combined = {:?}, {}, {} per sec",
            merged, merged.executions_complete, delta
        );
    }
}
