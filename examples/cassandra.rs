use futures::future::join_all;
use serde::{Deserialize, Serialize};
use thingvellir::upstreams::cassandra::CassandraKvCborFactoryBuilder;
use thingvellir::{
    service_builder, Commit, DefaultCommitPolicy, MutableServiceHandle, ServiceData, ShardError,
    ShardStats,
};

use cassandra_cpp::Cluster;
/// This really dumb upstream just simply creates structs using
/// their default impl when requested.
#[derive(Default, Clone, Serialize, Deserialize)]
struct Counter {
    s: i64,
}

impl Counter {
    fn incr(&mut self) -> i64 {
        self.s += 1;
        self.s
    }

    #[allow(dead_code)]
    fn get(&self) -> i64 {
        self.s
    }
}

impl ServiceData for Counter {}

#[tokio::main]
async fn main() -> Result<(), ShardError> {
    let mut cluster = Cluster::default();
    cluster.set_contact_points("127.0.0.1").unwrap();

    let factory = CassandraKvCborFactoryBuilder::new("test_keyspace", "cbor_counters")
        .create_table_if_not_exists()
        .build(&cluster)
        .await?;

    let mut service = service_builder::<i64, Counter>(1000000)
        .build_mutable(factory, DefaultCommitPolicy::Immediate);

    async fn bench(h: MutableServiceHandle<i64, Counter>) {
        let mut futs = vec![];
        for i in 0..50 {
            let s = i * 2000;
            let e = s + 2000;
            let mut handle = h.handle();
            let fut = tokio::spawn(async move {
                for i in s..e {
                    let _result = handle
                        .execute_mut(i, |n| Commit::immediately(n.incr()))
                        .await
                        .unwrap();
                    // println!("Got result {:?}", result);
                }
            });

            futs.push(fut);
        }
        join_all(futs).await;
    }

    for n in 1..5 {
        let s = std::time::Instant::now();
        bench(service.handle()).await;
        let e = s.elapsed();
        println!("bench {}: {:?}", n, e);

        let stats = service.get_shard_stats().await?;
        println!("stats {}: {:?}", n, stats);
        let merged = ShardStats::merge_stats(stats);
        println!("merged {}: {:?}", n, merged);
    }

    Ok(())
}
