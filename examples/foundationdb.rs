use futures::future::join_all;
use serde::{Deserialize, Serialize};
use thingvellir::upstreams::foundationdb::{
    FoundationDbFactoryBuilder, FoundationDbUpstreamFactory,
};
use thingvellir::upstreams::CborSerializer;
use thingvellir::{
    service_builder, Commit, DefaultCommitPolicy, MutableServiceHandle, ServiceData, ShardError,
    ShardStats,
};

use foundationdb::{
    api::{FdbApiBuilder, NetworkStop},
    tuple::Subspace,
    Database, FdbError, TransactOption,
};
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
    let network_builder = FdbApiBuilder::default()
        .build()
        .expect("fdb api initialized");
    let (runner, cond) = network_builder.build().expect("fdb network runners");

    let net_thread = std::thread::spawn(move || {
        unsafe { runner.run() }.expect("failed to run");
    });

    // Wait for the foundationDB network thread to start
    let fdb_network = cond.wait();
    let db = Database::default()?;

    let upstream: FoundationDbUpstreamFactory<CborSerializer> =
        FoundationDbFactoryBuilder::with_serializer(
            Subspace::all().subspace(&"thinkgvellir_benchmark"),
            CborSerializer {},
        )
        .build(db)?;

    let mut service = service_builder::<i64, Counter>(1000000)
        .build_mutable(upstream, DefaultCommitPolicy::Immediate);

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

        // let stats = service.get_shard_stats().await?;
        // println!("stats {}: {:?}", n, stats);
        // let merged = ShardStats::merge_stats(stats);
        // println!("merged {}: {:?}", n, merged);
    }

    fdb_network.stop().expect("stop network");
    net_thread.join().expect("join fdb thread");
    Ok(())
}
