use std::fmt::Display;
use thingvellir::{service_builder, DataLoadRequest, LoadFromUpstream, ServiceData, ShardStats};
use tokio::time::{delay_for, Duration, Instant};

#[derive(Clone)]
struct InMemoryUpstream {
    expires_at: Instant,
}

impl<Key: Display> LoadFromUpstream<Key, Expires> for InMemoryUpstream {
    fn load(&mut self, request: DataLoadRequest<Key, Expires>) {
        let data = Expires {
            expires_at: self.expires_at,
        };
        request.resolve(data);
    }
}

#[derive(Clone)]
struct Expires {
    expires_at: Instant,
}

impl ServiceData for Expires {
    fn get_expires_at(&self) -> Option<&Instant> {
        Some(&self.expires_at)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut service = service_builder::<u64, Expires>(50000000).build(InMemoryUpstream {
        expires_at: Instant::now() + Duration::from_secs(15),
    });

    for n in 0..50 {
        let mut handle = service.handle();
        tokio::spawn(async move {
            for n in (n * 1000000)..(n * 1000000 + 1000000) {
                let _result = handle
                    .execute(n, |n| n.get_expires_at().cloned())
                    .await
                    .unwrap();
            }
        });
    }

    let st = tokio::time::Instant::now();

    loop {
        delay_for(Duration::from_secs(1)).await;
        let now = std::time::Instant::now();
        let stats = service.get_shard_stats().await.unwrap();
        let elapsed = now.elapsed();
        // println!("per shard stats (latency: {:?}): {:?}", elapsed, stats);
        let merged = ShardStats::merge_stats(stats);
        println!(
            "[{:?}] stats get latency -> {:?} Combined{:?}",
            st.elapsed(),
            elapsed,
            merged
        );
    }
}
