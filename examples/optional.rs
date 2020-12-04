use anyhow::Error;
use thingvellir::{service_builder, DataLoadRequest, LoadFromUpstream, ServiceData};

#[derive(Clone, Default)]
struct MaybeExists {}

impl<Data: Default> LoadFromUpstream<u64, Option<Data>> for MaybeExists {
    fn load(&mut self, request: DataLoadRequest<u64, Option<Data>>) {
        if *request.key() == 1 {
            println!("loading {} found", request.key());
            request.resolve(Some(Default::default()));
        } else {
            println!("loading {} not found", request.key());
            request.resolve(None);
        }
    }
}

#[derive(Default, Clone)]
struct Counter;

impl ServiceData for Counter {}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut service = service_builder::<u64, Option<Counter>>(1000).build(MaybeExists {});

    assert!(service.execute(1, |x| x.is_some()).await? == true);
    assert!(service.execute(2, |x| x.is_some()).await? == false);
    // service
    //     .execute_mut(2, |x| Persist::default(x.replace(Default::default())))
    //     .await?;
    assert!(service.execute(2, |x| x.is_some()).await? == true);

    Ok(())
}
