use futures::FutureExt;
use std::marker::PhantomData;
use std::sync::Arc;

use foundationdb::{
    future::FdbSlice, tuple::Subspace, Database, FdbError, FdbResult, TransactOption,
};

use super::serializers::UpstreamSerializer;
use super::traits::ToPrimaryKey;
use crate::{
    CommitToUpstream, DataCommitRequest, DataLoadRequest, LoadFromUpstream, MutableUpstreamFactory,
    ServiceData, UpstreamError,
};

pub struct FoundationDbUpstreamFactory<S> {
    db: Arc<Database>,
    subspace: Arc<Subspace>,
    serializer: S,
}

pub struct FoundationDbFactoryBuilder<S> {
    subspace: Subspace,
    serializer: S,
}

impl<S> FoundationDbFactoryBuilder<S> {
    pub fn with_serializer(subspace: Subspace, serializer: S) -> Self {
        Self {
            subspace,
            serializer,
        }
    }

    pub fn build(self, db: Database) -> Result<FoundationDbUpstreamFactory<S>, UpstreamError> {
        Ok(FoundationDbUpstreamFactory {
            db: Arc::new(db),
            subspace: Arc::new(self.subspace),
            serializer: self.serializer,
        })
    }
}

impl<K, V, S> MutableUpstreamFactory<K, V> for FoundationDbUpstreamFactory<S>
where
    K: ToPrimaryKey + Send + 'static,
    V: ServiceData + Default,
    S: UpstreamSerializer<V>,
{
    type Upstream = FoundationDbUpstream<K, V, S>;

    fn create(&mut self) -> Self::Upstream {
        FoundationDbUpstream {
            db: Arc::clone(&self.db),
            subspace: Arc::clone(&self.subspace),
            serializer: self.serializer.clone(),
            _phantom: PhantomData,
        }
    }
}

pub struct FoundationDbUpstream<K, V, S> {
    _phantom: PhantomData<(K, V)>,
    db: Arc<Database>,
    subspace: Arc<Subspace>,
    serializer: S,
}

impl<K, V, S> LoadFromUpstream<K, V> for FoundationDbUpstream<K, V, S>
where
    K: ToPrimaryKey + Send + 'static,
    V: ServiceData + Default,
    S: UpstreamSerializer<V>,
{
    fn load(&mut self, request: DataLoadRequest<K, V>) {
        let db = Arc::clone(&self.db);
        let key = request.key().to_primary_key();
        let key = self.subspace.pack(&key);
        let serializer = self.serializer.clone();

        request.spawn_default(async move {
            let bytes = db
                .transact_boxed::<'_, _, _, _, FdbError>(
                    key,
                    |tx, key| async move { Ok(tx.get(key, false).await?) }.boxed(),
                    TransactOption::idempotent(),
                )
                .await?
                .ok_or(UpstreamError::KeyNotFound)?;
            let value = serializer
                .deserialize(bytes.as_ref())
                .map_err(|e| UpstreamError::serialization_error(e))?;

            Ok(value)
        })
    }
}

impl<K, V, S> CommitToUpstream<K, V> for FoundationDbUpstream<K, V, S>
where
    K: ToPrimaryKey + Send + 'static,
    V: ServiceData,
    S: UpstreamSerializer<V>,
{
    fn commit(&mut self, request: DataCommitRequest<K, V>) {
        let db = Arc::clone(&self.db);
        let key = request.key().to_primary_key();
        let key = self.subspace.pack(&key);

        let serialized = match self.serializer.serialize(request.data()) {
            Ok(vec) => vec,
            Err(e) => return request.reject(UpstreamError::serialization_error(e)),
        };

        request.into_processing().spawn(async move {
            db.transact_boxed::<'_, _, _, _, FdbError>(
                (key, serialized),
                |tx, (key, serialized)| {
                    async move {
                        tx.set(key, serialized.as_ref());
                        Ok(())
                    }
                    .boxed()
                },
                TransactOption::idempotent(),
            )
            .await?;
            Ok(())
        });
    }
}

#[cfg(feature = "cbor")]
#[cfg(test)]
mod test {
    use super::{FoundationDbFactoryBuilder, FoundationDbUpstream, FoundationDbUpstreamFactory};
    use crate::upstream::test_utils::*;
    use crate::upstreams::serializers::CborSerializer;
    use crate::{CommitToUpstream, LoadFromUpstream, MutableUpstreamFactory};
    use foundationdb::{
        api::{FdbApiBuilder, NetworkStop},
        tuple::Subspace,
        Database, FdbError, TransactOption,
    };
    use futures::FutureExt;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    struct Stop {
        stop: Option<NetworkStop>,
        handle: Option<std::thread::JoinHandle<()>>,
    }

    impl Drop for Stop {
        fn drop(&mut self) {
            self.stop.take().unwrap().stop().expect("stop network");
            self.handle.take().unwrap().join().expect("join fdb thread");
        }
    }

    fn test_fdb() -> Result<(Database, Stop), crate::UpstreamError> {
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

        Ok((
            db,
            Stop {
                stop: Some(fdb_network),
                handle: Some(net_thread),
            },
        ))
    }

    async fn create_upstream_factory()
    -> Result<(FoundationDbUpstreamFactory<CborSerializer>, Stop), crate::UpstreamError> {
        let (db, stop) = test_fdb()?;
        let sub = Subspace::all().subspace(&"test_thingvellir");

        db.transact_boxed::<'_, _, _, _, FdbError>(
            sub.clone(),
            |tx, sub| {
                async move {
                    tx.clear_subspace_range(sub);
                    Ok(())
                }
                .boxed()
            },
            TransactOption::idempotent(),
        )
        .await?;

        let upstream: FoundationDbUpstreamFactory<CborSerializer> =
            FoundationDbFactoryBuilder::with_serializer(sub, CborSerializer {}).build(db)?;
        Ok((upstream, stop))
    }

    #[derive(Serialize, Deserialize, Default)]
    struct Data {
        test: String,
    }

    impl crate::ServiceData for Data {}

    #[tokio::test]
    async fn test_upstream_load_commit() -> Result<(), crate::UpstreamError> {
        let (mut upstream_builder, _stop) = create_upstream_factory().await?;
        let mut upstream: FoundationDbUpstream<i32, Data, CborSerializer> =
            upstream_builder.create();

        // Try loading data that does not exist!
        {
            let (request, result) = data_load_request_pair::<i32, Data>(1);
            upstream.load(request);

            let result = result.await?;
            assert_eq!(result.test, "");
        }

        // Commit with some data:
        let mut new_data = Data {
            test: "hello cbor!".into(),
        };

        {
            let (request, result) = data_commit_request_pair(2, &mut new_data);
            upstream.commit(request);
            result.await?;
        }

        // Try loading that data again.
        {
            let (request, result) = data_load_request_pair::<i32, Data>(2);
            upstream.load(request);

            let result = result.await?;
            assert_eq!(result.test, new_data.test);
        }

        Ok(())
    }
}
