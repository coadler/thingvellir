use futures::FutureExt;
use std::marker::PhantomData;
use std::sync::Arc;

use foundationdb::tuple::Subspace;
use foundationdb::{future::FdbSlice, Database, FdbError, FdbResult, TransactOption};

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
    fn with_serializer(subspace: Subspace, serializer: S) -> Self {
        Self {
            subspace,
            serializer,
        }
    }

    pub async fn build(
        self,
        db: Database,
    ) -> Result<FoundationDbUpstreamFactory<S>, UpstreamError> {
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
                key,
                |tx, key| {
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
