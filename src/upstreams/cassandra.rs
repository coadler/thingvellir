use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;
use tokio::time::Instant;

use cassandra_cpp::{Cluster, Consistency, PreparedStatement, Session, Statement};

#[cfg(feature = "cbor")]
use super::serializers::CborSerializer;
use super::serializers::UpstreamSerializer;
use super::traits::ToPrimaryKey;
use crate::{
    CommitToUpstream, DataCommitRequest, DataLoadRequest, LoadFromUpstream, MutableUpstreamFactory,
    ServiceData, UpstreamError,
};

static DEFAULT_PRIMARY_KEY_FIELD_NAME: &str = "key";
static DEFAULT_VALUE_FIELD_NAME: &str = "value";

pub struct CassandraKvUpstreamFactory<S> {
    session: Arc<Session>,
    select_statement: Arc<PreparedStatement>,
    insert_statement: Arc<PreparedStatement>,
    serializer: S,
}

pub struct CassandraKvFactoryBuilder<S> {
    keyspace_name: String,
    table_name: String,
    primary_key_field_name: String,
    value_field_name: String,
    create_table_if_not_exists: bool,
    serializer: S,
}

impl<S> CassandraKvFactoryBuilder<S> {
    pub fn with_serializer<K, T>(keyspace_name: K, table_name: T, serializer: S) -> Self
    where
        K: Into<String>,
        T: Into<String>,
    {
        Self {
            keyspace_name: keyspace_name.into(),
            table_name: table_name.into(),
            primary_key_field_name: DEFAULT_PRIMARY_KEY_FIELD_NAME.into(),
            value_field_name: DEFAULT_VALUE_FIELD_NAME.into(),
            create_table_if_not_exists: false,
            serializer,
        }
    }

    pub fn create_table_if_not_exists(mut self) -> Self {
        self.create_table_if_not_exists = true;
        self
    }

    pub fn with_field_names<K, V>(mut self, primary_key_field_name: K, value_field_name: V) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.primary_key_field_name = primary_key_field_name.into();
        self.value_field_name = value_field_name.into();

        self
    }

    pub async fn build(
        self,
        cluster: &Cluster,
    ) -> Result<CassandraKvUpstreamFactory<S>, UpstreamError> {
        let session = Session::new();
        session
            .connect_keyspace(cluster, &self.keyspace_name)?
            .await?;

        let session = Arc::new(session);

        if self.create_table_if_not_exists {
            // We don't bother preparing this, as it'll be executed once or never.
            let statement = Statement::new(
                &format!(
                    "CREATE TABLE IF NOT EXISTS {} ({} text PRIMARY KEY, {} blob)",
                    &self.table_name, &self.primary_key_field_name, &self.value_field_name
                ),
                0,
            );
            session.execute(&statement).await?;
        }

        let select_statement = session
            .prepare(&format!(
                "SELECT {} FROM {} WHERE {} = ?",
                &self.value_field_name, &self.table_name, &self.primary_key_field_name
            ))?
            .await?;

        let insert_statement = session
            .prepare(&format!(
                "INSERT INTO {} ({}, {}) VALUES(?, ?) USING TTL ?",
                &self.table_name, &self.primary_key_field_name, &self.value_field_name,
            ))?
            .await?;

        let upstream = CassandraKvUpstreamFactory {
            session,
            select_statement: Arc::new(select_statement.into()),
            insert_statement: Arc::new(insert_statement.into()),
            serializer: self.serializer,
        };

        Ok(upstream)
    }
}

impl<K, V, S> MutableUpstreamFactory<K, V> for CassandraKvUpstreamFactory<S>
where
    K: ToPrimaryKey + Send + 'static,
    V: ServiceData + Default,
    S: UpstreamSerializer<V>,
{
    type Upstream = CassandraKvUpstream<K, V, S>;

    fn create(&mut self) -> Self::Upstream {
        CassandraKvUpstream {
            session: self.session.clone(),
            select_statement: self.select_statement.clone(),
            insert_statement: self.insert_statement.clone(),
            serializer: self.serializer.clone(),
            _phantom: PhantomData,
        }
    }
}

pub struct CassandraKvUpstream<K, V, S> {
    _phantom: PhantomData<(K, V)>,
    session: Arc<Session>,
    select_statement: Arc<PreparedStatement>,
    insert_statement: Arc<PreparedStatement>,
    serializer: S,
}

impl<K, V, S> LoadFromUpstream<K, V> for CassandraKvUpstream<K, V, S>
where
    K: ToPrimaryKey + Send + 'static,
    V: ServiceData + Default,
    S: UpstreamSerializer<V>,
{
    fn load(&mut self, request: DataLoadRequest<K, V>) {
        let session = self.session.clone();
        let mut select_statement = self.select_statement.bind();
        let key = request.key().to_primary_key();
        let serializer = self.serializer.clone();

        request.spawn_default(async move {
            select_statement.set_consistency(Consistency::LOCAL_QUORUM)?;
            select_statement.bind_string(0, &key)?;
            let fut = session.execute(&select_statement);
            let result = fut.await?;
            let row = result.first_row().ok_or(UpstreamError::KeyNotFound)?;
            let column = row.get_column(0)?;
            let bytes = column.get_bytes()?;
            let value = serializer
                .deserialize(bytes)
                .map_err(|e| UpstreamError::serialization_error(e))?;

            Ok(value)
        })
    }
}

impl<K, V, S> CommitToUpstream<K, V> for CassandraKvUpstream<K, V, S>
where
    K: ToPrimaryKey + Send + 'static,
    V: ServiceData,
    S: UpstreamSerializer<V>,
{
    fn commit(&mut self, request: DataCommitRequest<K, V>) {
        let session = self.session.clone();
        let mut insert_statement = self.insert_statement.bind();
        let key = request.key().to_primary_key();
        let expires_at = request.data().get_expires_at().cloned();

        let serialized = match self.serializer.serialize(request.data()) {
            Err(e) => return request.reject(UpstreamError::serialization_error(e)),
            Ok(vec) => vec,
        };

        request.into_processing().spawn(async move {
            insert_statement.set_consistency(Consistency::LOCAL_QUORUM)?;
            insert_statement.bind_string(0, &key)?;
            insert_statement.bind_bytes(1, serialized)?;

            if let Some(expires_at) = expires_at {
                let now = Instant::now();
                let expires_in_secs = expires_at.saturating_duration_since(now).as_secs();
                insert_statement.bind_int64(3, expires_in_secs as i64)?;
            }

            let fut = session.execute(&insert_statement);
            let _result = fut.await?;

            Ok(())
        });
    }
}

#[cfg(feature = "cbor")]
pub type CassandraKvCborFactoryBuilder = CassandraKvFactoryBuilder<CborSerializer>;
#[cfg(feature = "cbor")]
pub type CassandraKvCborUpstreamFactory = CassandraKvUpstreamFactory<CborSerializer>;
#[cfg(feature = "cbor")]
pub type CassandraKvCborUpstream<K, V> = CassandraKvUpstream<K, V, CborSerializer>;

#[cfg(feature = "cbor")]
impl CassandraKvCborFactoryBuilder {
    pub fn new<K, T>(keyspace_name: K, table_name: T) -> Self
    where
        K: Into<String>,
        T: Into<String>,
    {
        CassandraKvFactoryBuilder::with_serializer(keyspace_name, table_name, CborSerializer {})
    }
}

#[cfg(feature = "cbor")]
#[cfg(test)]
mod test {
    use super::{
        CassandraKvCborFactoryBuilder, CassandraKvCborUpstream, CassandraKvCborUpstreamFactory,
    };
    use crate::upstream::test_utils::*;
    use crate::{CommitToUpstream, LoadFromUpstream, MutableUpstreamFactory};
    use cassandra_cpp::{Cluster, Statement};
    use serde::{Deserialize, Serialize};

    async fn create_upstream_factory(
    ) -> Result<CassandraKvCborUpstreamFactory, crate::UpstreamError> {
        let mut cluster = Cluster::default();
        cluster.set_contact_points("127.0.0.1").unwrap();
        let session = cluster.connect_async().await?;
        let statement = Statement::new("CREATE KEYSPACE IF NOT EXISTS test_thingvellir_cassandra_upstream WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true", 0);
        session.execute(&statement).await?;
        let statement = Statement::new(
            "DROP TABLE IF EXISTS test_thingvellir_cassandra_upstream.data",
            0,
        );
        session.execute(&statement).await?;
        let factory =
            CassandraKvCborFactoryBuilder::new("test_thingvellir_cassandra_upstream", "data")
                .create_table_if_not_exists()
                .build(&cluster)
                .await?;

        Ok(factory)
    }

    #[derive(Serialize, Deserialize, Default)]
    struct Data {
        test: String,
    }

    impl crate::ServiceData for Data {}

    #[tokio::test]
    async fn test_upstream_load_commit() -> Result<(), crate::UpstreamError> {
        let mut factory = create_upstream_factory().await?;
        let mut upstream: CassandraKvCborUpstream<i32, Data> = factory.create();

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
