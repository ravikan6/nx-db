use crate::{CacheBackend, CacheError, CacheFuture, CacheKey, CacheWrite, Namespace};
use bytes::Bytes;
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Pipeline};

#[derive(Clone)]
pub struct RedisCacheBackend {
    connection: ConnectionManager,
    prefix: String,
}

impl RedisCacheBackend {
    pub async fn connect(url: impl AsRef<str>) -> Result<Self, CacheError> {
        let client = redis::Client::open(url.as_ref())?;
        let connection = client.get_connection_manager().await?;
        Ok(Self::new(connection))
    }

    pub fn new(connection: ConnectionManager) -> Self {
        Self::with_prefix(connection, "database-cache")
    }

    pub fn with_prefix(connection: ConnectionManager, prefix: impl Into<String>) -> Self {
        Self {
            connection,
            prefix: prefix.into(),
        }
    }

    fn namespace_token(namespace: &Namespace) -> &str {
        namespace.as_str()
    }

    fn data_key_with_prefix(prefix: &str, namespace: &Namespace, key: &CacheKey) -> String {
        format!(
            "{prefix}:data:{}:{}",
            Self::namespace_token(namespace),
            key.as_str()
        )
    }

    fn namespace_index_key_with_prefix(prefix: &str, namespace: &Namespace) -> String {
        format!("{prefix}:ns:{}", Self::namespace_token(namespace))
    }

    fn data_key(&self, namespace: &Namespace, key: &CacheKey) -> String {
        Self::data_key_with_prefix(&self.prefix, namespace, key)
    }

    fn namespace_index_key(&self, namespace: &Namespace) -> String {
        Self::namespace_index_key_with_prefix(&self.prefix, namespace)
    }

    async fn exec_pipeline<T>(&self, pipeline: Pipeline) -> Result<T, CacheError>
    where
        T: redis::FromRedisValue,
    {
        let mut connection = self.connection.clone();
        pipeline
            .query_async(&mut connection)
            .await
            .map_err(Into::into)
    }
}

impl CacheBackend for RedisCacheBackend {
    fn ping(&self) -> CacheFuture<'_, Result<(), CacheError>> {
        Box::pin(async move {
            let mut connection = self.connection.clone();
            let _: String = redis::cmd("PING").query_async(&mut connection).await?;
            Ok(())
        })
    }

    fn get<'a>(
        &'a self,
        namespace: &'a Namespace,
        key: &'a CacheKey,
    ) -> CacheFuture<'a, Result<Option<Bytes>, CacheError>> {
        Box::pin(async move {
            let mut connection = self.connection.clone();
            let value: Option<Vec<u8>> = connection.get(self.data_key(namespace, key)).await?;
            Ok(value.map(Bytes::from))
        })
    }

    fn get_many<'a>(
        &'a self,
        namespace: &'a Namespace,
        keys: &'a [CacheKey],
    ) -> CacheFuture<'a, Result<Vec<Option<Bytes>>, CacheError>> {
        Box::pin(async move {
            let redis_keys = keys
                .iter()
                .map(|key| self.data_key(namespace, key))
                .collect::<Vec<_>>();
            let mut connection = self.connection.clone();
            let values: Vec<Option<Vec<u8>>> = connection.get(redis_keys).await?;
            Ok(values
                .into_iter()
                .map(|value| value.map(Bytes::from))
                .collect())
        })
    }

    fn set<'a>(
        &'a self,
        namespace: &'a Namespace,
        write: CacheWrite,
    ) -> CacheFuture<'a, Result<(), CacheError>> {
        Box::pin(async move {
            let data_key = self.data_key(namespace, &write.key);
            let index_key = self.namespace_index_key(namespace);
            let mut pipeline = redis::pipe();
            pipeline.atomic().cmd("SADD").arg(&index_key).arg(&data_key);
            pipeline.cmd("SET").arg(&data_key).arg(write.value.as_ref());
            if let Some(ttl) = write.ttl {
                pipeline.arg("PX").arg(ttl.as_millis() as u64);
            }
            let _: () = self.exec_pipeline(pipeline).await?;
            Ok(())
        })
    }

    fn set_many<'a>(
        &'a self,
        namespace: &'a Namespace,
        writes: &'a [CacheWrite],
    ) -> CacheFuture<'a, Result<(), CacheError>> {
        Box::pin(async move {
            if writes.is_empty() {
                return Ok(());
            }

            let index_key = self.namespace_index_key(namespace);
            let mut pipeline = redis::pipe();
            pipeline.atomic();

            for write in writes {
                let data_key = self.data_key(namespace, &write.key);
                pipeline.cmd("SADD").arg(&index_key).arg(&data_key);
                pipeline.cmd("SET").arg(&data_key).arg(write.value.as_ref());
                if let Some(ttl) = write.ttl {
                    pipeline.arg("PX").arg(ttl.as_millis() as u64);
                }
            }

            let _: () = self.exec_pipeline(pipeline).await?;
            Ok(())
        })
    }

    fn delete<'a>(
        &'a self,
        namespace: &'a Namespace,
        key: &'a CacheKey,
    ) -> CacheFuture<'a, Result<bool, CacheError>> {
        Box::pin(async move {
            let data_key = self.data_key(namespace, key);
            let index_key = self.namespace_index_key(namespace);
            let mut pipeline = redis::pipe();
            pipeline.atomic();
            pipeline.cmd("SREM").arg(&index_key).arg(&data_key);
            pipeline.cmd("DEL").arg(&data_key);
            let (_, deleted): (u64, u64) = self.exec_pipeline(pipeline).await?;
            Ok(deleted > 0)
        })
    }

    fn delete_many<'a>(
        &'a self,
        namespace: &'a Namespace,
        keys: &'a [CacheKey],
    ) -> CacheFuture<'a, Result<u64, CacheError>> {
        Box::pin(async move {
            if keys.is_empty() {
                return Ok(0);
            }

            let redis_keys = keys
                .iter()
                .map(|key| self.data_key(namespace, key))
                .collect::<Vec<_>>();
            let index_key = self.namespace_index_key(namespace);
            let mut pipeline = redis::pipe();
            pipeline.atomic();
            pipeline.cmd("SREM").arg(&index_key).arg(redis_keys.clone());
            pipeline.cmd("DEL").arg(redis_keys);
            let (_, deleted): (u64, u64) = self.exec_pipeline(pipeline).await?;
            Ok(deleted)
        })
    }

    fn clear_namespace<'a>(
        &'a self,
        namespace: &'a Namespace,
    ) -> CacheFuture<'a, Result<u64, CacheError>> {
        Box::pin(async move {
            let index_key = self.namespace_index_key(namespace);
            let mut connection = self.connection.clone();
            let keys: Vec<String> = connection.smembers(&index_key).await?;
            if keys.is_empty() {
                let _: u64 = connection.del(index_key).await?;
                return Ok(0);
            }

            let mut pipeline = redis::pipe();
            pipeline.atomic();
            pipeline.cmd("DEL").arg(keys.clone());
            pipeline.cmd("DEL").arg(index_key);
            let (deleted, _): (u64, u64) = self.exec_pipeline(pipeline).await?;
            Ok(deleted)
        })
    }

    fn exists<'a>(
        &'a self,
        namespace: &'a Namespace,
        key: &'a CacheKey,
    ) -> CacheFuture<'a, Result<bool, CacheError>> {
        Box::pin(async move {
            let mut connection = self.connection.clone();
            let exists: bool = connection.exists(self.data_key(namespace, key)).await?;
            Ok(exists)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::RedisCacheBackend;
    use crate::{CacheKey, Namespace};

    #[test]
    fn formats_prefixed_keys() {
        let namespace = Namespace::new("users.profile").expect("valid namespace");
        let key = CacheKey::new("card").expect("valid key");

        assert_eq!(
            RedisCacheBackend::namespace_index_key_with_prefix("cache", &namespace),
            "cache:ns:users.profile"
        );
        assert_eq!(
            RedisCacheBackend::data_key_with_prefix("cache", &namespace, &key),
            "cache:data:users.profile:card"
        );
    }
}
