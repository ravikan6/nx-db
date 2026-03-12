use crate::{CacheBackend, CacheError, CacheFuture, CacheKey, CacheWrite, Namespace};
use bytes::Bytes;
use moka::future::Cache;
use moka::Expiry;
use parking_lot::RwLock;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

struct MemoryCacheExpiry;

// Use a very long duration for "never expire" since returning None 
// can sometimes lead to using builder-level defaults if they existed.
const NEVER_EXPIRE: Duration = Duration::from_secs(100 * 365 * 24 * 60 * 60);

impl Expiry<String, (Bytes, Option<Duration>)> for MemoryCacheExpiry {
    fn expire_after_create(
        &self,
        _key: &String,
        value: &(Bytes, Option<Duration>),
        _current_time: Instant,
    ) -> Option<Duration> {
        value.1.or(Some(NEVER_EXPIRE))
    }

    fn expire_after_update(
        &self,
        _key: &String,
        value: &(Bytes, Option<Duration>),
        _updated_at: Instant,
        _duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        value.1.or(Some(NEVER_EXPIRE))
    }
}

pub struct MemoryCacheBackend {
    cache: Cache<String, (Bytes, Option<Duration>)>,
    namespaces: Arc<RwLock<std::collections::HashMap<String, HashSet<String>>>>,
}

impl MemoryCacheBackend {
    pub fn new(max_capacity: u64) -> Self {
        Self {
            cache: Cache::builder()
                .max_capacity(max_capacity)
                .expire_after(MemoryCacheExpiry)
                .build(),
            namespaces: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }

    fn qualified_key(namespace: &Namespace, key: &CacheKey) -> String {
        format!("{}:{}", namespace.as_str(), key.as_str())
    }

    fn register_key(&self, namespace: &str, qualified_key: String) {
        let mut namespaces = self.namespaces.write();
        namespaces
            .entry(namespace.to_string())
            .or_default()
            .insert(qualified_key);
    }
}

impl Default for MemoryCacheBackend {
    fn default() -> Self {
        Self::new(10_000)
    }
}

impl CacheBackend for MemoryCacheBackend {
    fn ping(&self) -> CacheFuture<'_, Result<(), CacheError>> {
        Box::pin(async { Ok(()) })
    }

    fn get<'a>(
        &'a self,
        namespace: &'a Namespace,
        key: &'a CacheKey,
    ) -> CacheFuture<'a, Result<Option<Bytes>, CacheError>> {
        let qualified = Self::qualified_key(namespace, key);
        Box::pin(async move { Ok(self.cache.get(&qualified).await.map(|(v, _)| v)) })
    }

    fn get_many<'a>(
        &'a self,
        namespace: &'a Namespace,
        keys: &'a [CacheKey],
    ) -> CacheFuture<'a, Result<Vec<Option<Bytes>>, CacheError>> {
        Box::pin(async move {
            let mut results = Vec::with_capacity(keys.len());
            for key in keys {
                let qualified = Self::qualified_key(namespace, key);
                results.push(self.cache.get(&qualified).await.map(|(v, _)| v));
            }
            Ok(results)
        })
    }

    fn set<'a>(
        &'a self,
        namespace: &'a Namespace,
        write: CacheWrite,
    ) -> CacheFuture<'a, Result<(), CacheError>> {
        let qualified = Self::qualified_key(namespace, &write.key);
        let namespace_str = namespace.to_string();
        Box::pin(async move {
            self.cache
                .insert(qualified.clone(), (write.value, write.ttl))
                .await;
            self.register_key(&namespace_str, qualified);
            Ok(())
        })
    }

    fn set_many<'a>(
        &'a self,
        namespace: &'a Namespace,
        writes: &'a [CacheWrite],
    ) -> CacheFuture<'a, Result<(), CacheError>> {
        let namespace_str = namespace.to_string();
        Box::pin(async move {
            for write in writes {
                let qualified = Self::qualified_key(namespace, &write.key);
                self.cache
                    .insert(qualified.clone(), (write.value.clone(), write.ttl))
                    .await;
                self.register_key(&namespace_str, qualified);
            }
            Ok(())
        })
    }

    fn delete<'a>(
        &'a self,
        namespace: &'a Namespace,
        key: &'a CacheKey,
    ) -> CacheFuture<'a, Result<bool, CacheError>> {
        let qualified = Self::qualified_key(namespace, key);
        Box::pin(async move { Ok(self.cache.remove(&qualified).await.is_some()) })
    }

    fn delete_many<'a>(
        &'a self,
        namespace: &'a Namespace,
        keys: &'a [CacheKey],
    ) -> CacheFuture<'a, Result<u64, CacheError>> {
        Box::pin(async move {
            let mut count = 0;
            for key in keys {
                let qualified = Self::qualified_key(namespace, key);
                if self.cache.remove(&qualified).await.is_some() {
                    count += 1;
                }
            }
            Ok(count)
        })
    }

    fn clear_namespace<'a>(
        &'a self,
        namespace: &'a Namespace,
    ) -> CacheFuture<'a, Result<u64, CacheError>> {
        let namespace_str = namespace.to_string();
        Box::pin(async move {
            let keys = {
                let mut namespaces = self.namespaces.write();
                namespaces.remove(&namespace_str)
            };

            if let Some(keys) = keys {
                let count = keys.len() as u64;
                for key in keys {
                    self.cache.remove(&key).await;
                }
                Ok(count)
            } else {
                Ok(0)
            }
        })
    }

    fn exists<'a>(
        &'a self,
        namespace: &'a Namespace,
        key: &'a CacheKey,
    ) -> CacheFuture<'a, Result<bool, CacheError>> {
        let qualified = Self::qualified_key(namespace, key);
        Box::pin(async move { Ok(self.cache.contains_key(&qualified)) })
    }
}
