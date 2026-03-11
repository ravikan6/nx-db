use crate::{CacheError, CacheKey, CacheWrite, Namespace};
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;

pub type CacheFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait CacheBackend: Send + Sync + 'static {
    fn ping(&self) -> CacheFuture<'_, Result<(), CacheError>>;

    fn get<'a>(
        &'a self,
        namespace: &'a Namespace,
        key: &'a CacheKey,
    ) -> CacheFuture<'a, Result<Option<Bytes>, CacheError>>;

    fn get_many<'a>(
        &'a self,
        namespace: &'a Namespace,
        keys: &'a [CacheKey],
    ) -> CacheFuture<'a, Result<Vec<Option<Bytes>>, CacheError>>;

    fn set<'a>(
        &'a self,
        namespace: &'a Namespace,
        write: CacheWrite,
    ) -> CacheFuture<'a, Result<(), CacheError>>;

    fn set_many<'a>(
        &'a self,
        namespace: &'a Namespace,
        writes: &'a [CacheWrite],
    ) -> CacheFuture<'a, Result<(), CacheError>>;

    fn delete<'a>(
        &'a self,
        namespace: &'a Namespace,
        key: &'a CacheKey,
    ) -> CacheFuture<'a, Result<bool, CacheError>>;

    fn delete_many<'a>(
        &'a self,
        namespace: &'a Namespace,
        keys: &'a [CacheKey],
    ) -> CacheFuture<'a, Result<u64, CacheError>>;

    fn clear_namespace<'a>(
        &'a self,
        namespace: &'a Namespace,
    ) -> CacheFuture<'a, Result<u64, CacheError>>;

    fn exists<'a>(
        &'a self,
        namespace: &'a Namespace,
        key: &'a CacheKey,
    ) -> CacheFuture<'a, Result<bool, CacheError>>;
}

#[derive(Debug, Clone)]
pub struct Cache<B> {
    backend: B,
}

impl<B> Cache<B> {
    pub fn new(backend: B) -> Self {
        Self { backend }
    }

    pub fn backend(&self) -> &B {
        &self.backend
    }

    pub fn namespace(&self, namespace: impl Into<Namespace>) -> NamespacedCache<'_, B> {
        NamespacedCache {
            backend: &self.backend,
            namespace: namespace.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NamespacedCache<'a, B> {
    backend: &'a B,
    namespace: Namespace,
}

impl<'a, B> NamespacedCache<'a, B> {
    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }
}

impl<'a, B> NamespacedCache<'a, B>
where
    B: CacheBackend,
{
    pub fn ping(&self) -> CacheFuture<'_, Result<(), CacheError>> {
        self.backend.ping()
    }

    pub fn get<'b>(
        &'b self,
        key: &'b CacheKey,
    ) -> CacheFuture<'b, Result<Option<Bytes>, CacheError>> {
        self.backend.get(&self.namespace, key)
    }

    pub fn get_many<'b>(
        &'b self,
        keys: &'b [CacheKey],
    ) -> CacheFuture<'b, Result<Vec<Option<Bytes>>, CacheError>> {
        self.backend.get_many(&self.namespace, keys)
    }

    pub fn set(&self, write: CacheWrite) -> CacheFuture<'_, Result<(), CacheError>> {
        self.backend.set(&self.namespace, write)
    }

    pub fn set_many<'b>(
        &'b self,
        writes: &'b [CacheWrite],
    ) -> CacheFuture<'b, Result<(), CacheError>> {
        self.backend.set_many(&self.namespace, writes)
    }

    pub fn delete<'b>(&'b self, key: &'b CacheKey) -> CacheFuture<'b, Result<bool, CacheError>> {
        self.backend.delete(&self.namespace, key)
    }

    pub fn delete_many<'b>(
        &'b self,
        keys: &'b [CacheKey],
    ) -> CacheFuture<'b, Result<u64, CacheError>> {
        self.backend.delete_many(&self.namespace, keys)
    }

    pub fn clear(&self) -> CacheFuture<'_, Result<u64, CacheError>> {
        self.backend.clear_namespace(&self.namespace)
    }

    pub fn exists<'b>(&'b self, key: &'b CacheKey) -> CacheFuture<'b, Result<bool, CacheError>> {
        self.backend.exists(&self.namespace, key)
    }
}

impl From<Namespace> for String {
    fn from(value: Namespace) -> Self {
        value.to_string()
    }
}
