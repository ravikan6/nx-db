use thiserror::Error;

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("invalid namespace: {0}")]
    InvalidNamespace(String),
    #[error("invalid cache key: {0}")]
    InvalidKey(String),
    #[error("cache backend error: {0}")]
    Backend(String),
    #[cfg(feature = "redis")]
    #[error(transparent)]
    Redis(#[from] redis::RedisError),
}
