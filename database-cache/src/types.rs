use crate::CacheKey;
use bytes::Bytes;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheWrite {
    pub key: CacheKey,
    pub value: Bytes,
    pub ttl: Option<Duration>,
}

impl CacheWrite {
    pub fn new(key: CacheKey, value: impl Into<Bytes>) -> Self {
        Self {
            key,
            value: value.into(),
            ttl: None,
        }
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }
}
