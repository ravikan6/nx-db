use crate::CacheError;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CacheKey(Box<str>);

impl CacheKey {
    pub fn new(value: impl Into<String>) -> Result<Self, CacheError> {
        let value = value.into();
        validate_key(&value)?;
        Ok(Self(value.into_boxed_str()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for CacheKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

fn validate_key(value: &str) -> Result<(), CacheError> {
    if value.is_empty() {
        return Err(CacheError::InvalidKey("cache key cannot be empty".into()));
    }

    if !value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '/'))
    {
        return Err(CacheError::InvalidKey(format!(
            "cache key '{value}' contains unsupported characters"
        )));
    }

    Ok(())
}
