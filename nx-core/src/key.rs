use crate::errors::DatabaseError;
use std::borrow::Borrow;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key<const MAX: usize>(Box<str>);

impl<const MAX: usize> Key<MAX> {
    pub fn new(value: impl Into<String>) -> Result<Self, DatabaseError> {
        let value = value.into();

        if value.is_empty() {
            return Err(DatabaseError::Other("key cannot be empty".into()));
        }

        if value.len() > MAX {
            return Err(DatabaseError::Other(format!(
                "key length {} exceeds maximum {}",
                value.len(),
                MAX
            )));
        }

        Ok(Self(value.into_boxed_str()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<const MAX: usize> AsRef<str> for Key<MAX> {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl<const MAX: usize> Borrow<str> for Key<MAX> {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl<const MAX: usize> Display for Key<MAX> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<const MAX: usize> FromStr for Key<MAX> {
    type Err = DatabaseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl<const MAX: usize> TryFrom<&str> for Key<MAX> {
    type Error = DatabaseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl<const MAX: usize> TryFrom<String> for Key<MAX> {
    type Error = DatabaseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

#[cfg(test)]
mod tests {
    use super::Key;

    #[test]
    fn validates_length_once() {
        let key = Key::<8>::new("user_1").expect("key should be valid");
        assert_eq!(key.as_str(), "user_1");
        assert!(Key::<4>::new("toolong").is_err());
    }
}
