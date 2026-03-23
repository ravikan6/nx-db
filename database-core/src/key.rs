use crate::errors::DatabaseError;
use std::borrow::Borrow;
use std::fmt::Write as _;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub const GENERATED_ID_MIN_LENGTH: usize = 36;
const GENERATED_ID_LENGTH: usize = 48;
static GENERATED_ID_COUNTER: AtomicU64 = AtomicU64::new(0);
static GENERATED_ID_SEED: OnceLock<u64> = OnceLock::new();

pub trait GenerateId: Sized {
    fn generate() -> Result<Self, DatabaseError>;
}

fn generated_id_seed() -> u64 {
    *GENERATED_ID_SEED.get_or_init(|| {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let process = u64::from(std::process::id());
        let stack = &now as *const u64 as usize as u64;
        now.rotate_left(17) ^ process.rotate_left(7) ^ stack ^ 0x9e37_79b9_7f4a_7c15
    })
}

pub fn generate_id_string() -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let counter = GENERATED_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let seed = generated_id_seed();

    let mut id = String::with_capacity(GENERATED_ID_LENGTH);
    write!(&mut id, "{timestamp:016x}{counter:016x}{seed:016x}")
        .expect("writing to String cannot fail");
    id
}

impl GenerateId for String {
    fn generate() -> Result<Self, DatabaseError> {
        Ok(generate_id_string())
    }
}

impl<const MAX: usize> GenerateId for Key<MAX> {
    fn generate() -> Result<Self, DatabaseError> {
        Self::new(generate_id_string())
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
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

    pub fn generate() -> Result<Self, DatabaseError> {
        <Self as GenerateId>::generate()
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
    use super::{GENERATED_ID_MIN_LENGTH, Key, generate_id_string};
    use std::collections::BTreeSet;

    #[test]
    fn validates_length_once() {
        let key = Key::<8>::new("user_1").expect("key should be valid");
        assert_eq!(key.as_str(), "user_1");
        assert!(Key::<4>::new("toolong").is_err());
    }

    #[test]
    fn generated_ids_are_long_enough_and_unique() {
        let mut seen = BTreeSet::new();

        for _ in 0..512 {
            let id = generate_id_string();
            assert!(id.len() >= GENERATED_ID_MIN_LENGTH);
            assert!(seen.insert(id), "generated id must be unique");
        }
    }

    #[test]
    fn generated_key_respects_length_constraints() {
        let key = Key::<48>::generate().expect("generated key should fit");
        assert!(key.as_str().len() >= GENERATED_ID_MIN_LENGTH);
        assert!(Key::<35>::generate().is_err());
    }
}
