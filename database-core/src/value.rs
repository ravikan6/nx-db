use crate::errors::DatabaseError;
use crate::key::Key;
use crate::traits::storage::{StorageRecord, StorageValue};
use time::OffsetDateTime;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Patch<T> {
    Keep,
    Set(T),
}

impl<T> Patch<T> {
    pub fn set(value: T) -> Self {
        Self::Set(value)
    }

    pub fn keep() -> Self {
        Self::Keep
    }
}

impl<T> Default for Patch<T> {
    fn default() -> Self {
        Self::Keep
    }
}

impl<T> From<T> for Patch<T> {
    fn from(value: T) -> Self {
        Self::Set(value)
    }
}

pub trait IntoStorage {
    fn into_storage(self) -> StorageValue;
}

pub trait FromStorage: Sized {
    fn from_storage(value: StorageValue) -> Result<Self, DatabaseError>;
}

pub fn insert_value<T>(record: &mut StorageRecord, key: impl Into<String>, value: T)
where
    T: IntoStorage,
{
    record.insert(key.into(), value.into_storage());
}

pub fn take_required<T>(record: &mut StorageRecord, key: &str) -> Result<T, DatabaseError>
where
    T: FromStorage,
{
    let value = record
        .remove(key)
        .ok_or_else(|| DatabaseError::Other(format!("missing required field '{key}'")))?;
    T::from_storage(value)
}

pub fn get_required<T>(record: &StorageRecord, key: &str) -> Result<T, DatabaseError>
where
    T: FromStorage,
{
    let value = record
        .get(key)
        .ok_or_else(|| DatabaseError::Other(format!("missing required field '{key}'")))?;
    T::from_storage(value.clone())
}

pub fn take_optional<T>(record: &mut StorageRecord, key: &str) -> Result<Option<T>, DatabaseError>
where
    T: FromStorage,
{
    match record.remove(key) {
        Some(StorageValue::Null) | None => Ok(None),
        Some(value) => T::from_storage(value).map(Some),
    }
}

pub fn get_optional<T>(record: &StorageRecord, key: &str) -> Result<Option<T>, DatabaseError>
where
    T: FromStorage,
{
    match record.get(key) {
        Some(StorageValue::Null) | None => Ok(None),
        Some(value) => T::from_storage(value.clone()).map(Some),
    }
}

impl IntoStorage for StorageValue {
    fn into_storage(self) -> StorageValue {
        self
    }
}

impl FromStorage for StorageValue {
    fn from_storage(value: StorageValue) -> Result<Self, DatabaseError> {
        Ok(value)
    }
}

impl IntoStorage for String {
    fn into_storage(self) -> StorageValue {
        StorageValue::String(self)
    }
}

impl IntoStorage for &str {
    fn into_storage(self) -> StorageValue {
        StorageValue::String(self.to_string())
    }
}

impl FromStorage for String {
    fn from_storage(value: StorageValue) -> Result<Self, DatabaseError> {
        match value {
            StorageValue::String(value) => Ok(value),
            StorageValue::Json(value) => Ok(value),
            other => Err(DatabaseError::Other(format!(
                "expected string storage value, got {other:?}"
            ))),
        }
    }
}

impl IntoStorage for bool {
    fn into_storage(self) -> StorageValue {
        StorageValue::Bool(self)
    }
}

impl FromStorage for bool {
    fn from_storage(value: StorageValue) -> Result<Self, DatabaseError> {
        match value {
            StorageValue::Bool(value) => Ok(value),
            other => Err(DatabaseError::Other(format!(
                "expected bool storage value, got {other:?}"
            ))),
        }
    }
}

impl IntoStorage for i64 {
    fn into_storage(self) -> StorageValue {
        StorageValue::Int(self)
    }
}

impl IntoStorage for i32 {
    fn into_storage(self) -> StorageValue {
        StorageValue::Int(self.into())
    }
}

impl FromStorage for i64 {
    fn from_storage(value: StorageValue) -> Result<Self, DatabaseError> {
        match value {
            StorageValue::Int(value) => Ok(value),
            other => Err(DatabaseError::Other(format!(
                "expected int storage value, got {other:?}"
            ))),
        }
    }
}

impl FromStorage for i32 {
    fn from_storage(value: StorageValue) -> Result<Self, DatabaseError> {
        let value = i64::from_storage(value)?;
        value
            .try_into()
            .map_err(|_| DatabaseError::Other(format!("int value {value} does not fit in i32")))
    }
}

impl IntoStorage for f64 {
    fn into_storage(self) -> StorageValue {
        StorageValue::Float(self)
    }
}

impl IntoStorage for f32 {
    fn into_storage(self) -> StorageValue {
        StorageValue::Float(self.into())
    }
}

impl FromStorage for f64 {
    fn from_storage(value: StorageValue) -> Result<Self, DatabaseError> {
        match value {
            StorageValue::Float(value) => Ok(value),
            other => Err(DatabaseError::Other(format!(
                "expected float storage value, got {other:?}"
            ))),
        }
    }
}

impl FromStorage for f32 {
    fn from_storage(value: StorageValue) -> Result<Self, DatabaseError> {
        Ok(f64::from_storage(value)? as f32)
    }
}

impl IntoStorage for OffsetDateTime {
    fn into_storage(self) -> StorageValue {
        StorageValue::Timestamp(self)
    }
}

impl FromStorage for OffsetDateTime {
    fn from_storage(value: StorageValue) -> Result<Self, DatabaseError> {
        match value {
            StorageValue::Timestamp(value) => Ok(value),
            other => Err(DatabaseError::Other(format!(
                "expected timestamp storage value, got {other:?}"
            ))),
        }
    }
}

impl<const MAX: usize> IntoStorage for Key<MAX> {
    fn into_storage(self) -> StorageValue {
        StorageValue::String(self.to_string())
    }
}

impl<const MAX: usize> FromStorage for Key<MAX> {
    fn from_storage(value: StorageValue) -> Result<Self, DatabaseError> {
        match value {
            StorageValue::String(value) => Key::new(value),
            other => Err(DatabaseError::Other(format!(
                "expected key storage value, got {other:?}"
            ))),
        }
    }
}

impl<T> IntoStorage for Option<T>
where
    T: IntoStorage,
{
    fn into_storage(self) -> StorageValue {
        match self {
            Some(value) => value.into_storage(),
            None => StorageValue::Null,
        }
    }
}

impl<T> FromStorage for Option<T>
where
    T: FromStorage,
{
    fn from_storage(value: StorageValue) -> Result<Self, DatabaseError> {
        match value {
            StorageValue::Null => Ok(None),
            value => T::from_storage(value).map(Some),
        }
    }
}

impl IntoStorage for Vec<String> {
    fn into_storage(self) -> StorageValue {
        StorageValue::StringArray(self)
    }
}

impl FromStorage for Vec<String> {
    fn from_storage(value: StorageValue) -> Result<Self, DatabaseError> {
        match value {
            StorageValue::StringArray(value) => Ok(value),
            other => Err(DatabaseError::Other(format!(
                "expected string array storage value, got {other:?}"
            ))),
        }
    }
}

impl IntoStorage for Vec<bool> {
    fn into_storage(self) -> StorageValue {
        StorageValue::BoolArray(self)
    }
}

impl FromStorage for Vec<bool> {
    fn from_storage(value: StorageValue) -> Result<Self, DatabaseError> {
        match value {
            StorageValue::BoolArray(value) => Ok(value),
            other => Err(DatabaseError::Other(format!(
                "expected bool array storage value, got {other:?}"
            ))),
        }
    }
}

impl IntoStorage for Vec<i64> {
    fn into_storage(self) -> StorageValue {
        StorageValue::IntArray(self)
    }
}

impl FromStorage for Vec<i64> {
    fn from_storage(value: StorageValue) -> Result<Self, DatabaseError> {
        match value {
            StorageValue::IntArray(value) => Ok(value),
            other => Err(DatabaseError::Other(format!(
                "expected int array storage value, got {other:?}"
            ))),
        }
    }
}

impl IntoStorage for Vec<f64> {
    fn into_storage(self) -> StorageValue {
        StorageValue::FloatArray(self)
    }
}

impl FromStorage for Vec<f64> {
    fn from_storage(value: StorageValue) -> Result<Self, DatabaseError> {
        match value {
            StorageValue::FloatArray(value) => Ok(value),
            other => Err(DatabaseError::Other(format!(
                "expected float array storage value, got {other:?}"
            ))),
        }
    }
}

impl IntoStorage for Vec<OffsetDateTime> {
    fn into_storage(self) -> StorageValue {
        StorageValue::TimestampArray(self)
    }
}

impl FromStorage for Vec<OffsetDateTime> {
    fn from_storage(value: StorageValue) -> Result<Self, DatabaseError> {
        match value {
            StorageValue::TimestampArray(value) => Ok(value),
            other => Err(DatabaseError::Other(format!(
                "expected timestamp array storage value, got {other:?}"
            ))),
        }
    }
}

// ── Relationship population ────────────────────────────────────────────────

/// Represents a relationship field that may or may not have been eagerly loaded.
///
/// Use `Populated::NotLoaded` for the default state after a plain `find`/`get`,
/// and `Populated::Loaded(...)` after an explicit populate call.
///
/// # Serde / bincode round-trip
///
/// `Populated` intentionally serialises as `null` (or a 0-byte tag for bincode)
/// and always deserialises as `NotLoaded`.  This means populated relationships
/// are never written to cache or the database — which is the correct semantic.
#[derive(Debug, Clone, PartialEq)]
pub enum Populated<T> {
    /// The relationship has not been fetched yet.
    NotLoaded,
    /// The relationship was fetched.
    /// `Some(T)` — entity found; `None` — FK was `NULL` or entity is missing.
    Loaded(Option<T>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum RelationOne<T> {
    NotLoaded,
    Loaded(Option<T>),
}

impl<T> Default for RelationOne<T> {
    fn default() -> Self {
        Self::NotLoaded
    }
}

impl<T> RelationOne<T> {
    pub fn is_not_loaded(&self) -> bool {
        matches!(self, Self::NotLoaded)
    }

    pub fn get(&self) -> Option<&T> {
        match self {
            Self::Loaded(Some(value)) => Some(value),
            _ => None,
        }
    }

    pub fn into_loaded(self) -> Option<T> {
        match self {
            Self::Loaded(value) => value,
            Self::NotLoaded => None,
        }
    }

    pub fn is_loaded(&self) -> bool {
        !matches!(self, Self::NotLoaded)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RelationMany<T> {
    NotLoaded,
    Loaded(Vec<T>),
}

impl<T> Default for RelationMany<T> {
    fn default() -> Self {
        Self::NotLoaded
    }
}

impl<T> RelationMany<T> {
    pub fn is_not_loaded(&self) -> bool {
        matches!(self, Self::NotLoaded)
    }

    pub fn as_slice(&self) -> Option<&[T]> {
        match self {
            Self::Loaded(values) => Some(values.as_slice()),
            Self::NotLoaded => None,
        }
    }

    pub fn into_loaded(self) -> Option<Vec<T>> {
        match self {
            Self::Loaded(values) => Some(values),
            Self::NotLoaded => None,
        }
    }

    pub fn is_loaded(&self) -> bool {
        !matches!(self, Self::NotLoaded)
    }
}

impl<T> Default for Populated<T> {
    fn default() -> Self {
        Self::NotLoaded
    }
}

impl<T> Populated<T> {
    pub fn is_not_loaded(&self) -> bool {
        matches!(self, Self::NotLoaded)
    }

    /// Return a reference to the loaded entity, if any.
    pub fn get(&self) -> Option<&T> {
        match self {
            Self::Loaded(Some(v)) => Some(v),
            _ => None,
        }
    }

    /// Consume and return the inner `Option`, or `None` if not loaded.
    pub fn into_loaded(self) -> Option<T> {
        match self {
            Self::Loaded(v) => v,
            Self::NotLoaded => None,
        }
    }

    /// Returns `true` if `populate_*` has been called for this field.
    pub fn is_loaded(&self) -> bool {
        !matches!(self, Self::NotLoaded)
    }

    /// Map the inner value without changing the loaded state.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Populated<U> {
        match self {
            Self::NotLoaded => Populated::NotLoaded,
            Self::Loaded(opt) => Populated::Loaded(opt.map(f)),
        }
    }
}

impl<T: serde::Serialize> serde::Serialize for Populated<T> {
    /// Always serialises as `null` so populated data is never persisted to cache.
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_none()
    }
}

impl<'de, T: serde::de::DeserializeOwned> serde::Deserialize<'de> for Populated<T> {
    /// Always deserialises as `NotLoaded` regardless of the stored value.
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let _ = Option::<()>::deserialize(d)?;
        Ok(Self::NotLoaded)
    }
}

impl<T: serde::Serialize> serde::Serialize for RelationOne<T> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_none()
    }
}

impl<'de, T: serde::de::DeserializeOwned> serde::Deserialize<'de> for RelationOne<T> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let _ = Option::<()>::deserialize(d)?;
        Ok(Self::NotLoaded)
    }
}

impl<T: serde::Serialize> serde::Serialize for RelationMany<T> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_none()
    }
}

impl<'de, T: serde::de::DeserializeOwned> serde::Deserialize<'de> for RelationMany<T> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let _ = Option::<()>::deserialize(d)?;
        Ok(Self::NotLoaded)
    }
}

// bincode 2.0 encode/decode — encode as a single 0 byte; decode always returns NotLoaded.
impl<T: bincode::Encode> bincode::Encode for Populated<T> {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        0u8.encode(encoder)
    }
}

impl<Context, T: bincode::Decode<Context>> bincode::Decode<Context> for Populated<T> {
    fn decode<D: bincode::de::Decoder<Context = Context>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let _ = u8::decode(decoder)?;
        Ok(Self::NotLoaded)
    }
}

impl<'de, Context, T: bincode::BorrowDecode<'de, Context>> bincode::BorrowDecode<'de, Context>
    for Populated<T>
{
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de, Context = Context>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let _ = u8::borrow_decode(decoder)?;
        Ok(Self::NotLoaded)
    }
}

impl<T: bincode::Encode> bincode::Encode for RelationOne<T> {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        0u8.encode(encoder)
    }
}

impl<Context, T: bincode::Decode<Context>> bincode::Decode<Context> for RelationOne<T> {
    fn decode<D: bincode::de::Decoder<Context = Context>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let _ = u8::decode(decoder)?;
        Ok(Self::NotLoaded)
    }
}

impl<'de, Context, T: bincode::BorrowDecode<'de, Context>> bincode::BorrowDecode<'de, Context>
    for RelationOne<T>
{
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de, Context = Context>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let _ = u8::borrow_decode(decoder)?;
        Ok(Self::NotLoaded)
    }
}

impl<T: bincode::Encode> bincode::Encode for RelationMany<T> {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        0u8.encode(encoder)
    }
}

impl<Context, T: bincode::Decode<Context>> bincode::Decode<Context> for RelationMany<T> {
    fn decode<D: bincode::de::Decoder<Context = Context>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let _ = u8::decode(decoder)?;
        Ok(Self::NotLoaded)
    }
}

impl<'de, Context, T: bincode::BorrowDecode<'de, Context>> bincode::BorrowDecode<'de, Context>
    for RelationMany<T>
{
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de, Context = Context>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let _ = u8::borrow_decode(decoder)?;
        Ok(Self::NotLoaded)
    }
}

#[cfg(test)]
mod tests {
    use super::{FromStorage, IntoStorage, Patch, insert_value, take_optional, take_required};
    use crate::key::Key;
    use crate::traits::storage::{StorageRecord, StorageValue};

    #[test]
    fn converts_keys_and_scalars() {
        let key = Key::<16>::new("usr_1").expect("key should be valid");
        let stored = key.clone().into_storage();
        let roundtrip = Key::<16>::from_storage(stored).expect("key should decode");
        assert_eq!(roundtrip, key);
    }

    #[test]
    fn manages_record_helpers() {
        let mut record = StorageRecord::new();
        insert_value(&mut record, "name", "Ravi");
        insert_value::<Option<String>>(&mut record, "nickname", None);

        let name: String = take_required(&mut record, "name").expect("name should exist");
        let nickname: Option<String> =
            take_optional(&mut record, "nickname").expect("nickname should decode");

        assert_eq!(name, "Ravi");
        assert_eq!(nickname, None);
        assert_eq!(record.get("nickname"), None);
        assert_eq!(
            Option::<String>::from_storage(StorageValue::Null).expect("null should decode"),
            None
        );
    }

    #[test]
    fn patch_defaults_to_keep() {
        let patch = Patch::<String>::default();
        assert_eq!(patch, Patch::Keep);
        assert_eq!(
            Patch::from("Ravi".to_string()),
            Patch::Set("Ravi".to_string())
        );
    }
}
