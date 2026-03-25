use crate::context::Context;
use crate::enums::RelationshipKind;
use crate::errors::DatabaseError;
use crate::query::QuerySpec;
use crate::schema::CollectionSchema;
use crate::utils::PermissionEnum;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use time::OffsetDateTime;

pub type AdapterFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, Clone, PartialEq)]
pub enum StorageValue {
    Null,
    Bool(bool),
    BoolArray(Vec<bool>),
    Int(i64),
    IntArray(Vec<i64>),
    Float(f64),
    FloatArray(Vec<f64>),
    String(String),
    StringArray(Vec<String>),
    Bytes(Vec<u8>),
    Timestamp(OffsetDateTime),
    TimestampArray(Vec<OffsetDateTime>),
    Json(String),
}

impl StorageValue {
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Self::Int(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(v) | Self::Json(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_timestamp(&self) -> Option<OffsetDateTime> {
        match self {
            Self::Timestamp(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_string_array(&self) -> Option<&[String]> {
        match self {
            Self::StringArray(v) => Some(v),
            _ => None,
        }
    }
}

pub type StorageRecord = BTreeMap<String, StorageValue>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageThroughRelation {
    pub schema: &'static CollectionSchema,
    pub local_field: &'static str,
    pub remote_field: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageRelation {
    pub kind: RelationshipKind,
    pub local_field: &'static str,
    pub remote_field: &'static str,
    pub through: Option<StorageThroughRelation>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinedStorageRecord {
    pub base: StorageRecord,
    pub related: Option<StorageRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoragePopulate {
    pub name: &'static str,
    pub schema: &'static CollectionSchema,
    pub relation: StorageRelation,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PopulatedStorageRow {
    pub base: StorageRecord,
    pub related: BTreeMap<String, Option<StorageRecord>>,
}

pub trait StorageAdapter: Send + Sync {
    fn enforces_document_filtering(&self, _action: PermissionEnum) -> bool {
        false
    }

    fn ping(&self, context: &Context) -> AdapterFuture<'_, Result<(), DatabaseError>>;

    fn create_collection(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
    ) -> AdapterFuture<'_, Result<(), DatabaseError>>;

    fn insert(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        values: StorageRecord,
    ) -> AdapterFuture<'_, Result<StorageRecord, DatabaseError>>;

    fn insert_many(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        values: Vec<StorageRecord>,
    ) -> AdapterFuture<'_, Result<Vec<StorageRecord>, DatabaseError>>;

    fn get(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        id: &str,
    ) -> AdapterFuture<'_, Result<Option<StorageRecord>, DatabaseError>>;

    fn update(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        id: &str,
        values: StorageRecord,
    ) -> AdapterFuture<'_, Result<Option<StorageRecord>, DatabaseError>>;

    fn update_many(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        query: &QuerySpec,
        values: StorageRecord,
    ) -> AdapterFuture<'_, Result<u64, DatabaseError>>;

    fn delete(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        id: &str,
    ) -> AdapterFuture<'_, Result<bool, DatabaseError>>;

    fn delete_many(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        query: &QuerySpec,
    ) -> AdapterFuture<'_, Result<u64, DatabaseError>>;

    fn find(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        query: &QuerySpec,
    ) -> AdapterFuture<'_, Result<Vec<StorageRecord>, DatabaseError>>;

    fn find_related(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        related_schema: &'static CollectionSchema,
        query: &QuerySpec,
        relation: StorageRelation,
    ) -> AdapterFuture<'_, Result<Option<Vec<JoinedStorageRecord>>, DatabaseError>> {
        let _ = (context, schema, related_schema, query, relation);
        Box::pin(async { Ok(None) })
    }

    fn find_populated(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        query: &QuerySpec,
        populates: Vec<StoragePopulate>,
    ) -> AdapterFuture<'_, Result<Option<Vec<PopulatedStorageRow>>, DatabaseError>> {
        let _ = (context, schema, query, populates);
        Box::pin(async { Ok(None) })
    }

    fn count(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        query: &QuerySpec,
    ) -> AdapterFuture<'_, Result<u64, DatabaseError>>;
}
