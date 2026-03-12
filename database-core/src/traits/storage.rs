use crate::context::Context;
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

pub type StorageRecord = BTreeMap<String, StorageValue>;

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

    fn count(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        query: &QuerySpec,
    ) -> AdapterFuture<'_, Result<u64, DatabaseError>>;
}
