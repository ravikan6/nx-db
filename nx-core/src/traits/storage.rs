use crate::context::Context;
use crate::errors::DatabaseError;
use crate::schema::CollectionSchema;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use time::OffsetDateTime;

pub type AdapterFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, Clone, PartialEq)]
pub enum StorageValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    StringArray(Vec<String>),
    Bytes(Vec<u8>),
    Timestamp(OffsetDateTime),
}

pub type StorageRecord = BTreeMap<String, StorageValue>;

pub trait StorageAdapter: Send + Sync {
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

    fn delete(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        id: &str,
    ) -> AdapterFuture<'_, Result<bool, DatabaseError>>;
}
