use crate::context::Context;
use crate::errors::DatabaseError;
use std::future::Future;

pub trait Adapter: Sized {
    type Pool;

    fn new(pool: Self::Pool, context: Context) -> Self;
    fn pool(&self) -> &Self::Pool;
    fn context(&self) -> &Context;

    fn ping(&self) -> impl Future<Output=Result<(), DatabaseError>> + Send;

    fn create<'a>(
        &'a self,
        collection: &'a str,
        id: &'a str,
        payload: &'a str,
    ) -> impl Future<Output=Result<(), DatabaseError>> + Send + 'a;

    fn read<'a>(
        &'a self,
        collection: &'a str,
        id: &'a str,
    ) -> impl Future<Output=Result<Option<String>, DatabaseError>> + Send + 'a;

    fn update<'a>(
        &'a self,
        collection: &'a str,
        id: &'a str,
        payload: &'a str,
    ) -> impl Future<Output=Result<bool, DatabaseError>> + Send + 'a;

    fn delete<'a>(
        &'a self,
        collection: &'a str,
        id: &'a str,
    ) -> impl Future<Output=Result<bool, DatabaseError>> + Send + 'a;
}
