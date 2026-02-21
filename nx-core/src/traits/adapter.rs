use crate::context::Context;
use crate::errors::CoreError;
use std::future::Future;

pub trait Adapter: Sized {
    type Pool;

    fn new(pool: Self::Pool, context: Context) -> Self;
    fn pool(&self) -> &Self::Pool;
    fn context(&self) -> &Context;

    fn ping(&self) -> impl Future<Output=Result<(), CoreError>> + Send;

    fn create<'a>(
        &'a self,
        collection: &'a str,
        id: &'a str,
        payload: &'a str,
    ) -> impl Future<Output=Result<(), CoreError>> + Send + 'a;

    fn read<'a>(
        &'a self,
        collection: &'a str,
        id: &'a str,
    ) -> impl Future<Output=Result<Option<String>, CoreError>> + Send + 'a;

    fn update<'a>(
        &'a self,
        collection: &'a str,
        id: &'a str,
        payload: &'a str,
    ) -> impl Future<Output=Result<bool, CoreError>> + Send + 'a;

    fn delete<'a>(
        &'a self,
        collection: &'a str,
        id: &'a str,
    ) -> impl Future<Output=Result<bool, CoreError>> + Send + 'a;
}
