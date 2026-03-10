use crate::errors::DatabaseError;
use crate::schema::{AttributeSchema, CollectionSchema};
use std::future::Future;
use std::pin::Pin;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, Clone, Copy)]
pub struct ResolveContext<'a> {
    pub collection: &'static CollectionSchema,
    pub attribute: &'static AttributeSchema,
    pub document_id: Option<&'a str>,
}

pub trait Resolver<Document>: Send + Sync + 'static {
    type Output;

    fn name(&self) -> &'static str;

    fn resolve<'a>(
        &'a self,
        document: &'a Document,
        context: ResolveContext<'a>,
    ) -> BoxFuture<'a, Result<Self::Output, DatabaseError>>;
}
