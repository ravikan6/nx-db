use crate::errors::DatabaseError;
use std::marker::PhantomData;

pub trait Codec: Send + Sync + 'static {
    type Input;
    type Stored;
    type Output;

    fn name(&self) -> &'static str;
    fn encode(&self, value: Self::Input) -> Result<Self::Stored, DatabaseError>;
    fn decode(&self, value: Self::Stored) -> Result<Self::Output, DatabaseError>;
}

#[derive(Debug)]
pub struct IdentityCodec<T>(PhantomData<fn() -> T>);

impl<T> IdentityCodec<T> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> Default for IdentityCodec<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Clone for IdentityCodec<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for IdentityCodec<T> {}

impl<T> Codec for IdentityCodec<T>
where
    T: Send + Sync + 'static,
{
    type Input = T;
    type Stored = T;
    type Output = T;

    fn name(&self) -> &'static str {
        "identity"
    }

    fn encode(&self, value: Self::Input) -> Result<Self::Stored, DatabaseError> {
        Ok(value)
    }

    fn decode(&self, value: Self::Stored) -> Result<Self::Output, DatabaseError> {
        Ok(value)
    }
}
