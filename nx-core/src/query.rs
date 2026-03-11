use crate::errors::DatabaseError;
use crate::key::Key;
use crate::traits::storage::StorageValue;
use std::marker::PhantomData;
use time::OffsetDateTime;

#[derive(Debug, Clone, PartialEq)]
pub enum FilterOp {
    Eq(StorageValue),
    NotEq(StorageValue),
    In(Vec<StorageValue>),
    Gt(StorageValue),
    Gte(StorageValue),
    Lt(StorageValue),
    Lte(StorageValue),
    IsNull,
    IsNotNull,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Filter {
    pub field: String,
    pub op: FilterOp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sort {
    pub field: String,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct QuerySpec {
    filters: Vec<Filter>,
    sorts: Vec<Sort>,
    limit: Option<usize>,
    offset: Option<usize>,
}

impl QuerySpec {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn filter(mut self, filter: Filter) -> Self {
        self.filters.push(filter);
        self
    }

    pub fn try_filter(
        mut self,
        filter: Result<Filter, DatabaseError>,
    ) -> Result<Self, DatabaseError> {
        self.filters.push(filter?);
        Ok(self)
    }

    pub fn sort(mut self, sort: Sort) -> Self {
        self.sorts.push(sort);
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    pub fn filters(&self) -> &[Filter] {
        &self.filters
    }

    pub fn sorts(&self) -> &[Sort] {
        &self.sorts
    }

    pub fn limit_value(&self) -> Option<usize> {
        self.limit
    }

    pub fn offset_value(&self) -> Option<usize> {
        self.offset
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Field<M, T> {
    name: &'static str,
    marker: PhantomData<fn() -> (M, T)>,
}

impl<M, T> Field<M, T> {
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            marker: PhantomData,
        }
    }

    pub const fn name(&self) -> &'static str {
        self.name
    }

    pub fn eq<V>(&self, value: V) -> Filter
    where
        V: IntoQueryValue,
    {
        Filter {
            field: self.name.to_string(),
            op: FilterOp::Eq(value.into_query_value()),
        }
    }

    pub fn not_eq<V>(&self, value: V) -> Filter
    where
        V: IntoQueryValue,
    {
        Filter {
            field: self.name.to_string(),
            op: FilterOp::NotEq(value.into_query_value()),
        }
    }

    pub fn one_of<I, V>(&self, values: I) -> Filter
    where
        I: IntoIterator<Item = V>,
        V: IntoQueryValue,
    {
        Filter {
            field: self.name.to_string(),
            op: FilterOp::In(
                values
                    .into_iter()
                    .map(IntoQueryValue::into_query_value)
                    .collect(),
            ),
        }
    }

    pub fn gt<V>(&self, value: V) -> Filter
    where
        V: IntoQueryValue,
    {
        Filter {
            field: self.name.to_string(),
            op: FilterOp::Gt(value.into_query_value()),
        }
    }

    pub fn gte<V>(&self, value: V) -> Filter
    where
        V: IntoQueryValue,
    {
        Filter {
            field: self.name.to_string(),
            op: FilterOp::Gte(value.into_query_value()),
        }
    }

    pub fn lt<V>(&self, value: V) -> Filter
    where
        V: IntoQueryValue,
    {
        Filter {
            field: self.name.to_string(),
            op: FilterOp::Lt(value.into_query_value()),
        }
    }

    pub fn lte<V>(&self, value: V) -> Filter
    where
        V: IntoQueryValue,
    {
        Filter {
            field: self.name.to_string(),
            op: FilterOp::Lte(value.into_query_value()),
        }
    }

    pub fn is_null(&self) -> Filter {
        Filter {
            field: self.name.to_string(),
            op: FilterOp::IsNull,
        }
    }

    pub fn is_not_null(&self) -> Filter {
        Filter {
            field: self.name.to_string(),
            op: FilterOp::IsNotNull,
        }
    }

    pub fn asc(&self) -> Sort {
        Sort {
            field: self.name.to_string(),
            direction: SortDirection::Asc,
        }
    }

    pub fn desc(&self) -> Sort {
        Sort {
            field: self.name.to_string(),
            direction: SortDirection::Desc,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct EncodedField<M, T> {
    name: &'static str,
    encode: fn(T) -> Result<StorageValue, DatabaseError>,
    marker: PhantomData<fn() -> (M, T)>,
}

impl<M, T> EncodedField<M, T> {
    pub const fn new(
        name: &'static str,
        encode: fn(T) -> Result<StorageValue, DatabaseError>,
    ) -> Self {
        Self {
            name,
            encode,
            marker: PhantomData,
        }
    }

    pub const fn name(&self) -> &'static str {
        self.name
    }

    fn encode_value(&self, value: T) -> Result<StorageValue, DatabaseError> {
        (self.encode)(value)
    }

    pub fn eq(&self, value: T) -> Result<Filter, DatabaseError> {
        Ok(Filter {
            field: self.name.to_string(),
            op: FilterOp::Eq(self.encode_value(value)?),
        })
    }

    pub fn not_eq(&self, value: T) -> Result<Filter, DatabaseError> {
        Ok(Filter {
            field: self.name.to_string(),
            op: FilterOp::NotEq(self.encode_value(value)?),
        })
    }

    pub fn one_of<I>(&self, values: I) -> Result<Filter, DatabaseError>
    where
        I: IntoIterator<Item = T>,
    {
        let mut encoded = Vec::new();
        for value in values {
            encoded.push(self.encode_value(value)?);
        }

        Ok(Filter {
            field: self.name.to_string(),
            op: FilterOp::In(encoded),
        })
    }

    pub fn gt(&self, value: T) -> Result<Filter, DatabaseError> {
        Ok(Filter {
            field: self.name.to_string(),
            op: FilterOp::Gt(self.encode_value(value)?),
        })
    }

    pub fn gte(&self, value: T) -> Result<Filter, DatabaseError> {
        Ok(Filter {
            field: self.name.to_string(),
            op: FilterOp::Gte(self.encode_value(value)?),
        })
    }

    pub fn lt(&self, value: T) -> Result<Filter, DatabaseError> {
        Ok(Filter {
            field: self.name.to_string(),
            op: FilterOp::Lt(self.encode_value(value)?),
        })
    }

    pub fn lte(&self, value: T) -> Result<Filter, DatabaseError> {
        Ok(Filter {
            field: self.name.to_string(),
            op: FilterOp::Lte(self.encode_value(value)?),
        })
    }

    pub fn is_null(&self) -> Filter {
        Filter {
            field: self.name.to_string(),
            op: FilterOp::IsNull,
        }
    }

    pub fn is_not_null(&self) -> Filter {
        Filter {
            field: self.name.to_string(),
            op: FilterOp::IsNotNull,
        }
    }

    pub fn asc(&self) -> Sort {
        Sort {
            field: self.name.to_string(),
            direction: SortDirection::Asc,
        }
    }

    pub fn desc(&self) -> Sort {
        Sort {
            field: self.name.to_string(),
            direction: SortDirection::Desc,
        }
    }
}

pub trait IntoQueryValue {
    fn into_query_value(self) -> StorageValue;
}

impl IntoQueryValue for StorageValue {
    fn into_query_value(self) -> StorageValue {
        self
    }
}

impl IntoQueryValue for String {
    fn into_query_value(self) -> StorageValue {
        StorageValue::String(self)
    }
}

impl IntoQueryValue for &str {
    fn into_query_value(self) -> StorageValue {
        StorageValue::String(self.to_string())
    }
}

impl IntoQueryValue for bool {
    fn into_query_value(self) -> StorageValue {
        StorageValue::Bool(self)
    }
}

impl IntoQueryValue for i64 {
    fn into_query_value(self) -> StorageValue {
        StorageValue::Int(self)
    }
}

impl IntoQueryValue for i32 {
    fn into_query_value(self) -> StorageValue {
        StorageValue::Int(self.into())
    }
}

impl IntoQueryValue for f64 {
    fn into_query_value(self) -> StorageValue {
        StorageValue::Float(self)
    }
}

impl IntoQueryValue for f32 {
    fn into_query_value(self) -> StorageValue {
        StorageValue::Float(self.into())
    }
}

impl IntoQueryValue for OffsetDateTime {
    fn into_query_value(self) -> StorageValue {
        StorageValue::Timestamp(self)
    }
}

impl<const MAX: usize> IntoQueryValue for Key<MAX> {
    fn into_query_value(self) -> StorageValue {
        StorageValue::String(self.to_string())
    }
}

impl<const MAX: usize> IntoQueryValue for &Key<MAX> {
    fn into_query_value(self) -> StorageValue {
        StorageValue::String(self.to_string())
    }
}
