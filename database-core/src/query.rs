use crate::enums::RelationshipKind;
use crate::errors::DatabaseError;
use crate::key::Key;
use crate::model::Model;
use crate::traits::storage::StorageValue;
use crate::value::{RelationMany, RelationOne};
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
    Contains(StorageValue),
    StartsWith(StorageValue),
    EndsWith(StorageValue),
    TextSearch(StorageValue),
    IsNull,
    IsNotNull,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    Field { field: &'static str, op: FilterOp },
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
}

impl Filter {
    pub fn field(field: &'static str, op: FilterOp) -> Self {
        Self::Field { field, op }
    }

    pub fn and<I: IntoIterator<Item = Filter>>(filters: I) -> Self {
        Self::And(filters.into_iter().collect())
    }

    pub fn or<I: IntoIterator<Item = Filter>>(filters: I) -> Self {
        Self::Or(filters.into_iter().collect())
    }

    pub fn not(filter: Filter) -> Self {
        Self::Not(Box::new(filter))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sort {
    pub field: &'static str,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct QuerySpec {
    filters: Vec<Filter>,
    sorts: Vec<Sort>,
    selects: Vec<&'static str>,
    includes: Vec<QueryInclude>,
    limit: Option<usize>,
    offset: Option<usize>,
}

impl From<Filter> for QuerySpec {
    fn from(filter: Filter) -> Self {
        Self::new().filter(filter)
    }
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

    pub fn select(mut self, fields: Vec<&'static str>) -> Self {
        self.selects = fields;
        self
    }

    pub fn include<I>(mut self, include: I) -> Self
    where
        I: Into<QueryInclude>,
    {
        self.includes.push(include.into());
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

    pub fn selects(&self) -> &[&'static str] {
        &self.selects
    }

    pub fn includes(&self) -> &[QueryInclude] {
        &self.includes
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
        Filter::field(self.name, FilterOp::Eq(value.into_query_value()))
    }

    pub fn not_eq<V>(&self, value: V) -> Filter
    where
        V: IntoQueryValue,
    {
        Filter::field(self.name, FilterOp::NotEq(value.into_query_value()))
    }

    pub fn one_of<I, V>(&self, values: I) -> Filter
    where
        I: IntoIterator<Item = V>,
        V: IntoQueryValue,
    {
        Filter::field(
            self.name,
            FilterOp::In(
                values
                    .into_iter()
                    .map(IntoQueryValue::into_query_value)
                    .collect(),
            ),
        )
    }

    pub fn gt<V>(&self, value: V) -> Filter
    where
        V: IntoQueryValue,
    {
        Filter::field(self.name, FilterOp::Gt(value.into_query_value()))
    }

    pub fn gte<V>(&self, value: V) -> Filter
    where
        V: IntoQueryValue,
    {
        Filter::field(self.name, FilterOp::Gte(value.into_query_value()))
    }

    pub fn lt<V>(&self, value: V) -> Filter
    where
        V: IntoQueryValue,
    {
        Filter::field(self.name, FilterOp::Lt(value.into_query_value()))
    }

    pub fn lte<V>(&self, value: V) -> Filter
    where
        V: IntoQueryValue,
    {
        Filter::field(self.name, FilterOp::Lte(value.into_query_value()))
    }

    pub fn contains<V>(&self, value: V) -> Filter
    where
        V: IntoQueryValue,
    {
        Filter::field(self.name, FilterOp::Contains(value.into_query_value()))
    }

    pub fn starts_with<V>(&self, value: V) -> Filter
    where
        V: IntoQueryValue,
    {
        Filter::field(self.name, FilterOp::StartsWith(value.into_query_value()))
    }

    pub fn ends_with<V>(&self, value: V) -> Filter
    where
        V: IntoQueryValue,
    {
        Filter::field(self.name, FilterOp::EndsWith(value.into_query_value()))
    }

    pub fn text_search<V>(&self, value: V) -> Filter
    where
        V: IntoQueryValue,
    {
        Filter::field(self.name, FilterOp::TextSearch(value.into_query_value()))
    }

    pub fn is_null(&self) -> Filter {
        Filter::field(self.name, FilterOp::IsNull)
    }

    pub fn is_not_null(&self) -> Filter {
        Filter::field(self.name, FilterOp::IsNotNull)
    }

    pub fn asc(&self) -> Sort {
        Sort {
            field: self.name,
            direction: SortDirection::Asc,
        }
    }

    pub fn desc(&self) -> Sort {
        Sort {
            field: self.name,
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
        Ok(Filter::field(
            self.name,
            FilterOp::Eq(self.encode_value(value)?),
        ))
    }

    pub fn not_eq(&self, value: T) -> Result<Filter, DatabaseError> {
        Ok(Filter::field(
            self.name,
            FilterOp::NotEq(self.encode_value(value)?),
        ))
    }

    pub fn one_of<I>(&self, values: I) -> Result<Filter, DatabaseError>
    where
        I: IntoIterator<Item = T>,
    {
        let mut encoded = Vec::new();
        for value in values {
            encoded.push(self.encode_value(value)?);
        }

        Ok(Filter::field(self.name, FilterOp::In(encoded)))
    }

    pub fn gt(&self, value: T) -> Result<Filter, DatabaseError> {
        Ok(Filter::field(
            self.name,
            FilterOp::Gt(self.encode_value(value)?),
        ))
    }

    pub fn gte(&self, value: T) -> Result<Filter, DatabaseError> {
        Ok(Filter::field(
            self.name,
            FilterOp::Gte(self.encode_value(value)?),
        ))
    }

    pub fn lt(&self, value: T) -> Result<Filter, DatabaseError> {
        Ok(Filter::field(
            self.name,
            FilterOp::Lt(self.encode_value(value)?),
        ))
    }

    pub fn lte(&self, value: T) -> Result<Filter, DatabaseError> {
        Ok(Filter::field(
            self.name,
            FilterOp::Lte(self.encode_value(value)?),
        ))
    }

    pub fn contains(&self, value: T) -> Result<Filter, DatabaseError> {
        Ok(Filter::field(
            self.name,
            FilterOp::Contains(self.encode_value(value)?),
        ))
    }

    pub fn starts_with(&self, value: T) -> Result<Filter, DatabaseError> {
        Ok(Filter::field(
            self.name,
            FilterOp::StartsWith(self.encode_value(value)?),
        ))
    }

    pub fn ends_with(&self, value: T) -> Result<Filter, DatabaseError> {
        Ok(Filter::field(
            self.name,
            FilterOp::EndsWith(self.encode_value(value)?),
        ))
    }

    pub fn text_search(&self, value: T) -> Result<Filter, DatabaseError> {
        Ok(Filter::field(
            self.name,
            FilterOp::TextSearch(self.encode_value(value)?),
        ))
    }

    pub fn is_null(&self) -> Filter {
        Filter::field(self.name, FilterOp::IsNull)
    }

    pub fn is_not_null(&self) -> Filter {
        Filter::field(self.name, FilterOp::IsNotNull)
    }

    pub fn asc(&self) -> Sort {
        Sort {
            field: self.name,
            direction: SortDirection::Asc,
        }
    }

    pub fn desc(&self) -> Sort {
        Sort {
            field: self.name,
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

// ── Typed relationship descriptors ────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ThroughRel {
    pub collection: &'static str,
    pub local_field: &'static str,
    pub remote_field: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryInclude {
    pub name: &'static str,
    pub kind: RelationshipKind,
}

/// A compile-time descriptor for a typed relationship between two models.
///
/// `Rel` captures the relationship name, cardinality, local/remote join keys,
/// and optional through-collection metadata so that higher-level query/include
/// APIs can stay typed and self-documenting.
///
/// # Creating a descriptor
///
/// ```rust,ignore
/// // Many-to-one: Post.author_id → User.id
/// pub const POST_AUTHOR: Rel<Post, User> = Rel::many_to_one("author", "author_id");
///
/// // One-to-many: User.id ← Post.user_id
/// pub const USER_POSTS: Rel<User, Post> = Rel::one_to_many("posts", "userId");
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Rel<L, R> {
    /// Logical relationship name.
    pub name: &'static str,
    /// Cardinality/kind of the relationship.
    pub kind: RelationshipKind,
    /// The attribute id on `L` that participates in the join.
    /// For many-to-one this is the FK column; for one-to-many this is `"id"`.
    pub local_field: &'static str,
    /// The attribute id on `R` that participates in the join.
    /// For many-to-one this is `"id"`; for one-to-many this is the FK column.
    pub remote_field: &'static str,
    /// Optional join metadata for many-to-many relationships.
    pub through: Option<ThroughRel>,
    _phantom: std::marker::PhantomData<fn() -> (L, R)>,
}

impl<L, R> Rel<L, R> {
    const fn new(
        name: &'static str,
        kind: RelationshipKind,
        local_field: &'static str,
        remote_field: &'static str,
        through: Option<ThroughRel>,
    ) -> Self {
        Self {
            name,
            kind,
            local_field,
            remote_field,
            through,
            _phantom: std::marker::PhantomData,
        }
    }

    pub const fn include(self) -> QueryInclude {
        QueryInclude {
            name: self.name,
            kind: self.kind,
        }
    }

    pub const fn is_to_one(self) -> bool {
        matches!(
            self.kind,
            RelationshipKind::ManyToOne | RelationshipKind::OneToOne
        )
    }

    pub const fn is_to_many(self) -> bool {
        matches!(
            self.kind,
            RelationshipKind::OneToMany | RelationshipKind::ManyToMany
        )
    }

    /// Many-to-one: `local_fk` on `L` is a FK pointing to `R`'s primary key.
    pub const fn many_to_one(name: &'static str, local_fk: &'static str) -> Self {
        Self::new(
            name,
            RelationshipKind::ManyToOne,
            local_fk,
            crate::system_fields::FIELD_ID,
            None,
        )
    }

    /// One-to-many: `remote_fk` on `R` is a FK pointing back to `L`'s primary key.
    pub const fn one_to_many(name: &'static str, remote_fk: &'static str) -> Self {
        Self::new(
            name,
            RelationshipKind::OneToMany,
            crate::system_fields::FIELD_ID,
            remote_fk,
            None,
        )
    }

    /// One-to-one using explicit local and remote join keys.
    pub const fn one_to_one(
        name: &'static str,
        local_field: &'static str,
        remote_field: &'static str,
    ) -> Self {
        Self::new(
            name,
            RelationshipKind::OneToOne,
            local_field,
            remote_field,
            None,
        )
    }

    /// Many-to-many using a through collection/join table.
    pub const fn many_to_many(
        name: &'static str,
        through_collection: &'static str,
        through_local_field: &'static str,
        through_remote_field: &'static str,
    ) -> Self {
        Self::new(
            name,
            RelationshipKind::ManyToMany,
            crate::system_fields::FIELD_ID,
            crate::system_fields::FIELD_ID,
            Some(ThroughRel {
                collection: through_collection,
                local_field: through_local_field,
                remote_field: through_remote_field,
            }),
        )
    }

    /// Backwards-compatible alias for [`Rel::many_to_one`].
    pub const fn parent(local_fk: &'static str) -> Self {
        Self::many_to_one(local_fk, local_fk)
    }

    /// Backwards-compatible alias for [`Rel::one_to_many`].
    pub const fn children(remote_fk: &'static str) -> Self {
        Self::one_to_many(remote_fk, remote_fk)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PopulateOne<M, RM>
where
    M: Model,
    RM: Model,
{
    pub rel: Rel<M, RM>,
    pub extract_local_key: fn(&M::Entity) -> Option<String>,
    pub extract_remote_key: fn(&RM::Entity) -> Option<String>,
    pub set: fn(&mut M::Entity, RelationOne<RM::Entity>),
}

impl<M, RM> PopulateOne<M, RM>
where
    M: Model,
    RM: Model,
{
    pub const fn new(
        rel: Rel<M, RM>,
        extract_local_key: fn(&M::Entity) -> Option<String>,
        extract_remote_key: fn(&RM::Entity) -> Option<String>,
        set: fn(&mut M::Entity, RelationOne<RM::Entity>),
    ) -> Self {
        Self {
            rel,
            extract_local_key,
            extract_remote_key,
            set,
        }
    }

    pub const fn include(&self) -> QueryInclude {
        self.rel.include()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PopulateMany<M, RM>
where
    M: Model,
    RM: Model,
{
    pub rel: Rel<M, RM>,
    pub extract_local_key: fn(&M::Entity) -> String,
    pub extract_remote_key: fn(&RM::Entity) -> Option<String>,
    pub set: fn(&mut M::Entity, RelationMany<RM::Entity>),
}

impl<M, RM> PopulateMany<M, RM>
where
    M: Model,
    RM: Model,
{
    pub const fn new(
        rel: Rel<M, RM>,
        extract_local_key: fn(&M::Entity) -> String,
        extract_remote_key: fn(&RM::Entity) -> Option<String>,
        set: fn(&mut M::Entity, RelationMany<RM::Entity>),
    ) -> Self {
        Self {
            rel,
            extract_local_key,
            extract_remote_key,
            set,
        }
    }

    pub const fn include(&self) -> QueryInclude {
        self.rel.include()
    }
}

impl<L, R> From<Rel<L, R>> for QueryInclude {
    fn from(value: Rel<L, R>) -> Self {
        value.include()
    }
}

#[cfg(test)]
mod tests {
    use super::{QuerySpec, Rel};
    use crate::RelationshipKind;

    #[derive(Debug, Clone, Copy)]
    struct User;

    #[derive(Debug, Clone, Copy)]
    struct Post;

    #[test]
    fn query_spec_tracks_includes() {
        let rel = Rel::<Post, User>::many_to_one("author", "authorId");
        let query = QuerySpec::new().include(rel);

        assert_eq!(query.includes().len(), 1);
        assert_eq!(query.includes()[0].name, "author");
        assert_eq!(query.includes()[0].kind, RelationshipKind::ManyToOne);
    }

    #[test]
    fn rel_constructors_capture_cardinality() {
        let author = Rel::<Post, User>::many_to_one("author", "authorId");
        assert!(author.is_to_one());
        assert_eq!(author.remote_field, crate::FIELD_ID);

        let posts = Rel::<User, Post>::one_to_many("posts", "userId");
        assert!(posts.is_to_many());
        assert_eq!(posts.local_field, crate::FIELD_ID);

        let memberships =
            Rel::<User, Post>::many_to_many("roles", "user_roles", "userId", "roleId");
        assert!(memberships.is_to_many());
        assert_eq!(memberships.kind, RelationshipKind::ManyToMany);
        assert_eq!(
            memberships.through.expect("through").collection,
            "user_roles"
        );
    }
}
