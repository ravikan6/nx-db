use crate::enums::{
    AttributeKind, IndexKind, OnDeleteAction, Order, RelationshipKind, RelationshipSide, Value,
};
use crate::utils;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use time::OffsetDateTime;
use std::borrow::Cow;

#[derive(Debug, Clone)]
pub struct InternalId(u128);

#[derive(Debug, Clone, Serialize, Deserialize, Ord, PartialOrd, PartialEq, Eq)]
pub struct Id(Cow<'static, str>);

impl Id {
    pub fn from_static(s: &'static str) -> Self {
        Id(Cow::Borrowed(s))
    }

    pub fn new<S: Into<String>>(s: S) -> Self {
        Id(Cow::Owned(s.into()))
    }
}

#[derive(Debug, Clone)]
pub struct Collection {
    pub id: Id,
    pub permissions: Vec<utils::Permission>,
    pub created_at: Option<OffsetDateTime>,
    pub updated_at: Option<OffsetDateTime>,
    pub collection: Id,
    pub name: String,
    pub document_security: bool,
    pub enabled: bool,
    pub version: u64,
    pub attributes: BTreeMap<Id, Attribute>,
    pub indexes: BTreeMap<Id, Index>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attribute {
    pub kind: AttributeKind,
    pub size: Option<u64>,
    pub required: bool,
    pub array: bool,
    pub filters: Option<Vec<String>>,
    pub format: Option<String>,
    pub format_options: Option<BTreeMap<String, Value>>,
    pub default: Option<Value>,
    pub options: AttributeOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AttributeOptions {
    None,
    Relationship {
        kind: RelationshipKind,
        side: RelationshipSide,
        collection: Id,
        two_way: bool,
        two_way_key: Option<String>,
        on_delete: OnDeleteAction,
    },
}

#[derive(Debug, Clone)]
pub struct Index {
    kind: IndexKind,
    attributes: Option<Vec<Id>>,
    orders: Option<Vec<Order>>,
}

impl Collection {
    pub fn new(id: Id, name: impl Into<String>, collection_id: Id) -> Self {
        let now = OffsetDateTime::now_utc();

        Self {
            id,
            permissions: Vec::new(),
            created_at: Some(now),
            updated_at: Some(now),
            name: name.into(),
            collection: collection_id,
            document_security: false,
            enabled: true,
            version: 1,
            attributes: BTreeMap::new(),
            indexes: BTreeMap::new(),
        }
    }

    pub fn set_attributes(mut self, attributes: BTreeMap<Id, Attribute>) -> Self {
        self.attributes = attributes;
        self
    }

    pub fn set_indexes(mut self, indexes: BTreeMap<Id, Index>) -> Self {
        self.indexes = indexes;
        self
    }
}

impl Attribute {
    pub fn new(kind: AttributeKind) -> Self {
        Self {
            kind,
            size: None,
            required: false,
            array: false,
            filters: None,
            format: None,
            format_options: None,
            default: None,
            options: AttributeOptions::None,
        }
    }

    pub fn max_len(mut self, len: u64) -> Self {
        self.size = Some(len);
        self
    }

    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    pub fn array(mut self) -> Self {
        self.array = true;
        self
    }
}
