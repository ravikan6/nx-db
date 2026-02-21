use crate::enums::{AttributeKind, IndexKind, OnDeleteAction, Order, RelationshipKind, RelationshipSide, Value};
use crate::utils;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use time::OffsetDateTime;

#[derive(Debug, Clone)]
pub struct InternalId(u128);

#[derive(Debug, Clone, Serialize, Deserialize, Ord, PartialOrd, PartialEq, Eq)]
pub struct Id(pub String);

impl Id {
    pub fn new(value: &str) -> Id {
        Id(value.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct Collection {
    id: Id,
    permissions: Vec<utils::Permission>,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
    collection: Id,
    name: String,
    document_security: bool,
    enabled: bool,
    version: u64,
    attributes: BTreeMap<Id, Attribute>,
    indexes: BTreeMap<Id, Index>,
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
    // pub fn new(id: Id, permissions: Vec<utils::Permission>) -> Self {
    //     let now = OffsetDateTime::now_utc();
    //
    //     Self {
    //         id,
    //         permissions,
    //         created_at: now,
    //         updated_at: now,
    //     }
    // }

    pub fn touch(&mut self) {
        self.updated_at = OffsetDateTime::now_utc();
    }
}


impl Attribute {
    pub fn system(kind: AttributeKind, size: Option<u64>, required: bool, array: bool) -> Self {
        Self {
            kind,
            size,
            required,
            array,
            filters: None,
            format: None,
            format_options: None,
            default: None,
            options: AttributeOptions::None,
        }
    }
}