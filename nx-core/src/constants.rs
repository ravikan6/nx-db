use crate::collection::{Attribute, Collection, Id};
use crate::enums::AttributeKind;
use std::collections::BTreeMap;
use std::sync::LazyLock;

pub const KEY_LENGTH: u64 = 255;
pub const METADATA_COLLECTION_ID: &str = "_metadata";

pub static DEFAULT_ATTRIBUTES: LazyLock<BTreeMap<Id, Attribute>> = LazyLock::new(|| {
    BTreeMap::from([
        (
            Id::from_static("$id"),
            Attribute::new(AttributeKind::String).max_len(KEY_LENGTH).required(),
        ),
        (
            Id::from_static("$sequence"),
            Attribute::new(AttributeKind::Integer).max_len(8),
        ),
        (
            Id::from_static("$collection"),
            Attribute::new(AttributeKind::String).max_len(KEY_LENGTH).required(),
        ),
        (
            Id::from_static("$schema"),
            Attribute::new(AttributeKind::String).max_len(KEY_LENGTH),
        ),
        (
            Id::from_static("$tenant"),
            Attribute::new(AttributeKind::Integer).max_len(8),
        ),
        (
            Id::from_static("$createdAt"),
            Attribute::new(AttributeKind::Timestamp),
        ),
        (
            Id::from_static("$updatedAt"),
            Attribute::new(AttributeKind::Timestamp),
        ),
        (
            Id::from_static("$permissions"),
            Attribute::new(AttributeKind::String).max_len(KEY_LENGTH).array(),
        ),
    ])
});

pub static INTERNAL_ATTRIBUTES_KEYS: LazyLock<Vec<&str>, fn() -> Vec<&'static str>> =
    LazyLock::new(|| Vec::from(["_uid", "_createdAt", "_updatedAt", "_permissions"]));

pub static METADATA_COLLECTION: LazyLock<Collection, fn() -> Collection> = LazyLock::new(|| {
    Collection::new(
        Id::from_static(METADATA_COLLECTION_ID),
        "Metadata",
        Id::from_static(METADATA_COLLECTION_ID),
    )
        .set_attributes(BTreeMap::from([
            (
                Id::from_static("name"),
                Attribute::new(AttributeKind::String).max_len(KEY_LENGTH).required(),
            ),
            (
                Id::from_static("attributes"),
                Attribute::new(AttributeKind::Json),
            ),
            (
                Id::from_static("indexes"),
                Attribute::new(AttributeKind::Json),
            ),
            (
                Id::from_static("document_security"),
                Attribute::new(AttributeKind::Boolean).required(),
            ),
            (
                Id::from_static("enabled"),
                Attribute::new(AttributeKind::Boolean).required(),
            ),
            (
                Id::from_static("version"),
                Attribute::new(AttributeKind::Integer).max_len(8),
            ),
        ]))
});
