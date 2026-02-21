use crate::collection::{Attribute, Id};
use crate::enums::AttributeKind;
use std::collections::BTreeMap;
use std::sync::LazyLock;

pub const KEY_LENGTH: u64 = 255;
pub static DEFAULT_ATTRIBUTES: LazyLock<BTreeMap<Id, Attribute>> = LazyLock::new(|| {
    BTreeMap::from([
        (
            Id::new("$id"),
            Attribute::system(AttributeKind::String, Some(KEY_LENGTH), true, false),
        ),
        (
            Id::new("$sequence"),
            Attribute::system(AttributeKind::Integer, Some(8), true, false),
        ),
        (
            Id::new("$collection"),
            Attribute::system(AttributeKind::String, Some(KEY_LENGTH), true, false),
        ),
        (
            Id::new("$schema"),
            Attribute::system(AttributeKind::String, Some(KEY_LENGTH), false, false),
        ),
        (
            Id::new("$tenant"),
            Attribute::system(AttributeKind::Integer, Some(8), false, false),
        ),
        (
            Id::new("$createdAt"),
            Attribute::system(AttributeKind::Timestamp, None, false, false),
        ),
        (
            Id::new("$updatedAt"),
            Attribute::system(AttributeKind::Timestamp, None, false, false),
        ),
        (
            Id::new("$permissions"),
            Attribute::system(AttributeKind::String, Some(KEY_LENGTH), false, true),
        ),
    ])
});
