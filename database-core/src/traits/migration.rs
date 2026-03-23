use crate::enums::{AttributeKind, IndexKind, Order};
use crate::schema::{AttributePersistence, DefaultValue};

pub trait MigrationCollection {
    fn id(&self) -> &str;
    fn attributes(&self) -> Vec<MigrationAttribute>;
    fn indexes(&self) -> Vec<MigrationIndex>;
}

pub struct MigrationAttribute {
    pub id: String,
    pub column: String,
    pub kind: AttributeKind,
    pub required: bool,
    pub array: bool,
    pub length: Option<usize>,
    pub default: Option<DefaultValue>,
    pub persistence: AttributePersistence,
}

pub struct MigrationIndex {
    pub id: String,
    pub kind: IndexKind,
    pub attributes: Vec<String>,
    pub orders: Vec<Order>,
}
