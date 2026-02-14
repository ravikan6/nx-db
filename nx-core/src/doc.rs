pub use crate::traits::document::{Document};

#[derive(Clone, Default)]
pub struct Doc {
    metadata: Metadata
}

#[derive(Debug, Clone, Default)]
pub struct Metadata {
    pub id: Option<String>,
    pub collection: Option<String>,
}

impl Doc {
    
    pub fn getId(&self) -> &Option<String>{
        &self.metadata.id
    }
}