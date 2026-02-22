use crate::collection::Collection;
use crate::constants;
use crate::errors::DatabaseError;

pub struct Database {}

impl Database {
    pub fn get_collection(&self, id: &str) -> Result<&Collection, DatabaseError> {
        if id == constants::METADATA_COLLECTION_ID {
            Ok(&*constants::METADATA_COLLECTION)
        } else {
            Err(DatabaseError::Other("error".into()))
        }
    }

    pub fn get_document(&self, collection: &str, id: &str) {
        let coll = self.get_collection(collection).unwrap();
    }
}