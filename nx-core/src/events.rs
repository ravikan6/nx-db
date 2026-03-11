#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    CollectionCreated {
        collection: &'static str,
    },
    DocumentCreated {
        collection: &'static str,
        id: String,
    },
    DocumentUpdated {
        collection: &'static str,
        id: String,
    },
    DocumentDeleted {
        collection: &'static str,
        id: String,
    },
}

impl Event {
    pub fn collection_created(collection: &'static str) -> Self {
        Self::CollectionCreated { collection }
    }

    pub fn document_created(collection: &'static str, id: impl Into<String>) -> Self {
        Self::DocumentCreated {
            collection,
            id: id.into(),
        }
    }

    pub fn document_updated(collection: &'static str, id: impl Into<String>) -> Self {
        Self::DocumentUpdated {
            collection,
            id: id.into(),
        }
    }

    pub fn document_deleted(collection: &'static str, id: impl Into<String>) -> Self {
        Self::DocumentDeleted {
            collection,
            id: id.into(),
        }
    }
}

pub trait EventBus: Send + Sync {
    fn dispatch(&self, event: Event);
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopEventBus;

impl EventBus for NoopEventBus {
    fn dispatch(&self, _event: Event) {}
}
