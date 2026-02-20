use crate::auth;
use time::OffsetDateTime;

#[derive(Debug, Clone)]
pub struct InternalId(u128);

#[derive(Debug, Clone)]
pub struct Id(String);

#[derive(Debug, Clone)]
pub struct Collection {
    id: Id,
    permissions: Vec<auth::Permission>,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
}

impl Collection {
    pub fn new(id: Id, permissions: Vec<auth::Permission>) -> Self {
        let now = OffsetDateTime::now_utc();

        Self {
            id,
            permissions,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn touch(&mut self) {
        self.updated_at = OffsetDateTime::now_utc();
    }
}
