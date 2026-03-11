pub const FIELD_ID: &str = "id";
pub const FIELD_SEQUENCE: &str = "$sequence";
pub const FIELD_CREATED_AT: &str = "$createdAt";
pub const FIELD_UPDATED_AT: &str = "$updatedAt";
pub const FIELD_PERMISSIONS: &str = "$permissions";
pub const FIELD_TENANT: &str = "$tenant";

pub const COLUMN_SEQUENCE: &str = "_id";
pub const COLUMN_ID: &str = "_uid";
pub const COLUMN_CREATED_AT: &str = "_createdAt";
pub const COLUMN_UPDATED_AT: &str = "_updatedAt";
pub const COLUMN_PERMISSIONS: &str = "_permissions";
pub const COLUMN_TENANT: &str = "_tenant";

pub fn is_system_field(field: &str) -> bool {
    matches!(
        field,
        FIELD_ID
            | FIELD_SEQUENCE
            | FIELD_CREATED_AT
            | FIELD_UPDATED_AT
            | FIELD_PERMISSIONS
            | FIELD_TENANT
    )
}
