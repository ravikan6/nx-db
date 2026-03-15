use database_core::errors::DatabaseError;
use database_core::{
    AttributeKind, CollectionSchema, Context, 
    COLUMN_CREATED_AT, COLUMN_ID, COLUMN_PERMISSIONS, COLUMN_SEQUENCE, COLUMN_UPDATED_AT,
    FIELD_CREATED_AT, FIELD_ID, FIELD_PERMISSIONS, FIELD_SEQUENCE, FIELD_UPDATED_AT,
};
use sqlx::{Row, postgres::PgRow, types::time::OffsetDateTime};
use database_core::traits::storage::{StorageRecord, StorageValue};

pub struct PostgresUtils;

impl PostgresUtils {
    pub fn is_valid_identifier(identifier: &str) -> bool {
        let mut chars = identifier.chars();
        let Some(first) = chars.next() else { return false; };
        if !(first == '_' || first.is_ascii_alphabetic()) { return false; }
        chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
    }

    pub fn quote_identifier(identifier: &str) -> Result<String, DatabaseError> {
        if !Self::is_valid_identifier(identifier) {
            return Err(DatabaseError::Other(format!("invalid postgres identifier: {identifier}")));
        }
        Ok(format!("\"{identifier}\""))
    }

    pub fn qualified_table_name(context: &Context, collection: &str) -> Result<String, DatabaseError> {
        let schema = Self::quote_identifier(context.schema())?;
        let collection = Self::quote_identifier(collection)?;
        Ok(format!("{schema}.{collection}"))
    }

    pub fn qualified_permissions_table_name(context: &Context, collection: &str) -> Result<String, DatabaseError> {
        Self::qualified_table_name(context, &format!("{collection}_perms"))
    }

    pub fn sql_type(kind: AttributeKind, array: bool, length: Option<usize>) -> String {
        let base = match kind {
            AttributeKind::String | AttributeKind::Relationship | AttributeKind::Virtual => {
                if let Some(len) = length { format!("VARCHAR({len})") } else { "TEXT".to_string() }
            }
            AttributeKind::Integer => "BIGINT".to_string(),
            AttributeKind::Float => "DOUBLE PRECISION".to_string(),
            AttributeKind::Boolean => "BOOLEAN".to_string(),
            AttributeKind::Timestamp => "TIMESTAMPTZ".to_string(),
            AttributeKind::Json => "JSONB".to_string(),
        };
        if array { format!("{base}[]") } else { base }
    }

    pub fn column_for_field(schema: &'static CollectionSchema, field: &str) -> Result<String, DatabaseError> {
        let column = match field {
            FIELD_ID => COLUMN_ID,
            FIELD_SEQUENCE => COLUMN_SEQUENCE,
            FIELD_CREATED_AT => COLUMN_CREATED_AT,
            FIELD_UPDATED_AT => COLUMN_UPDATED_AT,
            FIELD_PERMISSIONS => COLUMN_PERMISSIONS,
            other => schema.attribute(other).map(|a| a.column).ok_or_else(|| {
                DatabaseError::Other(format!("collection '{}': unknown query field '{}'", schema.id, other))
            })?,
        };
        Self::quote_identifier(column)
    }

    pub fn select_columns(schema: &'static CollectionSchema) -> Result<String, DatabaseError> {
        let mut columns = vec![
            Self::quote_identifier(COLUMN_SEQUENCE)?,
            Self::quote_identifier(COLUMN_ID)?,
            Self::quote_identifier(COLUMN_CREATED_AT)?,
            Self::quote_identifier(COLUMN_UPDATED_AT)?,
            Self::quote_identifier(COLUMN_PERMISSIONS)?,
        ];
        for attr in schema.persisted_attributes() {
            columns.push(Self::quote_identifier(attr.column)?);
        }
        Ok(columns.join(", "))
    }

    pub fn extract_string(values: &StorageRecord, key: &str) -> Result<String, DatabaseError> {
        match values.get(key) {
            Some(StorageValue::String(value)) => Ok(value.clone()),
            _ => Err(DatabaseError::Other(format!("record field '{key}' must be a string"))),
        }
    }

    pub fn extract_optional_string_array(values: &StorageRecord, key: &str) -> Result<Vec<String>, DatabaseError> {
        match values.get(key) {
            Some(StorageValue::StringArray(value)) => Ok(value.clone()),
            Some(StorageValue::Null) | None => Ok(Vec::new()),
            _ => Err(DatabaseError::Other(format!("record field '{key}' must be a string array"))),
        }
    }

    pub fn extract_optional_timestamp(values: &StorageRecord, key: &str) -> Result<Option<OffsetDateTime>, DatabaseError> {
        match values.get(key) {
            Some(StorageValue::Timestamp(value)) => Ok(Some(*value)),
            Some(StorageValue::Null) | None => Ok(None),
            _ => Err(DatabaseError::Other(format!("record field '{key}' must be a timestamp"))),
        }
    }

    pub fn row_to_record_internal(row: &PgRow, schema: &'static CollectionSchema) -> Result<StorageRecord, DatabaseError> {
        let mut record = StorageRecord::new();

        let sequence: i64 = row.try_get(COLUMN_SEQUENCE).map_err(|e| DatabaseError::Other(format!("postgres sequence read failed: {e}")))?;
        let uid: String = row.try_get(COLUMN_ID).map_err(|e| DatabaseError::Other(format!("postgres id read failed: {e}")))?;
        let created_at: Option<OffsetDateTime> = row.try_get(COLUMN_CREATED_AT).map_err(|e| DatabaseError::Other(format!("postgres createdAt read failed: {e}")))?;
        let updated_at: Option<OffsetDateTime> = row.try_get(COLUMN_UPDATED_AT).map_err(|e| DatabaseError::Other(format!("postgres updatedAt read failed: {e}")))?;
        let permissions: Vec<String> = row.try_get(COLUMN_PERMISSIONS).map_err(|e| DatabaseError::Other(format!("postgres permissions read failed: {e}")))?;

        record.insert(FIELD_SEQUENCE.to_string(), StorageValue::Int(sequence));
        record.insert(FIELD_ID.to_string(), StorageValue::String(uid));
        record.insert(FIELD_CREATED_AT.to_string(), created_at.map(StorageValue::Timestamp).unwrap_or(StorageValue::Null));
        record.insert(FIELD_UPDATED_AT.to_string(), updated_at.map(StorageValue::Timestamp).unwrap_or(StorageValue::Null));
        record.insert(FIELD_PERMISSIONS.to_string(), StorageValue::StringArray(permissions));

        for attr in schema.persisted_attributes() {
            let key = attr.column;
            let value = if attr.array {
                match attr.kind {
                    AttributeKind::String | AttributeKind::Relationship | AttributeKind::Virtual => {
                        row.try_get::<Option<Vec<String>>, _>(key).map(|v| v.map(StorageValue::StringArray).unwrap_or(StorageValue::Null))
                    }
                    AttributeKind::Integer => row.try_get::<Option<Vec<i64>>, _>(key).map(|v| v.map(StorageValue::IntArray).unwrap_or(StorageValue::Null)),
                    AttributeKind::Float => row.try_get::<Option<Vec<f64>>, _>(key).map(|v| v.map(StorageValue::FloatArray).unwrap_or(StorageValue::Null)),
                    AttributeKind::Boolean => row.try_get::<Option<Vec<bool>>, _>(key).map(|v| v.map(StorageValue::BoolArray).unwrap_or(StorageValue::Null)),
                    AttributeKind::Timestamp => row.try_get::<Option<Vec<OffsetDateTime>>, _>(key).map(|v: Option<Vec<OffsetDateTime>>| v.map(StorageValue::TimestampArray).unwrap_or(StorageValue::Null)),
                    AttributeKind::Json => row.try_get::<Option<Vec<sqlx::types::Json<serde_json::Value>>>, _>(key).map(|v| {
                        v.map(|vals| StorageValue::StringArray(vals.into_iter().map(|v| v.0.to_string()).collect())).unwrap_or(StorageValue::Null)
                    }),
                }
            } else {
                match attr.kind {
                    AttributeKind::String | AttributeKind::Relationship | AttributeKind::Virtual => {
                        row.try_get::<Option<String>, _>(key).map(|v| v.map(StorageValue::String).unwrap_or(StorageValue::Null))
                    }
                    AttributeKind::Integer => row.try_get::<Option<i64>, _>(key).map(|v| v.map(StorageValue::Int).unwrap_or(StorageValue::Null)),
                    AttributeKind::Float => row.try_get::<Option<f64>, _>(key).map(|v| v.map(StorageValue::Float).unwrap_or(StorageValue::Null)),
                    AttributeKind::Boolean => row.try_get::<Option<bool>, _>(key).map(|v| v.map(StorageValue::Bool).unwrap_or(StorageValue::Null)),
                    AttributeKind::Timestamp => row.try_get::<Option<OffsetDateTime>, _>(key).map(|v: Option<OffsetDateTime>| v.map(StorageValue::Timestamp).unwrap_or(StorageValue::Null)),
                    AttributeKind::Json => row.try_get::<Option<sqlx::types::Json<serde_json::Value>>, _>(key).map(|v| {
                        v.map(|j| StorageValue::Json(j.0.to_string())).unwrap_or(StorageValue::Null)
                    }),
                }
            };

            match value {
                Ok(v) => { record.insert(attr.id.to_string(), v); }
                Err(sqlx::Error::ColumnNotFound(_)) => {}
                Err(e) => return Err(DatabaseError::Other(format!("postgres read error {}: {}", attr.id, e))),
            }
        }
        Ok(record)
    }
}
