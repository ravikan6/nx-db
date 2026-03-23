use database_core::errors::DatabaseError;
use database_core::traits::storage::{StorageRecord, StorageValue};
use database_core::{
    AttributeKind, COLUMN_CREATED_AT, COLUMN_ID, COLUMN_PERMISSIONS, COLUMN_SEQUENCE,
    COLUMN_UPDATED_AT, CollectionSchema, Context, FIELD_CREATED_AT, FIELD_ID, FIELD_PERMISSIONS,
    FIELD_SEQUENCE, FIELD_UPDATED_AT,
};
use sqlx::Row;
use time::OffsetDateTime;

pub struct SqliteUtils;

impl SqliteUtils {
    pub fn quote_identifier(identifier: &str) -> String {
        format!("\"{}\"", identifier.replace('"', "\"\""))
    }

    pub fn qualified_table_name(_context: &Context, collection: &str) -> String {
        Self::quote_identifier(collection)
    }

    pub fn qualified_permissions_table_name(context: &Context, collection: &str) -> String {
        Self::qualified_table_name(context, &format!("{collection}_perms"))
    }

    /// Render a `DEFAULT <expr>` fragment for a column DDL.
    pub fn sql_default(default: Option<database_core::schema::DefaultValue>) -> String {
        use database_core::schema::DefaultValue;
        match default {
            None => String::new(),
            Some(DefaultValue::Null) => " DEFAULT NULL".to_string(),
            Some(DefaultValue::Bool(true)) => " DEFAULT 1".to_string(),
            Some(DefaultValue::Bool(false)) => " DEFAULT 0".to_string(),
            Some(DefaultValue::Int(i)) => format!(" DEFAULT {i}"),
            Some(DefaultValue::Float(bits)) => {
                let f = f64::from_bits(bits);
                format!(" DEFAULT {f}")
            }
            Some(DefaultValue::Str(s)) => {
                let escaped = s.replace('\'', "''");
                format!(" DEFAULT '{escaped}'")
            }
            Some(DefaultValue::Now) => {
                " DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))".to_string()
            }
        }
    }

    pub fn sql_type(kind: AttributeKind, array: bool) -> &'static str {
        if array {
            return "TEXT";
        }
        match kind {
            AttributeKind::String
            | AttributeKind::Relationship
            | AttributeKind::Virtual
            | AttributeKind::Json => "TEXT",
            AttributeKind::Integer | AttributeKind::Boolean => "INTEGER",
            AttributeKind::Float => "REAL",
            AttributeKind::Timestamp => "TEXT",
        }
    }

    pub fn row_to_record(
        row: &sqlx::sqlite::SqliteRow,
        schema: &'static CollectionSchema,
    ) -> Result<StorageRecord, DatabaseError> {
        let mut record = StorageRecord::new();
        let sequence: i64 = row
            .try_get(COLUMN_SEQUENCE)
            .map_err(|e| DatabaseError::Other(e.to_string()))?;
        let uid: String = row
            .try_get(COLUMN_ID)
            .map_err(|e| DatabaseError::Other(e.to_string()))?;
        let created_at_str: String = row
            .try_get(COLUMN_CREATED_AT)
            .map_err(|e| DatabaseError::Other(e.to_string()))?;
        let updated_at_str: String = row
            .try_get(COLUMN_UPDATED_AT)
            .map_err(|e| DatabaseError::Other(e.to_string()))?;

        let created_at = OffsetDateTime::parse(
            &created_at_str,
            &time::format_description::well_known::Rfc3339,
        )
        .map_err(|e| DatabaseError::Other(e.to_string()))?;
        let updated_at = OffsetDateTime::parse(
            &updated_at_str,
            &time::format_description::well_known::Rfc3339,
        )
        .map_err(|e| DatabaseError::Other(e.to_string()))?;

        let permissions_json: String = row
            .try_get(COLUMN_PERMISSIONS)
            .map_err(|e| DatabaseError::Other(e.to_string()))?;
        let permissions: Vec<String> = serde_json::from_str(&permissions_json)
            .map_err(|e| DatabaseError::Other(e.to_string()))?;

        record.insert(FIELD_SEQUENCE.to_string(), StorageValue::Int(sequence));
        record.insert(FIELD_ID.to_string(), StorageValue::String(uid));
        record.insert(
            FIELD_CREATED_AT.to_string(),
            StorageValue::Timestamp(created_at),
        );
        record.insert(
            FIELD_UPDATED_AT.to_string(),
            StorageValue::Timestamp(updated_at),
        );
        record.insert(
            FIELD_PERMISSIONS.to_string(),
            StorageValue::StringArray(permissions),
        );

        for attr in schema.persisted_attributes() {
            let key = attr.column;
            let value = if attr.array {
                match row.try_get::<Option<String>, _>(key) {
                    Ok(Some(json)) => {
                        let val: serde_json::Value = serde_json::from_str(&json)
                            .map_err(|e| DatabaseError::Other(e.to_string()))?;
                        Self::json_to_storage_value(val, attr.kind, true)?
                    }
                    Ok(None) => StorageValue::Null,
                    Err(sqlx::Error::ColumnNotFound(_)) => continue,
                    Err(e) => return Err(DatabaseError::Other(e.to_string())),
                }
            } else {
                match attr.kind {
                    AttributeKind::String
                    | AttributeKind::Relationship
                    | AttributeKind::Virtual
                    | AttributeKind::Json => row
                        .try_get::<Option<String>, _>(key)
                        .map(|v| v.map(StorageValue::String).unwrap_or(StorageValue::Null))
                        .unwrap_or(StorageValue::Null),
                    AttributeKind::Integer | AttributeKind::Boolean => {
                        let v = row.try_get::<Option<i64>, _>(key).unwrap_or(None);
                        match v {
                            Some(val) => {
                                if attr.kind == AttributeKind::Boolean {
                                    StorageValue::Bool(val != 0)
                                } else {
                                    StorageValue::Int(val)
                                }
                            }
                            None => StorageValue::Null,
                        }
                    }
                    AttributeKind::Float => row
                        .try_get::<Option<f64>, _>(key)
                        .map(|v| v.map(StorageValue::Float).unwrap_or(StorageValue::Null))
                        .unwrap_or(StorageValue::Null),
                    AttributeKind::Timestamp => {
                        let s = row.try_get::<Option<String>, _>(key).unwrap_or(None);
                        match s {
                            Some(val) => {
                                let dt = OffsetDateTime::parse(
                                    &val,
                                    &time::format_description::well_known::Rfc3339,
                                )
                                .map_err(|e| DatabaseError::Other(e.to_string()))?;
                                StorageValue::Timestamp(dt)
                            }
                            None => StorageValue::Null,
                        }
                    }
                }
            };
            record.insert(attr.id.to_string(), value);
        }
        Ok(record)
    }

    pub fn json_to_storage_value(
        val: serde_json::Value,
        kind: AttributeKind,
        array: bool,
    ) -> Result<StorageValue, DatabaseError> {
        if array {
            let serde_json::Value::Array(elems) = val else {
                return Err(DatabaseError::Other("expected array".into()));
            };
            match kind {
                AttributeKind::String
                | AttributeKind::Relationship
                | AttributeKind::Virtual
                | AttributeKind::Json => Ok(StorageValue::StringArray(
                    elems
                        .into_iter()
                        .map(|e| e.as_str().unwrap_or_default().to_string())
                        .collect(),
                )),
                AttributeKind::Integer => Ok(StorageValue::IntArray(
                    elems
                        .into_iter()
                        .map(|e| e.as_i64().unwrap_or_default())
                        .collect(),
                )),
                AttributeKind::Boolean => Ok(StorageValue::BoolArray(
                    elems
                        .into_iter()
                        .map(|e| e.as_bool().unwrap_or_default())
                        .collect(),
                )),
                AttributeKind::Float => Ok(StorageValue::FloatArray(
                    elems
                        .into_iter()
                        .map(|e| e.as_f64().unwrap_or_default())
                        .collect(),
                )),
                AttributeKind::Timestamp => {
                    let mut dates = Vec::new();
                    for e in elems {
                        let s = e
                            .as_str()
                            .ok_or_else(|| DatabaseError::Other("expected string".into()))?;
                        dates.push(
                            OffsetDateTime::parse(
                                s,
                                &time::format_description::well_known::Rfc3339,
                            )
                            .map_err(|e| DatabaseError::Other(e.to_string()))?,
                        );
                    }
                    Ok(StorageValue::TimestampArray(dates))
                }
            }
        } else {
            match kind {
                AttributeKind::String | AttributeKind::Relationship | AttributeKind::Virtual => Ok(
                    StorageValue::String(val.as_str().unwrap_or_default().to_string()),
                ),
                AttributeKind::Json => Ok(StorageValue::Json(val.to_string())),
                AttributeKind::Integer => Ok(StorageValue::Int(val.as_i64().unwrap_or_default())),
                AttributeKind::Boolean => Ok(StorageValue::Bool(val.as_bool().unwrap_or_default())),
                AttributeKind::Float => Ok(StorageValue::Float(val.as_f64().unwrap_or_default())),
                AttributeKind::Timestamp => {
                    let s = val
                        .as_str()
                        .ok_or_else(|| DatabaseError::Other("expected string".into()))?;
                    let dt =
                        OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
                            .map_err(|e| DatabaseError::Other(e.to_string()))?;
                    Ok(StorageValue::Timestamp(dt))
                }
            }
        }
    }
}
