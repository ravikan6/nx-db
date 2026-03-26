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

    pub fn qualified_column_for_field(
        schema: &'static CollectionSchema,
        field: &str,
        alias: Option<&str>,
    ) -> Result<String, DatabaseError> {
        let column = match field {
            FIELD_ID => COLUMN_ID,
            FIELD_SEQUENCE => COLUMN_SEQUENCE,
            FIELD_CREATED_AT => COLUMN_CREATED_AT,
            FIELD_UPDATED_AT => COLUMN_UPDATED_AT,
            FIELD_PERMISSIONS => COLUMN_PERMISSIONS,
            other => schema.attribute(other).map(|a| a.column).ok_or_else(|| {
                DatabaseError::Other(format!(
                    "collection '{}': unknown query field '{}'",
                    schema.id, other
                ))
            })?,
        };
        let quoted = Self::quote_identifier(column);
        Ok(match alias {
            Some(alias) => format!("{}.{}", Self::quote_identifier(alias), quoted),
            None => quoted,
        })
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
            | AttributeKind::Json
            | AttributeKind::Enum => "TEXT",
            AttributeKind::Integer | AttributeKind::Boolean => "INTEGER",
            AttributeKind::Float => "REAL",
            AttributeKind::Timestamp => "TEXT",
        }
    }

    pub fn select_columns_for_alias(
        schema: &'static CollectionSchema,
        source_alias: &str,
        result_prefix: &str,
    ) -> String {
        let source_alias = Self::quote_identifier(source_alias);
        let mut columns = vec![
            format!(
                "{source_alias}.{} AS {}",
                Self::quote_identifier(COLUMN_SEQUENCE),
                Self::quote_identifier(&format!("{result_prefix}__{COLUMN_SEQUENCE}")),
            ),
            format!(
                "{source_alias}.{} AS {}",
                Self::quote_identifier(COLUMN_ID),
                Self::quote_identifier(&format!("{result_prefix}__{COLUMN_ID}")),
            ),
            format!(
                "{source_alias}.{} AS {}",
                Self::quote_identifier(COLUMN_CREATED_AT),
                Self::quote_identifier(&format!("{result_prefix}__{COLUMN_CREATED_AT}")),
            ),
            format!(
                "{source_alias}.{} AS {}",
                Self::quote_identifier(COLUMN_UPDATED_AT),
                Self::quote_identifier(&format!("{result_prefix}__{COLUMN_UPDATED_AT}")),
            ),
            format!(
                "{source_alias}.{} AS {}",
                Self::quote_identifier(COLUMN_PERMISSIONS),
                Self::quote_identifier(&format!("{result_prefix}__{COLUMN_PERMISSIONS}")),
            ),
        ];
        for attr in schema.persisted_attributes() {
            columns.push(format!(
                "{source_alias}.{} AS {}",
                Self::quote_identifier(attr.column),
                Self::quote_identifier(&format!("{result_prefix}__{}", attr.column)),
            ));
        }
        columns.join(", ")
    }

    pub fn select_columns(schema: &'static CollectionSchema) -> String {
        let mut columns = vec![
            Self::quote_identifier(COLUMN_SEQUENCE),
            Self::quote_identifier(COLUMN_ID),
            Self::quote_identifier(COLUMN_CREATED_AT),
            Self::quote_identifier(COLUMN_UPDATED_AT),
            Self::quote_identifier(COLUMN_PERMISSIONS),
        ];
        for attr in schema.persisted_attributes() {
            columns.push(Self::quote_identifier(attr.column));
        }
        columns.join(", ")
    }

    pub fn row_to_record(
        row: &sqlx::sqlite::SqliteRow,
        schema: &'static CollectionSchema,
    ) -> Result<StorageRecord, DatabaseError> {
        Self::row_to_record_prefixed(row, schema, None).map(|record| {
            record.expect("base records should always be present when decoding sqlite rows")
        })
    }

    pub fn row_to_record_prefixed(
        row: &sqlx::sqlite::SqliteRow,
        schema: &'static CollectionSchema,
        prefix: Option<&str>,
    ) -> Result<Option<StorageRecord>, DatabaseError> {
        let mut record = StorageRecord::new();
        let key = |column: &str| match prefix {
            Some(prefix) => format!("{prefix}__{column}"),
            None => column.to_string(),
        };

        let sequence_column = key(COLUMN_SEQUENCE);
        let sequence: Option<i64> = row
            .try_get(sequence_column.as_str())
            .map_err(|e| DatabaseError::Other(e.to_string()))?;
        let Some(sequence) = sequence else {
            return Ok(None);
        };

        let id_column = key(COLUMN_ID);
        let uid: String = row
            .try_get(id_column.as_str())
            .map_err(|e| DatabaseError::Other(e.to_string()))?;
        let created_column = key(COLUMN_CREATED_AT);
        let created_at_str: String = row
            .try_get(created_column.as_str())
            .map_err(|e| DatabaseError::Other(e.to_string()))?;
        let updated_column = key(COLUMN_UPDATED_AT);
        let updated_at_str: String = row
            .try_get(updated_column.as_str())
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

        let permissions_column = key(COLUMN_PERMISSIONS);
        let permissions_json: String = row
            .try_get(permissions_column.as_str())
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
            let column = key(attr.column);
            let column = column.as_str();
            let value = if attr.array {
                match row.try_get::<Option<String>, _>(column) {
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
                    | AttributeKind::Json
                    | AttributeKind::Enum => row
                        .try_get::<Option<String>, _>(column)
                        .map(|v| v.map(StorageValue::String).unwrap_or(StorageValue::Null))
                        .unwrap_or(StorageValue::Null),
                    AttributeKind::Integer | AttributeKind::Boolean => {
                        let v = row.try_get::<Option<i64>, _>(column).unwrap_or(None);
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
                        .try_get::<Option<f64>, _>(column)
                        .map(|v| v.map(StorageValue::Float).unwrap_or(StorageValue::Null))
                        .unwrap_or(StorageValue::Null),
                    AttributeKind::Timestamp => {
                        let s = row.try_get::<Option<String>, _>(column).unwrap_or(None);
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
        Ok(Some(record))
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
                | AttributeKind::Json
                | AttributeKind::Enum => Ok(if kind == AttributeKind::Enum {
                    StorageValue::EnumArray(
                        elems
                            .into_iter()
                            .map(|e| e.as_str().unwrap_or_default().to_string())
                            .collect(),
                    )
                } else {
                    StorageValue::StringArray(
                        elems
                            .into_iter()
                            .map(|e| e.as_str().unwrap_or_default().to_string())
                            .collect(),
                    )
                }),
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
                AttributeKind::String
                | AttributeKind::Relationship
                | AttributeKind::Virtual
                | AttributeKind::Enum => Ok(if kind == AttributeKind::Enum {
                    StorageValue::Enum(val.as_str().unwrap_or_default().to_string())
                } else {
                    StorageValue::String(val.as_str().unwrap_or_default().to_string())
                }),
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
