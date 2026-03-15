use database_core::errors::DatabaseError;
use database_core::query::{Filter, FilterOp};
use database_core::{
    CollectionSchema, COLUMN_ID, COLUMN_PERMISSIONS, COLUMN_SEQUENCE, COLUMN_CREATED_AT, COLUMN_UPDATED_AT,
    FIELD_ID, FIELD_PERMISSIONS, FIELD_SEQUENCE, FIELD_CREATED_AT, FIELD_UPDATED_AT,
};
use sqlx::{Sqlite, QueryBuilder};
use crate::utils::SqliteUtils;
use database_core::traits::storage::StorageValue;

pub struct SqliteQuery;

impl SqliteQuery {
    pub fn push_bind_value(builder: &mut QueryBuilder<'_, Sqlite>, value: &StorageValue) {
        match value {
            StorageValue::Null => { builder.push_bind(Option::<String>::None); }
            StorageValue::Bool(v) => { builder.push_bind(if *v { 1i64 } else { 0i64 }); }
            StorageValue::Int(v) => { builder.push_bind(*v); }
            StorageValue::Float(v) => { builder.push_bind(*v); }
            StorageValue::String(v) | StorageValue::Json(v) => { builder.push_bind(v.clone()); }
            StorageValue::Timestamp(v) => { builder.push_bind(v.format(&time::format_description::well_known::Rfc3339).unwrap()); }
            StorageValue::BoolArray(v) => { builder.push_bind(serde_json::to_string(v).unwrap()); }
            StorageValue::IntArray(v) => { builder.push_bind(serde_json::to_string(v).unwrap()); }
            StorageValue::FloatArray(v) => { builder.push_bind(serde_json::to_string(v).unwrap()); }
            StorageValue::StringArray(v) => { builder.push_bind(serde_json::to_string(v).unwrap()); }
            StorageValue::TimestampArray(v) => {
                let strings: Vec<String> = v.iter().map(|dt| dt.format(&time::format_description::well_known::Rfc3339).unwrap()).collect();
                builder.push_bind(serde_json::to_string(&strings).unwrap());
            }
            StorageValue::Bytes(v) => { builder.push_bind(v.clone()); }
        }
    }

    pub fn push_filter(builder: &mut QueryBuilder<'_, Sqlite>, schema: &'static CollectionSchema, filter: &Filter) -> Result<(), DatabaseError> {
        match filter {
            Filter::Field { field, op } => {
                let column = if let Some(attr) = schema.attribute(field) {
                    SqliteUtils::quote_identifier(attr.column)
                } else {
                    match *field {
                        FIELD_ID => COLUMN_ID.to_string(),
                        FIELD_SEQUENCE => COLUMN_SEQUENCE.to_string(),
                        FIELD_CREATED_AT => COLUMN_CREATED_AT.to_string(),
                        FIELD_UPDATED_AT => COLUMN_UPDATED_AT.to_string(),
                        FIELD_PERMISSIONS => COLUMN_PERMISSIONS.to_string(),
                        _ => return Err(DatabaseError::Other(format!("unknown field {field}"))),
                    }
                };

                match op {
                    FilterOp::Eq(StorageValue::Null) | FilterOp::IsNull => { builder.push(format!("{column} IS NULL")); }
                    FilterOp::NotEq(StorageValue::Null) | FilterOp::IsNotNull => { builder.push(format!("{column} IS NOT NULL")); }
                    FilterOp::Eq(v) => { builder.push(format!("{column} = ")); Self::push_bind_value(builder, v); }
                    FilterOp::NotEq(v) => { builder.push(format!("{column} <> ")); Self::push_bind_value(builder, v); }
                    FilterOp::Gt(v) => { builder.push(format!("{column} > ")); Self::push_bind_value(builder, v); }
                    FilterOp::Gte(v) => { builder.push(format!("{column} >= ")); Self::push_bind_value(builder, v); }
                    FilterOp::Lt(v) => { builder.push(format!("{column} < ")); Self::push_bind_value(builder, v); }
                    FilterOp::Lte(v) => { builder.push(format!("{column} <= ")); Self::push_bind_value(builder, v); }
                    FilterOp::In(vals) => {
                        if vals.is_empty() { builder.push("0 = 1"); }
                        else {
                            builder.push(format!("{column} IN ("));
                            let mut first = true;
                            for v in vals {
                                if !first { builder.push(", "); }
                                first = false;
                                Self::push_bind_value(builder, v);
                            }
                            builder.push(")");
                        }
                    }
                    FilterOp::Contains(v) => {
                        builder.push(format!("{column} LIKE '%' || "));
                        Self::push_bind_value(builder, v);
                        builder.push(" || '%'");
                    }
                    _ => return Err(DatabaseError::Other("unsupported filter op for sqlite".into())),
                }
            }
            Filter::And(filters) => {
                if filters.is_empty() { builder.push("1 = 1"); }
                else {
                    builder.push("(");
                    let mut first = true;
                    for f in filters {
                        if !first { builder.push(" AND "); }
                        first = false;
                        Self::push_filter(builder, schema, f)?;
                    }
                    builder.push(")");
                }
            }
            Filter::Or(filters) => {
                if filters.is_empty() { builder.push("0 = 1"); }
                else {
                    builder.push("(");
                    let mut first = true;
                    for f in filters {
                        if !first { builder.push(" OR "); }
                        first = false;
                        Self::push_filter(builder, schema, f)?;
                    }
                    builder.push(")");
                }
            }
            Filter::Not(f) => {
                builder.push("NOT (");
                Self::push_filter(builder, schema, f)?;
                builder.push(")");
            }
        }
        Ok(())
    }
}
