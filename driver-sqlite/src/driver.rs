use database_core::errors::DatabaseError;
use database_core::query::{Filter, FilterOp, QuerySpec, SortDirection};
use database_core::traits::storage::{AdapterFuture, StorageAdapter, StorageRecord, StorageValue};
use database_core::utils::{PermissionEnum};
use database_core::{
    AttributeKind, CollectionSchema, Context, COLUMN_CREATED_AT, COLUMN_ID,
    COLUMN_PERMISSIONS, COLUMN_SEQUENCE, COLUMN_UPDATED_AT, FIELD_CREATED_AT, FIELD_ID,
    FIELD_PERMISSIONS, FIELD_SEQUENCE, FIELD_UPDATED_AT,
};
use sqlx::{Pool, Sqlite, Row, QueryBuilder};
use time::OffsetDateTime;

pub struct SqliteAdapter<'a> {
    pool: &'a Pool<Sqlite>,
    any_pool: sqlx::AnyPool,
}

impl<'a> SqliteAdapter<'a> {
    pub fn new(pool: &'a Pool<Sqlite>) -> Self {
        Self { 
            pool,
            any_pool: pool.clone().into(),
        }
    }

    fn quote_identifier(identifier: &str) -> String {
        format!("\"{}\"", identifier.replace('"', "\"\""))
    }

    fn qualified_table_name(_context: &Context, collection: &str) -> String {
        Self::quote_identifier(collection)
    }

    fn qualified_permissions_table_name(context: &Context, collection: &str) -> String {
        Self::qualified_table_name(context, &format!("{collection}_perms"))
    }

    fn sql_type(kind: AttributeKind, array: bool) -> &'static str {
        if array {
            return "TEXT";
        }
        match kind {
            AttributeKind::String | AttributeKind::Relationship | AttributeKind::Virtual | AttributeKind::Json => "TEXT",
            AttributeKind::Integer => "INTEGER",
            AttributeKind::Float => "REAL",
            AttributeKind::Boolean => "INTEGER",
            AttributeKind::Timestamp => "TEXT",
        }
    }

    fn row_to_record(row: &sqlx::sqlite::SqliteRow, schema: &'static CollectionSchema) -> Result<StorageRecord, DatabaseError> {
        let mut record = StorageRecord::new();

        let sequence: i64 = row.try_get(COLUMN_SEQUENCE).map_err(|e| DatabaseError::Other(format!("sqlite sequence read failed: {e}")))?;
        let uid: String = row.try_get(COLUMN_ID).map_err(|e| DatabaseError::Other(format!("sqlite id read failed: {e}")))?;
        
        let created_at_str: String = row.try_get(COLUMN_CREATED_AT).map_err(|e| DatabaseError::Other(format!("sqlite createdAt read failed: {e}")))?;
        let updated_at_str: String = row.try_get(COLUMN_UPDATED_AT).map_err(|e| DatabaseError::Other(format!("sqlite updatedAt read failed: {e}")))?;
        
        let created_at = OffsetDateTime::parse(&created_at_str, &time::format_description::well_known::Rfc3339)
            .map_err(|e| DatabaseError::Other(format!("failed to parse createdAt: {e}")))?;
        let updated_at = OffsetDateTime::parse(&updated_at_str, &time::format_description::well_known::Rfc3339)
            .map_err(|e| DatabaseError::Other(format!("failed to parse updatedAt: {e}")))?;

        let permissions_json: String = row.try_get(COLUMN_PERMISSIONS).map_err(|e| DatabaseError::Other(format!("sqlite permissions read failed: {e}")))?;
        let permissions: Vec<String> = serde_json::from_str(&permissions_json).map_err(|e| DatabaseError::Other(format!("failed to parse permissions: {e}")))?;

        record.insert(FIELD_SEQUENCE.to_string(), StorageValue::Int(sequence));
        record.insert(FIELD_ID.to_string(), StorageValue::String(uid));
        record.insert(FIELD_CREATED_AT.to_string(), StorageValue::Timestamp(created_at));
        record.insert(FIELD_UPDATED_AT.to_string(), StorageValue::Timestamp(updated_at));
        record.insert(FIELD_PERMISSIONS.to_string(), StorageValue::StringArray(permissions));

        for attr in schema.persisted_attributes() {
            let column = attr.column;
            let value = if attr.array {
                match row.try_get::<Option<String>, _>(column) {
                    Ok(Some(json)) => {
                        let val: serde_json::Value = serde_json::from_str(&json).map_err(|e| DatabaseError::Other(format!("failed to parse array field {}: {}", attr.id, e)))?;
                        Self::json_to_storage_value(val, attr.kind, true)?
                    }
                    Ok(None) => StorageValue::Null,
                    Err(sqlx::Error::ColumnNotFound(_)) => continue,
                    Err(e) => return Err(DatabaseError::Other(format!("sqlite read error {}: {}", attr.id, e))),
                }
            } else {
                match attr.kind {
                    AttributeKind::String | AttributeKind::Relationship | AttributeKind::Virtual => {
                        match row.try_get::<Option<String>, _>(column) {
                            Ok(Some(v)) => StorageValue::String(v),
                            Ok(None) => StorageValue::Null,
                            Err(sqlx::Error::ColumnNotFound(_)) => continue,
                            Err(e) => return Err(DatabaseError::Other(format!("sqlite read error {}: {}", attr.id, e))),
                        }
                    }
                    AttributeKind::Integer | AttributeKind::Boolean => {
                        match row.try_get::<Option<i64>, _>(column) {
                            Ok(Some(v)) => if attr.kind == AttributeKind::Boolean { StorageValue::Bool(v != 0) } else { StorageValue::Int(v) },
                            Ok(None) => StorageValue::Null,
                            Err(sqlx::Error::ColumnNotFound(_)) => continue,
                            Err(e) => return Err(DatabaseError::Other(format!("sqlite read error {}: {}", attr.id, e))),
                        }
                    }
                    AttributeKind::Float => {
                        match row.try_get::<Option<f64>, _>(column) {
                            Ok(Some(v)) => StorageValue::Float(v),
                            Ok(None) => StorageValue::Null,
                            Err(sqlx::Error::ColumnNotFound(_)) => continue,
                            Err(e) => return Err(DatabaseError::Other(format!("sqlite read error {}: {}", attr.id, e))),
                        }
                    }
                    AttributeKind::Timestamp => {
                        match row.try_get::<Option<String>, _>(column) {
                            Ok(Some(v)) => {
                                let dt = OffsetDateTime::parse(&v, &time::format_description::well_known::Rfc3339)
                                    .map_err(|e| DatabaseError::Other(format!("failed to parse timestamp {}: {}", attr.id, e)))?;
                                StorageValue::Timestamp(dt)
                            }
                            Ok(None) => StorageValue::Null,
                            Err(sqlx::Error::ColumnNotFound(_)) => continue,
                            Err(e) => return Err(DatabaseError::Other(format!("sqlite read error {}: {}", attr.id, e))),
                        }
                    }
                    AttributeKind::Json => {
                        match row.try_get::<Option<String>, _>(column) {
                            Ok(Some(v)) => StorageValue::Json(v),
                            Ok(None) => StorageValue::Null,
                            Err(sqlx::Error::ColumnNotFound(_)) => continue,
                            Err(e) => return Err(DatabaseError::Other(format!("sqlite read error {}: {}", attr.id, e))),
                        }
                    }
                }
            };
            record.insert(attr.id.to_string(), value);
        }

        Ok(record)
    }

    fn json_to_storage_value(val: serde_json::Value, kind: AttributeKind, array: bool) -> Result<StorageValue, DatabaseError> {
        if array {
            let serde_json::Value::Array(elems) = val else { return Err(DatabaseError::Other("expected json array".into())); };
            match kind {
                AttributeKind::String | AttributeKind::Relationship | AttributeKind::Virtual | AttributeKind::Json => {
                    Ok(StorageValue::StringArray(elems.into_iter().map(|e| e.as_str().unwrap_or_default().to_string()).collect()))
                }
                AttributeKind::Integer => Ok(StorageValue::IntArray(elems.into_iter().map(|e| e.as_i64().unwrap_or_default()).collect())),
                AttributeKind::Boolean => Ok(StorageValue::BoolArray(elems.into_iter().map(|e| e.as_bool().unwrap_or_default()).collect())),
                AttributeKind::Float => Ok(StorageValue::FloatArray(elems.into_iter().map(|e| e.as_f64().unwrap_or_default()).collect())),
                AttributeKind::Timestamp => {
                    let mut dates = Vec::new();
                    for e in elems {
                        let s = e.as_str().ok_or_else(|| DatabaseError::Other("expected string for timestamp array".into()))?;
                        dates.push(OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339).map_err(|e| DatabaseError::Other(format!("timestamp parse error: {e}")))?);
                    }
                    Ok(StorageValue::TimestampArray(dates))
                }
            }
        } else {
            Err(DatabaseError::Other("scalar json_to_storage_value not implemented".into()))
        }
    }

    fn push_bind_value(builder: &mut QueryBuilder<'_, Sqlite>, value: &StorageValue) {
        match value {
            StorageValue::Null => { builder.push_bind(Option::<String>::None); }
            StorageValue::Bool(v) => { builder.push_bind(if *v { 1i64 } else { 0i64 }); }
            StorageValue::Int(v) => { builder.push_bind(*v); }
            StorageValue::Float(v) => { builder.push_bind(*v); }
            StorageValue::String(v) | StorageValue::Json(v) => { builder.push_bind(v.clone()); }
            StorageValue::Timestamp(v) => { 
                builder.push_bind(v.format(&time::format_description::well_known::Rfc3339).unwrap()); 
            }
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

    fn push_filter(builder: &mut QueryBuilder<'_, Sqlite>, schema: &'static CollectionSchema, filter: &Filter) -> Result<(), DatabaseError> {
        match filter {
            Filter::Field { field, op } => {
                let column = if let Some(attr) = schema.attribute(field) {
                    Self::quote_identifier(attr.column)
                } else if database_core::system_fields::is_system_field(field) {
                    match *field {
                        FIELD_ID => COLUMN_ID.to_string(),
                        FIELD_SEQUENCE => COLUMN_SEQUENCE.to_string(),
                        FIELD_CREATED_AT => COLUMN_CREATED_AT.to_string(),
                        FIELD_UPDATED_AT => COLUMN_UPDATED_AT.to_string(),
                        FIELD_PERMISSIONS => COLUMN_PERMISSIONS.to_string(),
                        _ => return Err(DatabaseError::Other(format!("unknown system field {field}"))),
                    }
                } else {
                    return Err(DatabaseError::Other(format!("unknown field {field}")));
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
                        if vals.is_empty() {
                            builder.push("0 = 1");
                        } else {
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
                    FilterOp::StartsWith(v) => {
                        builder.push(format!("{column} LIKE "));
                        Self::push_bind_value(builder, v);
                        builder.push(" || '%'");
                    }
                    FilterOp::EndsWith(v) => {
                        builder.push(format!("{column} LIKE '%' || "));
                        Self::push_bind_value(builder, v);
                    }
                    FilterOp::TextSearch(v) => {
                        builder.push(format!("{column} LIKE '%' || "));
                        Self::push_bind_value(builder, v);
                        builder.push(" || '%'");
                    }
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

impl<'a> StorageAdapter for SqliteAdapter<'a> {
    fn enforces_document_filtering(&self, _action: PermissionEnum) -> bool {
        true
    }

    fn pool_any(&self) -> &sqlx::AnyPool {
        &self.any_pool
    }

    fn ping(&self, _context: &Context) -> AdapterFuture<'_, Result<(), DatabaseError>> {
        let pool = self.pool;
        Box::pin(async move {
            sqlx::query("SELECT 1").execute(pool).await
                .map(|_| ())
                .map_err(|e| DatabaseError::Other(format!("sqlite ping failed: {e}")))
        })
    }

    fn create_collection(&self, context: &Context, schema: &'static CollectionSchema) -> AdapterFuture<'_, Result<(), DatabaseError>> {
        let pool = self.pool;
        let context = context.clone();
        Box::pin(async move {
            let table = Self::qualified_table_name(&context, schema.id);
            let perms_table = Self::qualified_permissions_table_name(&context, schema.id);

            let mut cols = vec![
                format!("{} INTEGER PRIMARY KEY AUTOINCREMENT", COLUMN_SEQUENCE),
                format!("{} TEXT NOT NULL UNIQUE", COLUMN_ID),
                format!("{} TEXT NOT NULL", COLUMN_CREATED_AT),
                format!("{} TEXT NOT NULL", COLUMN_UPDATED_AT),
                format!("{} TEXT NOT NULL DEFAULT '[]'", COLUMN_PERMISSIONS),
            ];

            for attr in schema.persisted_attributes() {
                let sql_type = Self::sql_type(attr.kind, attr.array);
                let nullable = if attr.required { "NOT NULL" } else { "DEFAULT NULL" };
                cols.push(format!("{} {} {}", Self::quote_identifier(attr.column), sql_type, nullable));
            }

            let create_table = format!("CREATE TABLE IF NOT EXISTS {table} ({})", cols.join(", "));
            let create_perms = format!("CREATE TABLE IF NOT EXISTS {perms_table} (document_id INTEGER NOT NULL, permission_type TEXT NOT NULL, permissions TEXT NOT NULL DEFAULT '[]', PRIMARY KEY (document_id, permission_type), FOREIGN KEY (document_id) REFERENCES {table}({}) ON DELETE CASCADE)", COLUMN_SEQUENCE);

            let mut tx = pool.begin().await.map_err(|e| DatabaseError::Other(e.to_string()))?;
            sqlx::query(&create_table).execute(&mut *tx).await.map_err(|e| DatabaseError::Other(e.to_string()))?;
            sqlx::query(&create_perms).execute(&mut *tx).await.map_err(|e| DatabaseError::Other(e.to_string()))?;
            
            for index in schema.indexes {
                let index_name = Self::quote_identifier(&index.id);
                let mut idx_cols = Vec::new();
                for (i, attr_id) in index.attributes.iter().enumerate() {
                    let attr = schema.attribute(attr_id).unwrap();
                    let mut col = Self::quote_identifier(attr.column);
                    if let Some(order) = index.orders.get(i) {
                        match order {
                            database_core::Order::Asc => col.push_str(" ASC"),
                            database_core::Order::Desc => col.push_str(" DESC"),
                            database_core::Order::None => {}
                        }
                    }
                    idx_cols.push(col);
                }
                let unique = if index.kind == database_core::IndexKind::Unique { "UNIQUE " } else { "" };
                let create_idx = format!("CREATE {unique}INDEX IF NOT EXISTS {index_name} ON {table} ({})", idx_cols.join(", "));
                sqlx::query(&create_idx).execute(&mut *tx).await.map_err(|e| DatabaseError::Other(e.to_string()))?;
            }

            tx.commit().await.map_err(|e| DatabaseError::Other(e.to_string()))?;
            Ok(())
        })
    }

    fn insert(&self, context: &Context, schema: &'static CollectionSchema, values: StorageRecord) -> AdapterFuture<'_, Result<StorageRecord, DatabaseError>> {
        let pool = self.pool;
        let context = context.clone();
        Box::pin(async move {
            let results = self.insert_many(&context, schema, vec![values]).await?;
            Ok(results.into_iter().next().unwrap())
        })
    }

    fn insert_many(&self, context: &Context, schema: &'static CollectionSchema, values: Vec<StorageRecord>) -> AdapterFuture<'_, Result<Vec<StorageRecord>, DatabaseError>> {
        let pool = self.pool;
        let context = context.clone();
        Box::pin(async move {
            if values.is_empty() { return Ok(Vec::new()); }
            let table = Self::qualified_table_name(&context, schema.id);
            let perms_table = Self::qualified_permissions_table_name(&context, schema.id);
            
            let mut tx = pool.begin().await.map_err(|e| DatabaseError::Other(e.to_string()))?;
            let mut results = Vec::new();

            for mut record in values {
                let uid = match record.get(FIELD_ID).unwrap() {
                    StorageValue::String(s) => s.clone(),
                    _ => return Err(DatabaseError::Other("missing id".into())),
                };
                let created_at = match record.get(FIELD_CREATED_AT).unwrap() {
                    StorageValue::Timestamp(t) => *t,
                    _ => return Err(DatabaseError::Other("missing createdAt".into())),
                };
                let updated_at = match record.get(FIELD_UPDATED_AT).unwrap() {
                    StorageValue::Timestamp(t) => *t,
                    _ => return Err(DatabaseError::Other("missing updatedAt".into())),
                };
                let permissions = match record.get(FIELD_PERMISSIONS).unwrap() {
                    StorageValue::StringArray(a) => a.clone(),
                    _ => return Err(DatabaseError::Other("missing permissions".into())),
                };

                let mut builder = QueryBuilder::<Sqlite>::new(format!("INSERT INTO {table} ("));
                builder.push(format!("{COLUMN_ID}, {COLUMN_CREATED_AT}, {COLUMN_UPDATED_AT}, {COLUMN_PERMISSIONS}"));
                
                let attrs: Vec<_> = schema.persisted_attributes().into_iter().filter(|a| record.contains_key(a.id)).collect();
                for a in &attrs {
                    builder.push(", ");
                    builder.push(Self::quote_identifier(a.column));
                }
                builder.push(") VALUES (");
                builder.push_bind(uid);
                builder.push_bind(created_at.format(&time::format_description::well_known::Rfc3339).unwrap());
                builder.push_bind(updated_at.format(&time::format_description::well_known::Rfc3339).unwrap());
                builder.push_bind(serde_json::to_string(&permissions).unwrap());

                for a in &attrs {
                    builder.push(", ");
                    Self::push_bind_value(&mut builder, record.get(a.id).unwrap());
                }
                builder.push(") RETURNING ");
                builder.push(COLUMN_SEQUENCE);

                let row = builder.build().fetch_one(&mut *tx).await.map_err(|e| DatabaseError::Other(e.to_string()))?;
                let sequence: i64 = row.get(0);

                sqlx::query(&format!("DELETE FROM {perms_table} WHERE document_id = ?")).bind(sequence).execute(&mut *tx).await.map_err(|e| DatabaseError::Other(e.to_string()))?;
                let grouped = database_core::utils::permission_rows(&permissions)?;
                for (pt, pv) in grouped {
                    sqlx::query(&format!("INSERT INTO {perms_table} (document_id, permission_type, permissions) VALUES (?, ?, ?)")).bind(sequence).bind(pt).bind(serde_json::to_string(&pv).unwrap()).execute(&mut *tx).await.map_err(|e| DatabaseError::Other(e.to_string()))?;
                }

                record.insert(FIELD_SEQUENCE.to_string(), StorageValue::Int(sequence));
                results.push(record);
            }

            tx.commit().await.map_err(|e| DatabaseError::Other(e.to_string()))?;
            Ok(results)
        })
    }

    fn get(&self, context: &Context, schema: &'static CollectionSchema, id: &str) -> AdapterFuture<'_, Result<Option<StorageRecord>, DatabaseError>> {
        let pool = self.pool;
        let context = context.clone();
        let id = id.to_string();
        Box::pin(async move {
            let table = Self::qualified_table_name(&context, schema.id);
            let perms_table = Self::qualified_permissions_table_name(&context, schema.id);
            
            let mut builder = QueryBuilder::<Sqlite>::new(format!("SELECT * FROM {table} AS main "));
            builder.push(" WHERE ");
            builder.push(COLUMN_ID);
            builder.push(" = ");
            builder.push_bind(id);

            if context.authorization_enabled() {
                let roles: Vec<String> = context.roles().map(|r| r.to_string()).collect();
                builder.push(format!(" AND EXISTS (SELECT 1 FROM {perms_table} p WHERE p.document_id = main.{COLUMN_SEQUENCE} AND p.permission_type = 'read' AND ("));
                builder.push("EXISTS (SELECT 1 FROM json_each(p.permissions) WHERE value IN (");
                let mut first = true;
                for role in roles {
                    if !first { builder.push(", "); }
                    first = false;
                    builder.push_bind(role);
                }
                builder.push("))))");
            }

            let row = builder.build().fetch_optional(pool).await.map_err(|e| DatabaseError::Other(e.to_string()))?;
            match row {
                Some(r) => Ok(Some(Self::row_to_record(&r, schema)?)),
                None => Ok(None),
            }
        })
    }

    fn update(&self, context: &Context, schema: &'static CollectionSchema, id: &str, values: StorageRecord) -> AdapterFuture<'_, Result<Option<StorageRecord>, DatabaseError>> {
        let pool = self.pool;
        let context = context.clone();
        let id = id.to_string();
        Box::pin(async move {
            let table = Self::qualified_table_name(&context, schema.id);
            let perms_table = Self::qualified_permissions_table_name(&context, schema.id);
            
            let mut tx = pool.begin().await.map_err(|e| DatabaseError::Other(e.to_string()))?;
            
            let mut builder = QueryBuilder::<Sqlite>::new(format!("UPDATE {table} SET "));
            let mut first = true;
            let attrs: Vec<_> = schema.persisted_attributes().into_iter().filter(|a| values.contains_key(a.id)).collect();
            
            for a in &attrs {
                if !first { builder.push(", "); }
                first = false;
                builder.push(Self::quote_identifier(a.column));
                builder.push(" = ");
                Self::push_bind_value(&mut builder, values.get(a.id).unwrap());
            }

            if let Some(val) = values.get(FIELD_UPDATED_AT) {
                if !first { builder.push(", "); }
                builder.push(COLUMN_UPDATED_AT);
                builder.push(" = ");
                let dt = match val {
                    StorageValue::Timestamp(t) => *t,
                    _ => return Err(DatabaseError::Other("expected timestamp for updatedAt".into())),
                };
                builder.push_bind(dt.format(&time::format_description::well_known::Rfc3339).unwrap());
            }

            builder.push(" WHERE ");
            builder.push(COLUMN_ID);
            builder.push(" = ");
            builder.push_bind(&id);

            if context.authorization_enabled() {
                let roles: Vec<String> = context.roles().map(|r| r.to_string()).collect();
                builder.push(format!(" AND EXISTS (SELECT 1 FROM {perms_table} p WHERE p.document_id = {table}.{COLUMN_SEQUENCE} AND p.permission_type = 'update' AND ("));
                builder.push("EXISTS (SELECT 1 FROM json_each(p.permissions) WHERE value IN (");
                let mut first_role = true;
                for role in roles {
                    if !first_role { builder.push(", "); }
                    first_role = false;
                    builder.push_bind(role);
                }
                builder.push("))))");
            }

            let result = builder.build().execute(&mut *tx).await.map_err(|e| DatabaseError::Other(e.to_string()))?;
            if result.rows_affected() == 0 {
                return Ok(None);
            }

            if let Some(StorageValue::StringArray(perms)) = values.get(FIELD_PERMISSIONS) {
                let row = sqlx::query(&format!("SELECT {COLUMN_SEQUENCE} FROM {table} WHERE {COLUMN_ID} = ?")).bind(&id).fetch_one(&mut *tx).await.map_err(|e| DatabaseError::Other(e.to_string()))?;
                let seq: i64 = row.get(0);
                sqlx::query(&format!("DELETE FROM {perms_table} WHERE document_id = ?")).bind(seq).execute(&mut *tx).await.map_err(|e| DatabaseError::Other(e.to_string()))?;
                let grouped = database_core::utils::permission_rows(perms)?;
                for (pt, pv) in grouped {
                    sqlx::query(&format!("INSERT INTO {perms_table} (document_id, permission_type, permissions) VALUES (?, ?, ?)")).bind(seq).bind(pt).bind(serde_json::to_string(&pv).unwrap()).execute(&mut *tx).await.map_err(|e| DatabaseError::Other(e.to_string()))?;
                }
            }

            tx.commit().await.map_err(|e| DatabaseError::Other(e.to_string()))?;
            self.get(&context, schema, &id).await
        })
    }

    fn update_many(&self, _context: &Context, _schema: &'static CollectionSchema, _query: &QuerySpec, _values: StorageRecord) -> AdapterFuture<'_, Result<u64, DatabaseError>> {
        Box::pin(async move { Err(DatabaseError::Other("update_many not implemented for sqlite".into())) })
    }

    fn delete(&self, context: &Context, schema: &'static CollectionSchema, id: &str) -> AdapterFuture<'_, Result<bool, DatabaseError>> {
        let pool = self.pool;
        let context = context.clone();
        let id = id.to_string();
        Box::pin(async move {
            let table = Self::qualified_table_name(&context, schema.id);
            let perms_table = Self::qualified_permissions_table_name(&context, schema.id);
            
            let mut builder = QueryBuilder::<Sqlite>::new(format!("DELETE FROM {table} "));
            builder.push(" WHERE ");
            builder.push(COLUMN_ID);
            builder.push(" = ");
            builder.push_bind(&id);

            if context.authorization_enabled() {
                let roles: Vec<String> = context.roles().map(|r| r.to_string()).collect();
                builder.push(format!(" AND EXISTS (SELECT 1 FROM {perms_table} p WHERE p.document_id = {table}.{COLUMN_SEQUENCE} AND p.permission_type = 'delete' AND ("));
                builder.push("EXISTS (SELECT 1 FROM json_each(p.permissions) WHERE value IN (");
                let mut first = true;
                for role in roles {
                    if !first { builder.push(", "); }
                    first = false;
                    builder.push_bind(role);
                }
                builder.push("))))");
            }

            let result = builder.build().execute(pool).await.map_err(|e| DatabaseError::Other(e.to_string()))?;
            Ok(result.rows_affected() > 0)
        })
    }

    fn delete_many(&self, _context: &Context, _schema: &'static CollectionSchema, _query: &QuerySpec) -> AdapterFuture<'_, Result<u64, DatabaseError>> {
        Box::pin(async move { Err(DatabaseError::Other("delete_many not implemented for sqlite".into())) })
    }

    fn find(&self, context: &Context, schema: &'static CollectionSchema, query: &QuerySpec) -> AdapterFuture<'_, Result<Vec<StorageRecord>, DatabaseError>> {
        let pool = self.pool;
        let context = context.clone();
        let query = query.clone();
        Box::pin(async move {
            let table = Self::qualified_table_name(&context, schema.id);
            let perms_table = Self::qualified_permissions_table_name(&context, schema.id);
            
            let mut builder = QueryBuilder::<Sqlite>::new("SELECT * FROM ");
            builder.push(&table);
            builder.push(" AS main ");

            let mut has_where = false;
            if context.authorization_enabled() {
                builder.push(" WHERE ");
                has_where = true;
                let roles: Vec<String> = context.roles().map(|r| r.to_string()).collect();
                builder.push(format!(" EXISTS (SELECT 1 FROM {perms_table} p WHERE p.document_id = main.{COLUMN_SEQUENCE} AND p.permission_type = 'read' AND ("));
                builder.push("EXISTS (SELECT 1 FROM json_each(p.permissions) WHERE value IN (");
                let mut first = true;
                for role in roles {
                    if !first { builder.push(", "); }
                    first = false;
                    builder.push_bind(role);
                }
                builder.push("))))");
            }

            for f in query.filters() {
                if !has_where { builder.push(" WHERE "); has_where = true; }
                else { builder.push(" AND "); }
                Self::push_filter(&mut builder, schema, f)?;
            }

            if !query.sorts().is_empty() {
                builder.push(" ORDER BY ");
                let mut first = true;
                for s in query.sorts() {
                    if !first { builder.push(", "); }
                    first = false;
                    let col = if let Some(a) = schema.attribute(s.field) { Self::quote_identifier(a.column) }
                             else { s.field.to_string() };
                    builder.push(col);
                    match s.direction {
                        SortDirection::Asc => { builder.push(" ASC"); }
                        SortDirection::Desc => { builder.push(" DESC"); }
                    }
                }
            }

            if let Some(l) = query.limit_value() {
                builder.push(" LIMIT ");
                builder.push_bind(l as i64);
            }
            if let Some(o) = query.offset_value() {
                builder.push(" OFFSET ");
                builder.push_bind(o as i64);
            }

            let rows = builder.build().fetch_all(pool).await.map_err(|e| DatabaseError::Other(e.to_string()))?;
            let mut results = Vec::new();
            for r in rows {
                results.push(Self::row_to_record(&r, schema)?);
            }
            Ok(results)
        })
    }

    fn count(&self, context: &Context, schema: &'static CollectionSchema, query: &QuerySpec) -> AdapterFuture<'_, Result<u64, DatabaseError>> {
        let pool = self.pool;
        let context = context.clone();
        let query = query.clone();
        Box::pin(async move {
            let table = Self::qualified_table_name(&context, schema.id);
            let perms_table = Self::qualified_permissions_table_name(&context, schema.id);
            
            let mut builder = QueryBuilder::<Sqlite>::new("SELECT COUNT(*) FROM ");
            builder.push(&table);
            builder.push(" AS main ");

            let mut has_where = false;
            if context.authorization_enabled() {
                builder.push(" WHERE ");
                has_where = true;
                let roles: Vec<String> = context.roles().map(|r| r.to_string()).collect();
                builder.push(format!(" EXISTS (SELECT 1 FROM {perms_table} p WHERE p.document_id = main.{COLUMN_SEQUENCE} AND p.permission_type = 'read' AND ("));
                builder.push("EXISTS (SELECT 1 FROM json_each(p.permissions) WHERE value IN (");
                let mut first = true;
                for role in roles {
                    if !first { builder.push(", "); }
                    first = false;
                    builder.push_bind(role);
                }
                builder.push("))))");
            }

            for f in query.filters() {
                if !has_where { builder.push(" WHERE "); has_where = true; }
                else { builder.push(" AND "); }
                Self::push_filter(&mut builder, schema, f)?;
            }

            let row = builder.build().fetch_one(pool).await.map_err(|e| DatabaseError::Other(e.to_string()))?;
            let count: i64 = row.get(0);
            Ok(count as u64)
        })
    }
}
