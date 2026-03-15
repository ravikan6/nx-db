use database_core::errors::DatabaseError;
use database_core::query::{QuerySpec, SortDirection};
use database_core::traits::storage::{AdapterFuture, StorageAdapter, StorageRecord, StorageValue};
use database_core::utils::{PermissionEnum};
use database_core::{CollectionSchema, Context, COLUMN_ID, COLUMN_SEQUENCE, FIELD_ID, FIELD_CREATED_AT, FIELD_UPDATED_AT, FIELD_PERMISSIONS, FIELD_SEQUENCE};
use sqlx::{Pool, Sqlite, Row, QueryBuilder};

use crate::utils::SqliteUtils;
use crate::query::SqliteQuery;

#[derive(Clone)]
pub struct SqliteAdapter {
    pool: Pool<Sqlite>,
}

impl SqliteAdapter {
    pub fn new(pool: Pool<Sqlite>) -> Self {
        // Sqlite busy timeout handling
        let _pool_clone = pool.clone(); 
        tokio::spawn(async move { 
            let _ = sqlx::query("PRAGMA busy_timeout = 5000").execute(&_pool_clone).await; 
        });
        
        Self { pool }
    }

    pub fn quote_identifier(identifier: &str) -> String {
        SqliteUtils::quote_identifier(identifier)
    }

    pub fn sql_type(kind: database_core::AttributeKind, array: bool) -> &'static str {
        SqliteUtils::sql_type(kind, array)
    }

    pub fn qualified_table_name(context: &Context, collection: &str) -> String {
        SqliteUtils::qualified_table_name(context, collection)
    }

    pub fn get_pool(&self) -> &Pool<Sqlite> {
        &self.pool
    }
}

impl StorageAdapter for SqliteAdapter {
    fn enforces_document_filtering(&self, _action: PermissionEnum) -> bool {
        true
    }

    fn ping(&self, _context: &Context) -> AdapterFuture<'_, Result<(), DatabaseError>> {
        let pool = self.pool.clone();
        Box::pin(async move {
            sqlx::query("SELECT 1").execute(&pool).await
                .map(|_| ())
                .map_err(|e| DatabaseError::Other(e.to_string()))
        })
    }

    fn create_collection(&self, context: &Context, schema: &'static CollectionSchema) -> AdapterFuture<'_, Result<(), DatabaseError>> {
        let pool = self.pool.clone();
        let context = context.clone();
        Box::pin(async move {
            let table = SqliteUtils::qualified_table_name(&context, schema.id);
            let mut cols = vec![
                format!("{} INTEGER PRIMARY KEY AUTOINCREMENT", database_core::COLUMN_SEQUENCE),
                format!("{} TEXT NOT NULL UNIQUE", database_core::COLUMN_ID),
                format!("{} TEXT NOT NULL", database_core::COLUMN_CREATED_AT),
                format!("{} TEXT NOT NULL", database_core::COLUMN_UPDATED_AT),
                format!("{} TEXT NOT NULL DEFAULT '[]'", database_core::COLUMN_PERMISSIONS),
            ];
            for attr in schema.persisted_attributes() {
                let st = SqliteUtils::sql_type(attr.kind, attr.array);
                cols.push(format!("{} {} {}", SqliteUtils::quote_identifier(attr.column), st, if attr.required { "NOT NULL" } else { "DEFAULT NULL" }));
            }
            let sql = format!("CREATE TABLE IF NOT EXISTS {table} ({})", cols.join(", "));
            sqlx::query(&sql).execute(&pool).await.map_err(|e| DatabaseError::Other(e.to_string()))?;
            Ok(())
        })
    }

    fn insert(&self, context: &Context, schema: &'static CollectionSchema, values: StorageRecord) -> AdapterFuture<'_, Result<StorageRecord, DatabaseError>> {
        let pool = self.pool.clone();
        let context = context.clone();
        Box::pin(async move {
            let results = self.insert_many(&context, schema, vec![values]).await?;
            Ok(results.into_iter().next().unwrap())
        })
    }

    fn insert_many(&self, context: &Context, schema: &'static CollectionSchema, values: Vec<StorageRecord>) -> AdapterFuture<'_, Result<Vec<StorageRecord>, DatabaseError>> {
        let pool = self.pool.clone();
        let context = context.clone();
        Box::pin(async move {
            if values.is_empty() { return Ok(Vec::new()); }
            let table = SqliteUtils::qualified_table_name(&context, schema.id);
            let mut tx = pool.begin().await.map_err(|e| DatabaseError::Other(e.to_string()))?;
            let mut results = Vec::new();
            let attrs: Vec<_> = schema.persisted_attributes().collect();

            for mut record in values {
                let mut builder = QueryBuilder::<Sqlite>::new(format!("INSERT INTO {table} ("));
                builder.push(format!("{COLUMN_ID}, {}, {}, {}", database_core::COLUMN_CREATED_AT, database_core::COLUMN_UPDATED_AT, database_core::COLUMN_PERMISSIONS));
                for a in &attrs { builder.push(", "); builder.push(SqliteUtils::quote_identifier(a.column)); }
                builder.push(") VALUES (");
                
                builder.push_bind(record.get(FIELD_ID).unwrap().as_str().unwrap().to_string());
                builder.push_bind(record.get(FIELD_CREATED_AT).unwrap().as_timestamp().unwrap().format(&time::format_description::well_known::Rfc3339).unwrap());
                builder.push_bind(record.get(FIELD_UPDATED_AT).unwrap().as_timestamp().unwrap().format(&time::format_description::well_known::Rfc3339).unwrap());
                builder.push_bind(serde_json::to_string(record.get(FIELD_PERMISSIONS).unwrap().as_string_array().unwrap()).unwrap());

                for a in &attrs {
                    builder.push(", ");
                    SqliteQuery::push_bind_value(&mut builder, record.get(a.id).unwrap_or(&StorageValue::Null));
                }
                builder.push(") RETURNING ");
                builder.push(COLUMN_SEQUENCE);

                let row = builder.build().fetch_one(&mut *tx).await.map_err(|e| DatabaseError::Other(e.to_string()))?;
                record.insert(FIELD_SEQUENCE.to_string(), StorageValue::Int(row.get(0)));
                results.push(record);
            }
            tx.commit().await.map_err(|e| DatabaseError::Other(e.to_string()))?;
            Ok(results)
        })
    }

    fn get(&self, context: &Context, schema: &'static CollectionSchema, id: &str) -> AdapterFuture<'_, Result<Option<StorageRecord>, DatabaseError>> {
        let pool = self.pool.clone();
        let context = context.clone();
        let id = id.to_string();
        Box::pin(async move {
            let table = SqliteUtils::qualified_table_name(&context, schema.id);
            let mut builder = QueryBuilder::<Sqlite>::new(format!("SELECT * FROM {table} WHERE {COLUMN_ID} = "));
            builder.push_bind(id);
            let row = builder.build().fetch_optional(&pool).await.map_err(|e| DatabaseError::Other(e.to_string()))?;
            match row {
                Some(r) => Ok(Some(SqliteUtils::row_to_record(&r, schema)?)),
                None => Ok(None)
            }
        })
    }

    fn update(&self, _context: &Context, _schema: &'static CollectionSchema, _id: &str, _values: StorageRecord) -> AdapterFuture<'_, Result<Option<StorageRecord>, DatabaseError>> {
        Box::pin(async move { Err(DatabaseError::Other("sqlite update not implemented yet".into())) })
    }

    fn update_many(&self, _context: &Context, _schema: &'static CollectionSchema, _query: &QuerySpec, _values: StorageRecord) -> AdapterFuture<'_, Result<u64, DatabaseError>> {
        Box::pin(async move { Err(DatabaseError::Other("sqlite update_many not implemented yet".into())) })
    }

    fn delete(&self, context: &Context, schema: &'static CollectionSchema, id: &str) -> AdapterFuture<'_, Result<bool, DatabaseError>> {
        let pool = self.pool.clone();
        let context = context.clone();
        let id = id.to_string();
        Box::pin(async move {
            let table = SqliteUtils::qualified_table_name(&context, schema.id);
            let mut builder = QueryBuilder::<Sqlite>::new(format!("DELETE FROM {table} WHERE {COLUMN_ID} = "));
            builder.push_bind(id);
            let res = builder.build().execute(&pool).await.map_err(|e| DatabaseError::Other(e.to_string()))?;
            Ok(res.rows_affected() > 0)
        })
    }

    fn delete_many(&self, _context: &Context, _schema: &'static CollectionSchema, _query: &QuerySpec) -> AdapterFuture<'_, Result<u64, DatabaseError>> {
        Box::pin(async move { Err(DatabaseError::Other("sqlite delete_many not implemented yet".into())) })
    }

    fn find(&self, context: &Context, schema: &'static CollectionSchema, query: &QuerySpec) -> AdapterFuture<'_, Result<Vec<StorageRecord>, DatabaseError>> {
        let pool = self.pool.clone();
        let context = context.clone();
        let query = query.clone();
        Box::pin(async move {
            let table = SqliteUtils::qualified_table_name(&context, schema.id);
            let mut builder = QueryBuilder::<Sqlite>::new(format!("SELECT * FROM {table}"));
            let mut has_where = false;
            for f in query.filters() {
                if !has_where { builder.push(" WHERE "); has_where = true; }
                else { builder.push(" AND "); }
                SqliteQuery::push_filter(&mut builder, schema, f)?;
            }
            if !query.sorts().is_empty() {
                builder.push(" ORDER BY ");
                let mut first = true;
                for s in query.sorts() {
                    if !first { builder.push(", "); }
                    first = false;
                    let col = if let Some(a) = schema.attribute(s.field) { SqliteUtils::quote_identifier(a.column) } else { s.field.to_string() };
                    builder.push(col);
                    match s.direction { SortDirection::Asc => { builder.push(" ASC"); } SortDirection::Desc => { builder.push(" DESC"); } }
                }
            }
            if let Some(l) = query.limit_value() { builder.push(" LIMIT "); builder.push_bind(l as i64); }
            if let Some(o) = query.offset_value() { builder.push(" OFFSET "); builder.push_bind(o as i64); }
            let rows = builder.build().fetch_all(&pool).await.map_err(|e| DatabaseError::Other(e.to_string()))?;
            rows.into_iter().map(|r| SqliteUtils::row_to_record(&r, schema)).collect()
        })
    }

    fn count(&self, context: &Context, schema: &'static CollectionSchema, query: &QuerySpec) -> AdapterFuture<'_, Result<u64, DatabaseError>> {
        let pool = self.pool.clone();
        let context = context.clone();
        let query = query.clone();
        Box::pin(async move {
            let table = SqliteUtils::qualified_table_name(&context, schema.id);
            let mut builder = QueryBuilder::<Sqlite>::new(format!("SELECT COUNT(*) FROM {table}"));
            let mut has_where = false;
            for f in query.filters() {
                if !has_where { builder.push(" WHERE "); has_where = true; }
                else { builder.push(" AND "); }
                SqliteQuery::push_filter(&mut builder, schema, f)?;
            }
            let row = builder.build().fetch_one(&pool).await.map_err(|e| DatabaseError::Other(e.to_string()))?;
            let count: i64 = row.get(0);
            Ok(count as u64)
        })
    }

}
