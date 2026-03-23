use database_core::errors::DatabaseError;
use database_core::query::{QuerySpec, SortDirection};
use database_core::traits::storage::{AdapterFuture, StorageAdapter, StorageRecord, StorageValue};
use database_core::utils::PermissionEnum;
use database_core::{
    COLUMN_ID, COLUMN_SEQUENCE, CollectionSchema, Context, FIELD_CREATED_AT, FIELD_ID,
    FIELD_PERMISSIONS, FIELD_SEQUENCE, FIELD_UPDATED_AT,
};
use sqlx::{Pool, QueryBuilder, Row, Sqlite};

use crate::query::SqliteQuery;
use crate::utils::SqliteUtils;

#[derive(Clone)]
pub struct SqliteAdapter {
    pool: Pool<Sqlite>,
}

impl SqliteAdapter {
    pub fn new(pool: Pool<Sqlite>) -> Self {
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
            sqlx::query("SELECT 1")
                .execute(&pool)
                .await
                .map(|_| ())
                .map_err(|e| DatabaseError::Other(e.to_string()))
        })
    }

    fn create_collection(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
    ) -> AdapterFuture<'_, Result<(), DatabaseError>> {
        let pool = self.pool.clone();
        let context = context.clone();
        Box::pin(async move {
            let table = SqliteUtils::qualified_table_name(&context, schema.id);
            let mut cols = vec![
                format!(
                    "{} INTEGER PRIMARY KEY AUTOINCREMENT",
                    database_core::COLUMN_SEQUENCE
                ),
                format!("{} TEXT NOT NULL UNIQUE", database_core::COLUMN_ID),
                format!("{} TEXT NOT NULL", database_core::COLUMN_CREATED_AT),
                format!("{} TEXT NOT NULL", database_core::COLUMN_UPDATED_AT),
                format!(
                    "{} TEXT NOT NULL DEFAULT '[]'",
                    database_core::COLUMN_PERMISSIONS
                ),
            ];
            for attr in schema.persisted_attributes() {
                let st = SqliteUtils::sql_type(attr.kind, attr.array);
                let not_null = if attr.required { " NOT NULL" } else { "" };
                let default_clause = SqliteUtils::sql_default(attr.default);
                cols.push(format!(
                    "{} {}{}{}",
                    SqliteUtils::quote_identifier(attr.column),
                    st,
                    not_null,
                    default_clause,
                ));
            }
            let sql = format!("CREATE TABLE IF NOT EXISTS {table} ({})", cols.join(", "));
            sqlx::query(&sql)
                .execute(&pool)
                .await
                .map_err(|e| DatabaseError::Other(e.to_string()))?;

            // Create the permissions table for document-level security.
            let perms_table = SqliteUtils::qualified_permissions_table_name(&context, schema.id);
            let perms_sql = format!(
                "CREATE TABLE IF NOT EXISTS {perms_table} (\
                    document_id INTEGER NOT NULL REFERENCES {table}({seq}) ON DELETE CASCADE, \
                    permission_type TEXT NOT NULL, \
                    permissions TEXT NOT NULL DEFAULT '[]', \
                    PRIMARY KEY (document_id, permission_type)\
                )",
                seq = database_core::COLUMN_SEQUENCE,
            );
            sqlx::query(&perms_sql)
                .execute(&pool)
                .await
                .map_err(|e| DatabaseError::Other(e.to_string()))?;

            Ok(())
        })
    }

    fn insert(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        values: StorageRecord,
    ) -> AdapterFuture<'_, Result<StorageRecord, DatabaseError>> {
        let _pool = self.pool.clone();
        let context = context.clone();
        Box::pin(async move {
            let results = self.insert_many(&context, schema, vec![values]).await?;
            Ok(results.into_iter().next().unwrap())
        })
    }

    fn insert_many(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        values: Vec<StorageRecord>,
    ) -> AdapterFuture<'_, Result<Vec<StorageRecord>, DatabaseError>> {
        let pool = self.pool.clone();
        let context = context.clone();
        Box::pin(async move {
            if values.is_empty() {
                return Ok(Vec::new());
            }
            let table = SqliteUtils::qualified_table_name(&context, schema.id);
            let perms_table = SqliteUtils::qualified_permissions_table_name(&context, schema.id);
            let attrs: Vec<_> = schema.persisted_attributes().collect();

            // SQLite RETURNING on multi-row inserts is supported since 3.35.
            // We insert all rows in one statement and collect the auto-generated
            // _id (ROWID) values in order.
            let mut builder = QueryBuilder::<Sqlite>::new(format!("INSERT INTO {table} ("));
            {
                // column list
                builder.push(format!(
                    "{}, {}, {}, {}",
                    COLUMN_ID,
                    database_core::COLUMN_CREATED_AT,
                    database_core::COLUMN_UPDATED_AT,
                    database_core::COLUMN_PERMISSIONS
                ));
                for a in &attrs {
                    builder.push(", ");
                    builder.push(SqliteUtils::quote_identifier(a.column));
                }
            }
            builder.push(") ");
            builder.push_values(values.iter(), |mut sep, record| {
                sep.push_bind(record.get(FIELD_ID).unwrap().as_str().unwrap().to_string());
                sep.push_bind(
                    record
                        .get(FIELD_CREATED_AT)
                        .unwrap()
                        .as_timestamp()
                        .unwrap()
                        .format(&time::format_description::well_known::Rfc3339)
                        .unwrap(),
                );
                sep.push_bind(
                    record
                        .get(FIELD_UPDATED_AT)
                        .unwrap()
                        .as_timestamp()
                        .unwrap()
                        .format(&time::format_description::well_known::Rfc3339)
                        .unwrap(),
                );
                sep.push_bind(
                    serde_json::to_string(
                        record
                            .get(FIELD_PERMISSIONS)
                            .unwrap()
                            .as_string_array()
                            .unwrap_or(&[]),
                    )
                    .unwrap(),
                );
                for a in &attrs {
                    let val = record.get(a.id).unwrap_or(&StorageValue::Null);
                    SqliteQuery::push_bind_value_separated(&mut sep, val);
                }
            });
            builder.push(format!(" RETURNING {COLUMN_SEQUENCE}"));

            let mut tx = pool
                .begin()
                .await
                .map_err(|e| DatabaseError::Other(e.to_string()))?;

            let rows = builder
                .build()
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| DatabaseError::Other(e.to_string()))?;

            // ── Collect results and build perms rows ──────────────────────
            let mut perms_rows: Vec<(i64, String, String)> = Vec::new(); // (seq, type, json_roles)
            let mut results = Vec::with_capacity(values.len());

            for (i, row) in rows.into_iter().enumerate() {
                let seq: i64 = row.get(0);
                let permissions = match values[i].get(FIELD_PERMISSIONS) {
                    Some(StorageValue::StringArray(v)) => v.clone(),
                    _ => Vec::new(),
                };

                if !permissions.is_empty() {
                    let grouped = database_core::utils::permission_rows(&permissions)?;
                    for (pt, roles) in grouped {
                        perms_rows.push((seq, pt, serde_json::to_string(&roles).unwrap()));
                    }
                }

                let mut record = values[i].clone();
                record.insert(FIELD_SEQUENCE.to_string(), StorageValue::Int(seq));
                results.push(record);
            }

            // ── Bulk-insert into the permissions table ────────────────────
            // SQLite doesn't support array bind parameters so we use individual
            // binds in a VALUES list.
            if !perms_rows.is_empty() {
                let mut pb = QueryBuilder::<Sqlite>::new(format!(
                    "INSERT INTO {perms_table} (document_id, permission_type, permissions) "
                ));
                pb.push_values(perms_rows.iter(), |mut sep, (seq, pt, roles_json)| {
                    sep.push_bind(*seq);
                    sep.push_bind(pt.clone());
                    sep.push_bind(roles_json.clone());
                });
                pb.build()
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| DatabaseError::Other(e.to_string()))?;
            }

            tx.commit()
                .await
                .map_err(|e| DatabaseError::Other(e.to_string()))?;

            Ok(results)
        })
    }

    fn get(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        id: &str,
    ) -> AdapterFuture<'_, Result<Option<StorageRecord>, DatabaseError>> {
        let pool = self.pool.clone();
        let context = context.clone();
        let id = id.to_string();
        Box::pin(async move {
            let table = SqliteUtils::qualified_table_name(&context, schema.id);
            let mut builder =
                QueryBuilder::<Sqlite>::new(format!("SELECT * FROM {table} AS main "));
            let mut has_where = false;
            SqliteQuery::push_condition_separator(&mut builder, &mut has_where);
            builder.push(format!("main.{} = ", COLUMN_ID));
            builder.push_bind(&id);
            SqliteQuery::push_document_action_condition(
                &mut builder,
                &context,
                schema,
                "main",
                PermissionEnum::Read,
                &mut has_where,
            )?;
            let row = builder
                .build()
                .fetch_optional(&pool)
                .await
                .map_err(|e| DatabaseError::Other(e.to_string()))?;
            match row {
                Some(r) => Ok(Some(SqliteUtils::row_to_record(&r, schema)?)),
                None => Ok(None),
            }
        })
    }

    fn update(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        id: &str,
        values: StorageRecord,
    ) -> AdapterFuture<'_, Result<Option<StorageRecord>, DatabaseError>> {
        let pool = self.pool.clone();
        let context = context.clone();
        let id = id.to_string();
        Box::pin(async move {
            let table = SqliteUtils::qualified_table_name(&context, schema.id);
            let mut tx = pool
                .begin()
                .await
                .map_err(|e| DatabaseError::Other(e.to_string()))?;
            let mut builder = QueryBuilder::<Sqlite>::new(format!("UPDATE {table} SET "));
            let mut first = true;

            let attributes: Vec<_> = schema
                .persisted_attributes()
                .filter(|a| values.contains_key(a.id))
                .collect();
            for a in &attributes {
                if !first {
                    builder.push(", ");
                }
                first = false;
                builder.push(format!("{} = ", SqliteUtils::quote_identifier(a.column)));
                SqliteQuery::push_bind_value(&mut builder, values.get(a.id).unwrap());
            }

            if let Some(val) = values.get(database_core::FIELD_UPDATED_AT) {
                if !first {
                    builder.push(", ");
                }
                first = false;
                builder.push(format!("{} = ", database_core::COLUMN_UPDATED_AT));
                SqliteQuery::push_bind_value(&mut builder, val);
            }

            let permissions = match values.get(database_core::FIELD_PERMISSIONS) {
                Some(StorageValue::StringArray(v)) => Some(v.clone()),
                Some(StorageValue::Null) | None => None,
                _ => {
                    return Err(DatabaseError::Other(
                        "permissions must be a string array".into(),
                    ));
                }
            };
            if let Some(perms) = &permissions {
                if !first {
                    builder.push(", ");
                }
                builder.push(format!("{} = ", database_core::COLUMN_PERMISSIONS));
                builder.push_bind(serde_json::to_string(perms).unwrap());
            }

            // SQLite UPDATE doesn't support table aliases in the main SET clause standardly like Postgres,
            // so we do subquery for document_action_condition or just use standard UPDATE WHERE.
            // Wait, SQLite UPDATE doesn't support aliases (AS main) in the UPDATE statement directly.
            // `UPDATE table SET ... WHERE id = ? AND EXISTS (...)`

            let mut has_where = false;
            SqliteQuery::push_condition_separator(&mut builder, &mut has_where);
            builder.push(format!("{} = ", COLUMN_ID));
            builder.push_bind(&id);
            SqliteQuery::push_document_action_condition(
                &mut builder,
                &context,
                schema,
                &table,
                PermissionEnum::Update,
                &mut has_where,
            )?;
            builder.push(format!(" RETURNING *"));

            let row = builder
                .build()
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| DatabaseError::Other(e.to_string()))?;
            let record = match row {
                Some(r) => SqliteUtils::row_to_record(&r, schema)?,
                None => return Ok(None),
            };

            if let Some(perms) = permissions {
                let seq = record
                    .get(database_core::FIELD_SEQUENCE)
                    .unwrap()
                    .as_int()
                    .unwrap();
                let perms_table =
                    SqliteUtils::qualified_permissions_table_name(&context, schema.id);
                sqlx::query(&format!("DELETE FROM {perms_table} WHERE document_id = ?"))
                    .bind(seq)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| DatabaseError::Other(e.to_string()))?;

                let grouped_perms = database_core::utils::permission_rows(&perms)?;
                for (pt, pv) in grouped_perms {
                    sqlx::query(&format!("INSERT INTO {perms_table} (document_id, permission_type, permissions) VALUES (?, ?, ?)"))
                        .bind(seq)
                        .bind(pt)
                        .bind(serde_json::to_string(&pv).unwrap())
                        .execute(&mut *tx).await.map_err(|e| DatabaseError::Other(e.to_string()))?;
                }
            }

            tx.commit()
                .await
                .map_err(|e| DatabaseError::Other(e.to_string()))?;
            Ok(Some(record))
        })
    }

    fn update_many(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        query: &QuerySpec,
        values: StorageRecord,
    ) -> AdapterFuture<'_, Result<u64, DatabaseError>> {
        let pool = self.pool.clone();
        let context = context.clone();
        let query = query.clone();
        Box::pin(async move {
            let table = SqliteUtils::qualified_table_name(&context, schema.id);
            let mut builder = QueryBuilder::<Sqlite>::new(format!("UPDATE {table} SET "));
            let mut first = true;

            let attributes: Vec<_> = schema
                .persisted_attributes()
                .filter(|a| values.contains_key(a.id))
                .collect();
            for a in &attributes {
                if !first {
                    builder.push(", ");
                }
                first = false;
                builder.push(format!("{} = ", SqliteUtils::quote_identifier(a.column)));
                SqliteQuery::push_bind_value(&mut builder, values.get(a.id).unwrap());
            }

            if let Some(val) = values.get(database_core::FIELD_UPDATED_AT) {
                if !first {
                    builder.push(", ");
                }
                builder.push(format!("{} = ", database_core::COLUMN_UPDATED_AT));
                SqliteQuery::push_bind_value(&mut builder, val);
            }

            if values.contains_key(database_core::FIELD_PERMISSIONS) {
                return Err(DatabaseError::Other(
                    "update_many does not support updating permissions".into(),
                ));
            }

            let mut has_where = false;
            SqliteQuery::push_document_action_condition(
                &mut builder,
                &context,
                schema,
                &table,
                PermissionEnum::Update,
                &mut has_where,
            )?;
            for f in query.filters() {
                SqliteQuery::push_condition_separator(&mut builder, &mut has_where);
                SqliteQuery::push_filter(&mut builder, schema, f)?;
            }

            let res = builder
                .build()
                .execute(&pool)
                .await
                .map_err(|e| DatabaseError::Other(e.to_string()))?;
            Ok(res.rows_affected() as u64)
        })
    }

    fn delete(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        id: &str,
    ) -> AdapterFuture<'_, Result<bool, DatabaseError>> {
        let pool = self.pool.clone();
        let context = context.clone();
        let id = id.to_string();
        Box::pin(async move {
            let table = SqliteUtils::qualified_table_name(&context, schema.id);
            let mut builder = QueryBuilder::<Sqlite>::new(format!("DELETE FROM {table} WHERE "));
            builder.push(format!("{} = ", COLUMN_ID));
            builder.push_bind(id);
            let mut has_where = true;
            SqliteQuery::push_document_action_condition(
                &mut builder,
                &context,
                schema,
                &table,
                PermissionEnum::Delete,
                &mut has_where,
            )?;
            let res = builder
                .build()
                .execute(&pool)
                .await
                .map_err(|e| DatabaseError::Other(e.to_string()))?;
            Ok(res.rows_affected() > 0)
        })
    }

    fn delete_many(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        query: &QuerySpec,
    ) -> AdapterFuture<'_, Result<u64, DatabaseError>> {
        let pool = self.pool.clone();
        let context = context.clone();
        let query = query.clone();
        Box::pin(async move {
            let table = SqliteUtils::qualified_table_name(&context, schema.id);
            let mut builder = QueryBuilder::<Sqlite>::new(format!("DELETE FROM {table}"));
            let mut has_where = false;
            SqliteQuery::push_document_action_condition(
                &mut builder,
                &context,
                schema,
                &table,
                PermissionEnum::Delete,
                &mut has_where,
            )?;
            for f in query.filters() {
                SqliteQuery::push_condition_separator(&mut builder, &mut has_where);
                SqliteQuery::push_filter(&mut builder, schema, f)?;
            }
            let res = builder
                .build()
                .execute(&pool)
                .await
                .map_err(|e| DatabaseError::Other(e.to_string()))?;
            Ok(res.rows_affected() as u64)
        })
    }

    fn find(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        query: &QuerySpec,
    ) -> AdapterFuture<'_, Result<Vec<StorageRecord>, DatabaseError>> {
        let pool = self.pool.clone();
        let context = context.clone();
        let query = query.clone();
        Box::pin(async move {
            let table = SqliteUtils::qualified_table_name(&context, schema.id);
            let mut builder = QueryBuilder::<Sqlite>::new(format!("SELECT * FROM {table} AS main"));
            let mut has_where = false;
            SqliteQuery::push_document_action_condition(
                &mut builder,
                &context,
                schema,
                "main",
                PermissionEnum::Read,
                &mut has_where,
            )?;
            for f in query.filters() {
                SqliteQuery::push_condition_separator(&mut builder, &mut has_where);
                SqliteQuery::push_filter(&mut builder, schema, f)?;
            }
            if !query.sorts().is_empty() {
                builder.push(" ORDER BY ");
                let mut first = true;
                for s in query.sorts() {
                    if !first {
                        builder.push(", ");
                    }
                    first = false;
                    let col = if let Some(a) = schema.attribute(s.field) {
                        SqliteUtils::quote_identifier(a.column)
                    } else {
                        s.field.to_string()
                    };
                    builder.push(col);
                    match s.direction {
                        SortDirection::Asc => {
                            builder.push(" ASC");
                        }
                        SortDirection::Desc => {
                            builder.push(" DESC");
                        }
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
            let rows = builder
                .build()
                .fetch_all(&pool)
                .await
                .map_err(|e| DatabaseError::Other(e.to_string()))?;
            let mut results = Vec::new();
            for r in rows {
                results.push(SqliteUtils::row_to_record(&r, schema)?);
            }
            Ok(results)
        })
    }

    fn count(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        query: &QuerySpec,
    ) -> AdapterFuture<'_, Result<u64, DatabaseError>> {
        let pool = self.pool.clone();
        let context = context.clone();
        let query = query.clone();
        Box::pin(async move {
            let table = SqliteUtils::qualified_table_name(&context, schema.id);
            let mut builder =
                QueryBuilder::<Sqlite>::new(format!("SELECT COUNT(*) FROM {table} AS main"));
            let mut has_where = false;
            SqliteQuery::push_document_action_condition(
                &mut builder,
                &context,
                schema,
                "main",
                PermissionEnum::Read,
                &mut has_where,
            )?;
            for f in query.filters() {
                SqliteQuery::push_condition_separator(&mut builder, &mut has_where);
                SqliteQuery::push_filter(&mut builder, schema, f)?;
            }
            let row = builder
                .build()
                .fetch_one(&pool)
                .await
                .map_err(|e| DatabaseError::Other(e.to_string()))?;
            let count: i64 = row.get(0);
            Ok(count as u64)
        })
    }
}
