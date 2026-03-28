use crate::error::{map_sqlite_error, map_sqlite_error_with_context};
use database_core::errors::DatabaseError;
use database_core::query::{QuerySpec, SortDirection};
use database_core::traits::storage::{
    AdapterFuture, JoinedStorageRecord, PopulatedStorageRow, StorageAdapter, StoragePopulate,
    StorageRecord, StorageRelation, StorageValue,
};
use database_core::utils::PermissionEnum;
use database_core::{
    COLUMN_CREATED_AT, COLUMN_ID, COLUMN_SEQUENCE, COLUMN_UPDATED_AT, CollectionSchema, Context,
    FIELD_CREATED_AT, FIELD_ID, FIELD_PERMISSIONS, FIELD_SEQUENCE, FIELD_UPDATED_AT,
    RelationshipKind,
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
                .map_err(|error| map_sqlite_error_with_context("sqlite ping failed", error))
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
            sqlx::query(&sql).execute(&pool).await.map_err(|error| {
                map_sqlite_error_with_context("failed to create collection table", error)
            })?;

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
                .map_err(|error| {
                    map_sqlite_error_with_context("failed to create permissions table", error)
                })?;

            let internal_indexes = [
                format!(
                    "CREATE INDEX IF NOT EXISTS {} ON {table} ({})",
                    SqliteUtils::quote_identifier(&format!("{}_created_at", schema.id)),
                    SqliteUtils::quote_identifier(COLUMN_CREATED_AT),
                ),
                format!(
                    "CREATE INDEX IF NOT EXISTS {} ON {table} ({})",
                    SqliteUtils::quote_identifier(&format!("{}_updated_at", schema.id)),
                    SqliteUtils::quote_identifier(COLUMN_UPDATED_AT),
                ),
                format!(
                    "CREATE INDEX IF NOT EXISTS {} ON {perms_table} (permission_type)",
                    SqliteUtils::quote_identifier(&format!(
                        "{}_perms_permission_type_idx",
                        schema.id
                    )),
                ),
                format!(
                    "CREATE INDEX IF NOT EXISTS {} ON {perms_table} (document_id)",
                    SqliteUtils::quote_identifier(&format!("{}_perms_document_id_idx", schema.id)),
                ),
            ];
            for statement in internal_indexes {
                sqlx::query(&statement)
                    .execute(&pool)
                    .await
                    .map_err(|error| {
                        map_sqlite_error_with_context(
                            format!("failed to create internal index with statement `{statement}`"),
                            error,
                        )
                    })?;
            }

            for index in schema.indexes {
                let index_name = SqliteUtils::quote_identifier(index.id);
                let mut col_parts = Vec::with_capacity(index.attributes.len());
                for (pos, attr_id) in index.attributes.iter().enumerate() {
                    let column = SqliteUtils::qualified_column_for_field(schema, attr_id, None)?;
                    let direction = index
                        .orders
                        .get(pos)
                        .map(|order| match order {
                            database_core::Order::Asc => " ASC",
                            database_core::Order::Desc => " DESC",
                            database_core::Order::None => "",
                        })
                        .unwrap_or("");
                    col_parts.push(format!("{column}{direction}"));
                }
                let columns = col_parts.join(", ");
                let statement = match index.kind {
                    database_core::IndexKind::Key | database_core::IndexKind::FullText => {
                        format!("CREATE INDEX IF NOT EXISTS {index_name} ON {table} ({columns})")
                    }
                    database_core::IndexKind::Unique => {
                        format!(
                            "CREATE UNIQUE INDEX IF NOT EXISTS {index_name} ON {table} ({columns})"
                        )
                    }
                    database_core::IndexKind::Spatial => {
                        return Err(DatabaseError::Other(format!(
                            "collection '{}': sqlite driver does not support spatial indexes",
                            schema.id
                        )));
                    }
                };
                sqlx::query(&statement)
                    .execute(&pool)
                    .await
                    .map_err(|error| {
                        map_sqlite_error_with_context(
                            format!("failed to create schema index with statement `{statement}`"),
                            error,
                        )
                    })?;
            }

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

            let mut tx = pool.begin().await.map_err(|error| {
                map_sqlite_error_with_context("failed to start insert transaction", error)
            })?;

            let rows =
                builder.build().fetch_all(&mut *tx).await.map_err(|error| {
                    map_sqlite_error_with_context("failed to insert rows", error)
                })?;

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
                pb.build().execute(&mut *tx).await.map_err(|error| {
                    map_sqlite_error_with_context("failed to insert permission rows", error)
                })?;
            }

            tx.commit().await.map_err(|error| {
                map_sqlite_error_with_context("failed to commit insert transaction", error)
            })?;

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
                .map_err(map_sqlite_error)?;
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
            let mut tx = pool.begin().await.map_err(|error| {
                map_sqlite_error_with_context("failed to start update transaction", error)
            })?;
            let mut builder = QueryBuilder::<Sqlite>::new(format!("UPDATE {table} AS main SET "));
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

            let mut has_where = false;
            SqliteQuery::push_condition_separator(&mut builder, &mut has_where);
            builder.push(format!(
                "main.{} = ",
                SqliteUtils::quote_identifier(COLUMN_ID)
            ));
            builder.push_bind(&id);
            SqliteQuery::push_document_action_condition(
                &mut builder,
                &context,
                schema,
                "main",
                PermissionEnum::Update,
                &mut has_where,
            )?;
            builder.push(format!(" RETURNING *"));

            let row = builder
                .build()
                .fetch_optional(&mut *tx)
                .await
                .map_err(|error| map_sqlite_error_with_context("failed to update row", error))?;
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
                    .map_err(|error| {
                        map_sqlite_error_with_context("failed to replace permission rows", error)
                    })?;

                let grouped_perms = database_core::utils::permission_rows(&perms)?;
                for (pt, pv) in grouped_perms {
                    sqlx::query(&format!("INSERT INTO {perms_table} (document_id, permission_type, permissions) VALUES (?, ?, ?)"))
                        .bind(seq)
                        .bind(pt)
                        .bind(serde_json::to_string(&pv).unwrap())
                        .execute(&mut *tx)
                        .await
                        .map_err(|error| {
                            map_sqlite_error_with_context(
                                "failed to insert replacement permission row",
                                error,
                            )
                        })?;
                }
            }

            tx.commit().await.map_err(|error| {
                map_sqlite_error_with_context("failed to commit update transaction", error)
            })?;
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
            let mut builder = QueryBuilder::<Sqlite>::new(format!("UPDATE {table} AS main SET "));
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
                "main",
                PermissionEnum::Update,
                &mut has_where,
            )?;
            for f in query.filters() {
                SqliteQuery::push_condition_separator(&mut builder, &mut has_where);
                SqliteQuery::push_filter_for_alias(&mut builder, schema, f, Some("main"))?;
            }

            let res =
                builder.build().execute(&pool).await.map_err(|error| {
                    map_sqlite_error_with_context("failed to update rows", error)
                })?;
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
            let mut builder =
                QueryBuilder::<Sqlite>::new(format!("DELETE FROM {table} AS main WHERE "));
            builder.push(format!(
                "main.{} = ",
                SqliteUtils::quote_identifier(COLUMN_ID)
            ));
            builder.push_bind(id);
            let mut has_where = true;
            SqliteQuery::push_document_action_condition(
                &mut builder,
                &context,
                schema,
                "main",
                PermissionEnum::Delete,
                &mut has_where,
            )?;
            let res =
                builder.build().execute(&pool).await.map_err(|error| {
                    map_sqlite_error_with_context("failed to delete row", error)
                })?;
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
            let mut builder = QueryBuilder::<Sqlite>::new(format!("DELETE FROM {table} AS main"));
            let mut has_where = false;
            SqliteQuery::push_document_action_condition(
                &mut builder,
                &context,
                schema,
                "main",
                PermissionEnum::Delete,
                &mut has_where,
            )?;
            for f in query.filters() {
                SqliteQuery::push_condition_separator(&mut builder, &mut has_where);
                SqliteQuery::push_filter_for_alias(&mut builder, schema, f, Some("main"))?;
            }
            let res =
                builder.build().execute(&pool).await.map_err(|error| {
                    map_sqlite_error_with_context("failed to delete rows", error)
                })?;
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
            let rows =
                builder.build().fetch_all(&pool).await.map_err(|error| {
                    map_sqlite_error_with_context("failed to query rows", error)
                })?;
            let mut results = Vec::new();
            for r in rows {
                results.push(SqliteUtils::row_to_record(&r, schema)?);
            }
            Ok(results)
        })
    }

    fn find_related(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        related_schema: &'static CollectionSchema,
        query: &QuerySpec,
        relation: StorageRelation,
    ) -> AdapterFuture<'_, Result<Option<Vec<JoinedStorageRecord>>, DatabaseError>> {
        let pool = self.pool.clone();
        let context = context.clone();
        let query = query.clone();
        Box::pin(async move {
            let base_table = SqliteUtils::qualified_table_name(&context, schema.id);
            let related_table = SqliteUtils::qualified_table_name(&context, related_schema.id);
            let base_select = SqliteUtils::select_columns_for_alias(schema, "main", "base");
            let related_select =
                SqliteUtils::select_columns_for_alias(related_schema, "rel", "rel");
            let use_base_subquery = matches!(
                relation.kind,
                RelationshipKind::OneToMany | RelationshipKind::ManyToMany
            );

            let mut builder = QueryBuilder::<Sqlite>::new(format!(
                "SELECT {base_select}, {related_select} FROM "
            ));
            if use_base_subquery {
                builder.push("(SELECT ");
                builder.push(SqliteUtils::select_columns(schema));
                builder.push(format!(" FROM {base_table} AS main"));
                let mut has_where = false;
                SqliteQuery::push_document_action_condition(
                    &mut builder,
                    &context,
                    schema,
                    "main",
                    PermissionEnum::Read,
                    &mut has_where,
                )?;
                for filter in query.filters() {
                    SqliteQuery::push_condition_separator(&mut builder, &mut has_where);
                    SqliteQuery::push_filter_for_alias(&mut builder, schema, filter, Some("main"))?;
                }
                if !query.sorts().is_empty() {
                    builder.push(" ORDER BY ");
                    let mut first = true;
                    for sort in query.sorts() {
                        if !first {
                            builder.push(", ");
                        }
                        first = false;
                        let column = SqliteUtils::qualified_column_for_field(
                            schema,
                            sort.field,
                            Some("main"),
                        )?;
                        builder.push(column);
                        match sort.direction {
                            SortDirection::Asc => builder.push(" ASC"),
                            SortDirection::Desc => builder.push(" DESC"),
                        };
                    }
                }
                if let Some(limit) = query.limit_value() {
                    builder.push(" LIMIT ");
                    builder.push_bind(limit as i64);
                }
                if let Some(offset) = query.offset_value() {
                    builder.push(" OFFSET ");
                    builder.push_bind(offset as i64);
                }
                builder.push(") AS main ");
            } else {
                builder.push(format!("{base_table} AS main "));
            }

            match relation.kind {
                RelationshipKind::ManyToOne
                | RelationshipKind::OneToOne
                | RelationshipKind::OneToMany => {
                    let base_join_column = SqliteUtils::qualified_column_for_field(
                        schema,
                        relation.local_field,
                        Some("main"),
                    )?;
                    let related_join_column = SqliteUtils::qualified_column_for_field(
                        related_schema,
                        relation.remote_field,
                        Some("rel"),
                    )?;
                    builder.push(format!(
                        "LEFT JOIN {related_table} AS rel ON {base_join_column} = {related_join_column} AND "
                    ));
                    SqliteQuery::push_document_action_expression(
                        &mut builder,
                        &context,
                        related_schema,
                        "rel",
                        PermissionEnum::Read,
                    )?;
                }
                RelationshipKind::ManyToMany => {
                    let through = relation.through.ok_or_else(|| {
                        DatabaseError::Other(
                            "many-to-many relation is missing through metadata".into(),
                        )
                    })?;
                    let through_table =
                        SqliteUtils::qualified_table_name(&context, through.schema.id);
                    let base_join_column = SqliteUtils::qualified_column_for_field(
                        schema,
                        relation.local_field,
                        Some("main"),
                    )?;
                    let through_local_column = SqliteUtils::qualified_column_for_field(
                        through.schema,
                        through.local_field,
                        Some("jt"),
                    )?;
                    let through_remote_column = SqliteUtils::qualified_column_for_field(
                        through.schema,
                        through.remote_field,
                        Some("jt"),
                    )?;
                    let related_join_column = SqliteUtils::qualified_column_for_field(
                        related_schema,
                        relation.remote_field,
                        Some("rel"),
                    )?;
                    builder.push(format!(
                        "LEFT JOIN {through_table} AS jt ON {base_join_column} = {through_local_column} AND "
                    ));
                    SqliteQuery::push_document_action_expression(
                        &mut builder,
                        &context,
                        through.schema,
                        "jt",
                        PermissionEnum::Read,
                    )?;
                    builder.push(format!(
                        " LEFT JOIN {related_table} AS rel ON {through_remote_column} = {related_join_column} AND "
                    ));
                    SqliteQuery::push_document_action_expression(
                        &mut builder,
                        &context,
                        related_schema,
                        "rel",
                        PermissionEnum::Read,
                    )?;
                }
            }

            if !use_base_subquery {
                let mut has_where = false;
                SqliteQuery::push_document_action_condition(
                    &mut builder,
                    &context,
                    schema,
                    "main",
                    PermissionEnum::Read,
                    &mut has_where,
                )?;
                for filter in query.filters() {
                    SqliteQuery::push_condition_separator(&mut builder, &mut has_where);
                    SqliteQuery::push_filter_for_alias(&mut builder, schema, filter, Some("main"))?;
                }
            }

            if !query.sorts().is_empty() {
                builder.push(" ORDER BY ");
                let mut first = true;
                for sort in query.sorts() {
                    if !first {
                        builder.push(", ");
                    }
                    first = false;
                    let column =
                        SqliteUtils::qualified_column_for_field(schema, sort.field, Some("main"))?;
                    builder.push(column);
                    match sort.direction {
                        SortDirection::Asc => builder.push(" ASC"),
                        SortDirection::Desc => builder.push(" DESC"),
                    };
                }
                if use_base_subquery {
                    builder.push(", ");
                    builder.push(format!(
                        "rel.{} ASC",
                        SqliteUtils::quote_identifier(COLUMN_SEQUENCE)
                    ));
                }
            } else if use_base_subquery {
                builder.push(format!(
                    " ORDER BY main.{}, rel.{} ASC",
                    SqliteUtils::quote_identifier(COLUMN_SEQUENCE),
                    SqliteUtils::quote_identifier(COLUMN_SEQUENCE)
                ));
            }

            if !use_base_subquery {
                if let Some(limit) = query.limit_value() {
                    builder.push(" LIMIT ");
                    builder.push_bind(limit as i64);
                }
                if let Some(offset) = query.offset_value() {
                    builder.push(" OFFSET ");
                    builder.push_bind(offset as i64);
                }
            }

            let rows = builder.build().fetch_all(&pool).await.map_err(|error| {
                map_sqlite_error_with_context("failed to query related rows", error)
            })?;

            let mut joined = Vec::with_capacity(rows.len());
            for row in rows {
                let Some(base) = SqliteUtils::row_to_record_prefixed(&row, schema, Some("base"))?
                else {
                    continue;
                };
                let related =
                    SqliteUtils::row_to_record_prefixed(&row, related_schema, Some("rel"))?;
                joined.push(JoinedStorageRecord { base, related });
            }

            Ok(Some(joined))
        })
    }

    fn find_populated(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        query: &QuerySpec,
        populates: Vec<StoragePopulate>,
    ) -> AdapterFuture<'_, Result<Option<Vec<PopulatedStorageRow>>, DatabaseError>> {
        let pool = self.pool.clone();
        let context = context.clone();
        let query = query.clone();
        Box::pin(async move {
            if populates.is_empty() {
                return Ok(None);
            }

            let base_table = SqliteUtils::qualified_table_name(&context, schema.id);
            let mut select_parts = vec![SqliteUtils::select_columns_for_alias(
                schema, "main", "base",
            )];
            for (index, populate) in populates.iter().enumerate() {
                let alias = format!("rel_{index}");
                select_parts.push(SqliteUtils::select_columns_for_alias(
                    populate.schema,
                    &alias,
                    &alias,
                ));
            }

            let use_base_subquery = populates.iter().any(|populate| {
                matches!(
                    populate.relation.kind,
                    RelationshipKind::OneToMany | RelationshipKind::ManyToMany
                )
            });

            let mut builder =
                QueryBuilder::<Sqlite>::new(format!("SELECT {} FROM ", select_parts.join(", ")));

            if use_base_subquery {
                builder.push("(SELECT ");
                builder.push(SqliteUtils::select_columns(schema));
                builder.push(format!(" FROM {base_table} AS main"));
                let mut has_where = false;
                SqliteQuery::push_document_action_condition(
                    &mut builder,
                    &context,
                    schema,
                    "main",
                    PermissionEnum::Read,
                    &mut has_where,
                )?;
                for filter in query.filters() {
                    SqliteQuery::push_condition_separator(&mut builder, &mut has_where);
                    SqliteQuery::push_filter_for_alias(&mut builder, schema, filter, Some("main"))?;
                }
                if !query.sorts().is_empty() {
                    builder.push(" ORDER BY ");
                    let mut first = true;
                    for sort in query.sorts() {
                        if !first {
                            builder.push(", ");
                        }
                        first = false;
                        let column = SqliteUtils::qualified_column_for_field(
                            schema,
                            sort.field,
                            Some("main"),
                        )?;
                        builder.push(column);
                        match sort.direction {
                            SortDirection::Asc => builder.push(" ASC"),
                            SortDirection::Desc => builder.push(" DESC"),
                        };
                    }
                }
                if let Some(limit) = query.limit_value() {
                    builder.push(" LIMIT ");
                    builder.push_bind(limit as i64);
                }
                if let Some(offset) = query.offset_value() {
                    builder.push(" OFFSET ");
                    builder.push_bind(offset as i64);
                }
                builder.push(") AS main ");
            } else {
                builder.push(format!("{base_table} AS main "));
            }

            for (index, populate) in populates.iter().enumerate() {
                let relation = &populate.relation;
                let related_table = SqliteUtils::qualified_table_name(&context, populate.schema.id);
                let relation_alias = format!("rel_{index}");

                match relation.kind {
                    RelationshipKind::ManyToOne
                    | RelationshipKind::OneToOne
                    | RelationshipKind::OneToMany => {
                        let base_join_column = SqliteUtils::qualified_column_for_field(
                            schema,
                            relation.local_field,
                            Some("main"),
                        )?;
                        let related_join_column = SqliteUtils::qualified_column_for_field(
                            populate.schema,
                            relation.remote_field,
                            Some(&relation_alias),
                        )?;
                        builder.push(format!(
                            "LEFT JOIN {related_table} AS {relation_alias} ON {base_join_column} = {related_join_column} AND "
                        ));
                        SqliteQuery::push_document_action_expression(
                            &mut builder,
                            &context,
                            populate.schema,
                            &relation_alias,
                            PermissionEnum::Read,
                        )?;
                    }
                    RelationshipKind::ManyToMany => {
                        let through = relation.through.as_ref().ok_or_else(|| {
                            DatabaseError::Other(
                                "many-to-many relation is missing through metadata".into(),
                            )
                        })?;
                        let through_alias = format!("jt_{index}");
                        let through_table =
                            SqliteUtils::qualified_table_name(&context, through.schema.id);
                        let base_join_column = SqliteUtils::qualified_column_for_field(
                            schema,
                            relation.local_field,
                            Some("main"),
                        )?;
                        let through_local_column = SqliteUtils::qualified_column_for_field(
                            through.schema,
                            through.local_field,
                            Some(&through_alias),
                        )?;
                        let through_remote_column = SqliteUtils::qualified_column_for_field(
                            through.schema,
                            through.remote_field,
                            Some(&through_alias),
                        )?;
                        let related_join_column = SqliteUtils::qualified_column_for_field(
                            populate.schema,
                            relation.remote_field,
                            Some(&relation_alias),
                        )?;
                        builder.push(format!(
                            "LEFT JOIN {through_table} AS {through_alias} ON {base_join_column} = {through_local_column} AND "
                        ));
                        SqliteQuery::push_document_action_expression(
                            &mut builder,
                            &context,
                            through.schema,
                            &through_alias,
                            PermissionEnum::Read,
                        )?;
                        builder.push(format!(
                            " LEFT JOIN {related_table} AS {relation_alias} ON {through_remote_column} = {related_join_column} AND "
                        ));
                        SqliteQuery::push_document_action_expression(
                            &mut builder,
                            &context,
                            populate.schema,
                            &relation_alias,
                            PermissionEnum::Read,
                        )?;
                    }
                }
                builder.push(" ");
            }

            if !use_base_subquery {
                let mut has_where = false;
                SqliteQuery::push_document_action_condition(
                    &mut builder,
                    &context,
                    schema,
                    "main",
                    PermissionEnum::Read,
                    &mut has_where,
                )?;
                for filter in query.filters() {
                    SqliteQuery::push_condition_separator(&mut builder, &mut has_where);
                    SqliteQuery::push_filter_for_alias(&mut builder, schema, filter, Some("main"))?;
                }
            }

            if !query.sorts().is_empty() {
                builder.push(" ORDER BY ");
                let mut first = true;
                for sort in query.sorts() {
                    if !first {
                        builder.push(", ");
                    }
                    first = false;
                    let column =
                        SqliteUtils::qualified_column_for_field(schema, sort.field, Some("main"))?;
                    builder.push(column);
                    match sort.direction {
                        SortDirection::Asc => builder.push(" ASC"),
                        SortDirection::Desc => builder.push(" DESC"),
                    };
                }
                if use_base_subquery {
                    for index in 0..populates.len() {
                        builder.push(", ");
                        builder.push(format!(
                            "rel_{index}.{} ASC",
                            SqliteUtils::quote_identifier(COLUMN_SEQUENCE)
                        ));
                    }
                }
            } else if use_base_subquery {
                builder.push(format!(
                    " ORDER BY main.{} ASC",
                    SqliteUtils::quote_identifier(COLUMN_SEQUENCE)
                ));
                for index in 0..populates.len() {
                    builder.push(", ");
                    builder.push(format!(
                        "rel_{index}.{} ASC",
                        SqliteUtils::quote_identifier(COLUMN_SEQUENCE)
                    ));
                }
            }

            if !use_base_subquery {
                if let Some(limit) = query.limit_value() {
                    builder.push(" LIMIT ");
                    builder.push_bind(limit as i64);
                }
                if let Some(offset) = query.offset_value() {
                    builder.push(" OFFSET ");
                    builder.push_bind(offset as i64);
                }
            }

            let rows = builder.build().fetch_all(&pool).await.map_err(|error| {
                map_sqlite_error_with_context("failed to query populated rows", error)
            })?;

            let mut populated_rows = Vec::with_capacity(rows.len());
            for row in rows {
                let Some(base) = SqliteUtils::row_to_record_prefixed(&row, schema, Some("base"))?
                else {
                    continue;
                };
                let mut related = std::collections::BTreeMap::new();
                for (index, populate) in populates.iter().enumerate() {
                    let prefix = format!("rel_{index}");
                    let record =
                        SqliteUtils::row_to_record_prefixed(&row, populate.schema, Some(&prefix))?;
                    related.insert(populate.name.to_string(), record);
                }
                populated_rows.push(PopulatedStorageRow { base, related });
            }

            Ok(Some(populated_rows))
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
            let row =
                builder.build().fetch_one(&pool).await.map_err(|error| {
                    map_sqlite_error_with_context("failed to count rows", error)
                })?;
            let count: i64 = row.get(0);
            Ok(count as u64)
        })
    }
}
