use crate::query::PostgresQuery;
use crate::utils::PostgresUtils;
use database_core::errors::DatabaseError;
use database_core::query::QuerySpec;
use database_core::traits::storage::{
    AdapterFuture, JoinedStorageRecord, PopulatedStorageRow, StorageAdapter, StoragePopulate,
    StorageRecord, StorageRelation, StorageValue,
};
use database_core::utils::PermissionEnum;
use database_core::{
    AttributeKind, CollectionSchema, Context, FIELD_CREATED_AT, FIELD_ID, FIELD_PERMISSIONS,
    FIELD_SEQUENCE, FIELD_UPDATED_AT, RelationshipKind,
};
use sqlx::{Pool, Postgres, QueryBuilder, Row};

#[derive(Clone)]
pub struct PostgresAdapter {
    pool: Pool<Postgres>,
}

impl PostgresAdapter {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    pub fn quote_identifier(identifier: &str) -> Result<String, DatabaseError> {
        PostgresUtils::quote_identifier(identifier)
    }

    pub fn sql_type(
        kind: database_core::AttributeKind,
        array: bool,
        length: Option<usize>,
        custom_type: Option<&str>,
    ) -> String {
        PostgresUtils::sql_type(kind, array, length, custom_type)
    }

    pub fn qualified_table_name(
        context: &Context,
        collection: &str,
    ) -> Result<String, DatabaseError> {
        PostgresUtils::qualified_table_name(context, collection)
    }

    pub fn qualified_permissions_table_name(
        context: &Context,
        collection: &str,
    ) -> Result<String, DatabaseError> {
        PostgresUtils::qualified_permissions_table_name(context, collection)
    }

    pub fn quoted_system_column(identifier: &str) -> Result<String, DatabaseError> {
        PostgresUtils::quote_identifier(identifier)
    }

    pub fn get_pool(&self) -> &Pool<Postgres> {
        &self.pool
    }
}

impl StorageAdapter for PostgresAdapter {
    fn enforces_document_filtering(&self, action: PermissionEnum) -> bool {
        matches!(
            action,
            PermissionEnum::Read | PermissionEnum::Update | PermissionEnum::Delete
        )
    }

    fn ping(&self, _context: &Context) -> AdapterFuture<'_, Result<(), DatabaseError>> {
        let pool = self.pool.clone();
        Box::pin(async move {
            sqlx::query("SELECT 1")
                .execute(&pool)
                .await
                .map(|_| ())
                .map_err(|error| DatabaseError::Other(format!("postgres ping failed: {error}")))
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
            let schema_sql = format!(
                "CREATE SCHEMA IF NOT EXISTS {}",
                PostgresUtils::quote_identifier(context.schema())?
            );
            let table = PostgresUtils::qualified_table_name(&context, schema.id)?;
            let seq_col = PostgresUtils::quote_identifier(database_core::COLUMN_SEQUENCE)?;
            let id_col = PostgresUtils::quote_identifier(database_core::COLUMN_ID)?;
            let created_col = PostgresUtils::quote_identifier(database_core::COLUMN_CREATED_AT)?;
            let updated_col = PostgresUtils::quote_identifier(database_core::COLUMN_UPDATED_AT)?;
            let perms_col = PostgresUtils::quote_identifier(database_core::COLUMN_PERMISSIONS)?;

            let mut cols = vec![
                format!("{seq_col} BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY"),
                format!("{id_col} VARCHAR(255) NOT NULL"),
                format!("{created_col} TIMESTAMPTZ NOT NULL DEFAULT NOW()"),
                format!("{updated_col} TIMESTAMPTZ NOT NULL DEFAULT NOW()"),
                format!("{perms_col} TEXT[] NOT NULL DEFAULT '{{}}'"),
            ];
            let mut enum_statements = Vec::new();
            for attr in schema.persisted_attributes() {
                let custom_type = if attr.kind == AttributeKind::Enum {
                    let type_name = PostgresUtils::enum_type_name(schema.id, attr.id);
                    let schema_name = PostgresUtils::quote_identifier(context.schema())?;
                    let full_type_name = format!("{}.{}", schema_name, type_name);
                    if let Some(elements) = attr.elements {
                        let elements_str = elements
                            .iter()
                            .map(|e| format!("'{}'", e.replace('\'', "''")))
                            .collect::<Vec<_>>()
                            .join(", ");
                        enum_statements.push(format!(
                            "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_type t JOIN pg_namespace n ON t.typnamespace = n.oid WHERE t.typname = '{}' AND n.nspname = '{}') THEN CREATE TYPE {} AS ENUM ({}); END IF; END $$;",
                            type_name, context.schema(), full_type_name, elements_str
                        ));
                    }
                    Some(full_type_name)
                } else {
                    None
                };

                let sql_type = PostgresUtils::sql_type(
                    attr.kind,
                    attr.array,
                    attr.length,
                    custom_type.as_deref(),
                );
                let not_null = if attr.required { " NOT NULL" } else { "" };
                let default_clause = PostgresUtils::sql_default(attr.default);
                cols.push(format!(
                    "{} {}{}{}",
                    PostgresUtils::quote_identifier(attr.column)?,
                    sql_type,
                    not_null,
                    default_clause,
                ));
            }
            let table_sql = format!(
                "CREATE TABLE IF NOT EXISTS {table} ({}, PRIMARY KEY ({seq_col}))",
                cols.join(", "),
            );

            let perms_table = PostgresUtils::qualified_permissions_table_name(&context, schema.id)?;
            let perms_sql = format!(
                "CREATE TABLE IF NOT EXISTS {perms_table} (\
                    document_id BIGINT NOT NULL, \
                    permission_type TEXT NOT NULL, \
                    permissions TEXT[] NOT NULL DEFAULT '{{}}', \
                    PRIMARY KEY (document_id, permission_type), \
                    FOREIGN KEY (document_id) REFERENCES {table}({seq_col}) ON DELETE CASCADE\
                )"
            );

            // ── System indexes ────────────────────────────────────────────
            let cid = schema.id;
            let uid_idx = format!(
                "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {table} ({id_col})",
                PostgresUtils::quote_identifier(&format!("{cid}_uid"))?
            );
            let created_idx = format!(
                "CREATE INDEX IF NOT EXISTS {} ON {table} ({created_col})",
                PostgresUtils::quote_identifier(&format!("{cid}_created_at"))?
            );
            let updated_idx = format!(
                "CREATE INDEX IF NOT EXISTS {} ON {table} ({updated_col})",
                PostgresUtils::quote_identifier(&format!("{cid}_updated_at"))?
            );
            let perms_gin_idx = format!(
                "CREATE INDEX IF NOT EXISTS {} ON {table} USING GIN ({perms_col})",
                PostgresUtils::quote_identifier(&format!("{cid}_permissions_gin_idx"))?
            );
            let perms_table_gin_idx = format!(
                "CREATE INDEX IF NOT EXISTS {} ON {perms_table} USING GIN (permissions)",
                PostgresUtils::quote_identifier(&format!("{cid}_perms_permissions_gin_idx"))?
            );
            let perms_type_idx = format!(
                "CREATE INDEX IF NOT EXISTS {} ON {perms_table} (permission_type)",
                PostgresUtils::quote_identifier(&format!("{cid}_perms_permission_type_idx"))?
            );
            let perms_doc_idx = format!(
                "CREATE INDEX IF NOT EXISTS {} ON {perms_table} (document_id)",
                PostgresUtils::quote_identifier(&format!("{cid}_perms_document_id_idx"))?
            );

            // ── Schema-declared indexes ───────────────────────────────────
            let mut schema_indexes: Vec<String> = Vec::new();
            for index in schema.indexes {
                let idx_name = PostgresUtils::quote_identifier(index.id)?;

                // Build column list with optional sort directions
                let mut col_parts: Vec<String> = Vec::new();
                for (pos, attr_id) in index.attributes.iter().enumerate() {
                    let col = PostgresUtils::column_for_field(schema, attr_id)?;
                    let dir = index
                        .orders
                        .get(pos)
                        .map(|o| match o {
                            database_core::Order::Asc => " ASC",
                            database_core::Order::Desc => " DESC",
                            database_core::Order::None => "",
                        })
                        .unwrap_or("");
                    col_parts.push(format!("{col}{dir}"));
                }
                let col_list = col_parts.join(", ");

                let stmt = match index.kind {
                    database_core::IndexKind::Key => {
                        format!("CREATE INDEX IF NOT EXISTS {idx_name} ON {table} ({col_list})")
                    }
                    database_core::IndexKind::Unique => {
                        format!(
                            "CREATE UNIQUE INDEX IF NOT EXISTS {idx_name} ON {table} ({col_list})"
                        )
                    }
                    database_core::IndexKind::FullText => {
                        // Build tsvector expression over all listed columns
                        let ts_expr = index
                            .attributes
                            .iter()
                            .map(|attr_id| {
                                PostgresUtils::column_for_field(schema, attr_id)
                                    .map(|col| format!("COALESCE({col}::text, '')"))
                            })
                            .collect::<Result<Vec<_>, _>>()?
                            .join(" || ' ' || ");
                        format!(
                            "CREATE INDEX IF NOT EXISTS {idx_name} ON {table} \
                             USING GIN (to_tsvector('simple', {ts_expr}))"
                        )
                    }
                    database_core::IndexKind::Spatial => {
                        return Err(DatabaseError::Other(format!(
                            "collection '{}': postgres driver does not support spatial indexes",
                            schema.id
                        )));
                    }
                };
                schema_indexes.push(stmt);
            }

            // ── Execute everything in one transaction ─────────────────────
            let mut tx = pool
                .begin()
                .await
                .map_err(|e| DatabaseError::Other(e.to_string()))?;

            for stmt in &enum_statements {
                sqlx::query(stmt)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| DatabaseError::Other(format!("Enum creation failed ({stmt}): {e}")))?;
            }

            let ddl_statements: &[&str] = &[
                schema_sql.as_str(),
                table_sql.as_str(),
                perms_sql.as_str(),
                uid_idx.as_str(),
                created_idx.as_str(),
                updated_idx.as_str(),
                perms_gin_idx.as_str(),
                perms_table_gin_idx.as_str(),
                perms_type_idx.as_str(),
                perms_doc_idx.as_str(),
            ];
            for stmt in ddl_statements {
                sqlx::query(stmt)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| DatabaseError::Other(format!("DDL failed ({stmt}): {e}")))?;
            }
            for stmt in &schema_indexes {
                sqlx::query(stmt)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| DatabaseError::Other(format!("index DDL failed ({stmt}): {e}")))?;
            }

            tx.commit()
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

            let table = PostgresUtils::qualified_table_name(&context, schema.id)?;
            let perms_table = PostgresUtils::qualified_permissions_table_name(&context, schema.id)?;
            let attributes: Vec<_> = schema.persisted_attributes().collect();

            // ── Build the bulk INSERT for the main table ──────────────────
            let mut builder = QueryBuilder::<Postgres>::new(format!("INSERT INTO {table} ("));
            {
                let mut sep = builder.separated(", ");
                sep.push(PostgresUtils::quote_identifier(database_core::COLUMN_ID)?);
                sep.push(PostgresUtils::quote_identifier(
                    database_core::COLUMN_CREATED_AT,
                )?);
                sep.push(PostgresUtils::quote_identifier(
                    database_core::COLUMN_UPDATED_AT,
                )?);
                sep.push(PostgresUtils::quote_identifier(
                    database_core::COLUMN_PERMISSIONS,
                )?);
                for a in &attributes {
                    sep.push(PostgresUtils::quote_identifier(a.column)?);
                }
            }
            builder.push(") ");
            builder.push_values(values.iter(), |mut sep, record| {
                sep.push_bind(PostgresUtils::extract_string(record, FIELD_ID).unwrap_or_default());
                sep.push_bind(
                    PostgresUtils::extract_optional_timestamp(record, FIELD_CREATED_AT)
                        .unwrap_or_default(),
                );
                sep.push_bind(
                    PostgresUtils::extract_optional_timestamp(record, FIELD_UPDATED_AT)
                        .unwrap_or_default(),
                );
                sep.push_bind(
                    PostgresUtils::extract_optional_string_array(record, FIELD_PERMISSIONS)
                        .unwrap_or_default(),
                );
                for a in &attributes {
                    let val = record.get(a.id).unwrap_or(&StorageValue::Null);
                    PostgresQuery::push_bind_value_separated(&mut sep, val);
                    if a.kind == AttributeKind::Json {
                        sep.push_unseparated("::jsonb");
                    }
                }
            });
            builder.push(format!(
                " RETURNING {}",
                PostgresUtils::quote_identifier(database_core::COLUMN_SEQUENCE)?
            ));

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
            // Each entry: (document_id, permission_type, role_array)
            let mut perms_rows: Vec<(i64, String, Vec<String>)> = Vec::new();
            let mut results = Vec::with_capacity(values.len());

            for (i, row) in rows.into_iter().enumerate() {
                let seq: i64 = row.get(0);
                let permissions =
                    PostgresUtils::extract_optional_string_array(&values[i], FIELD_PERMISSIONS)
                        .unwrap_or_default();

                if !permissions.is_empty() {
                    let grouped = database_core::utils::permission_rows(&permissions)?;
                    for (pt, roles) in grouped {
                        perms_rows.push((seq, pt, roles));
                    }
                }

                let mut record = values[i].clone();
                record.insert(FIELD_SEQUENCE.to_string(), StorageValue::Int(seq));
                results.push(record);
            }

            // ── Bulk-insert into the permissions table ────────────────────
            if !perms_rows.is_empty() {
                let mut pb = QueryBuilder::<Postgres>::new(format!(
                    "INSERT INTO {perms_table} (document_id, permission_type, permissions) "
                ));
                pb.push_values(perms_rows.iter(), |mut sep, (seq, pt, roles)| {
                    sep.push_bind(*seq);
                    sep.push_bind(pt.clone());
                    sep.push_bind(roles.clone());
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
            let table = PostgresUtils::qualified_table_name(&context, schema.id)?;
            let select = PostgresUtils::select_columns(schema)?;
            let mut builder =
                QueryBuilder::<Postgres>::new(format!("SELECT {select} FROM {table} AS main "));
            let mut has_where = false;
            PostgresQuery::push_condition_separator(&mut builder, &mut has_where);
            builder.push(format!(
                "main.{} = ",
                PostgresUtils::quote_identifier(database_core::COLUMN_ID)?
            ));
            builder.push_bind(id);
            PostgresQuery::push_document_action_condition(
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
            row.map(|r| PostgresUtils::row_to_record_internal(&r, schema))
                .transpose()
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
            let table = PostgresUtils::qualified_table_name(&context, schema.id)?;
            let mut tx = pool
                .begin()
                .await
                .map_err(|e| DatabaseError::Other(e.to_string()))?;

            let mut builder = QueryBuilder::<Postgres>::new(format!("UPDATE {table} AS main SET "));
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
                builder.push(format!("{} = ", PostgresUtils::quote_identifier(a.column)?));
                PostgresQuery::push_bind_value(&mut builder, values.get(a.id).unwrap());
            }

            if let Some(val) = values.get(database_core::FIELD_UPDATED_AT) {
                if !first {
                    builder.push(", ");
                }
                first = false;
                builder.push(format!(
                    "{} = ",
                    PostgresUtils::quote_identifier(database_core::COLUMN_UPDATED_AT)?
                ));
                PostgresQuery::push_bind_value(&mut builder, val);
            }

            let permissions = PostgresUtils::extract_optional_string_array(
                &values,
                database_core::FIELD_PERMISSIONS,
            )
            .unwrap_or_default();
            let has_perms = values.contains_key(database_core::FIELD_PERMISSIONS);
            if has_perms {
                if !first {
                    builder.push(", ");
                }
                builder.push(format!(
                    "{} = ",
                    PostgresUtils::quote_identifier(database_core::COLUMN_PERMISSIONS)?
                ));
                builder.push_bind(permissions.clone());
            }

            let mut has_where = false;
            PostgresQuery::push_condition_separator(&mut builder, &mut has_where);
            builder.push(format!(
                "main.{} = ",
                PostgresUtils::quote_identifier(database_core::COLUMN_ID)?
            ));
            builder.push_bind(&id);
            PostgresQuery::push_document_action_condition(
                &mut builder,
                &context,
                schema,
                "main",
                PermissionEnum::Update,
                &mut has_where,
            )?;
            builder.push(format!(
                " RETURNING {}",
                PostgresUtils::select_columns(schema)?
            ));

            let row = builder
                .build()
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| DatabaseError::Other(e.to_string()))?;
            let record = match row {
                Some(r) => PostgresUtils::row_to_record_internal(&r, schema)?,
                None => return Ok(None),
            };

            if has_perms {
                let seq = record
                    .get(database_core::FIELD_SEQUENCE)
                    .unwrap()
                    .as_int()
                    .unwrap();
                let perms_table =
                    PostgresUtils::qualified_permissions_table_name(&context, schema.id)?;
                sqlx::query(&format!("DELETE FROM {perms_table} WHERE document_id = $1"))
                    .bind(seq)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| DatabaseError::Other(e.to_string()))?;

                let grouped_perms = database_core::utils::permission_rows(&permissions)?;
                for (pt, pv) in grouped_perms {
                    sqlx::query(&format!("INSERT INTO {perms_table} (document_id, permission_type, permissions) VALUES ($1, $2, $3)"))
                        .bind(seq)
                        .bind(pt)
                        .bind(pv)
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
            let table = PostgresUtils::qualified_table_name(&context, schema.id)?;
            let mut builder = QueryBuilder::<Postgres>::new(format!("UPDATE {table} AS main SET "));
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
                builder.push(format!("{} = ", PostgresUtils::quote_identifier(a.column)?));
                PostgresQuery::push_bind_value(&mut builder, values.get(a.id).unwrap());
            }

            if let Some(val) = values.get(database_core::FIELD_UPDATED_AT) {
                if !first {
                    builder.push(", ");
                }
                builder.push(format!(
                    "{} = ",
                    PostgresUtils::quote_identifier(database_core::COLUMN_UPDATED_AT)?
                ));
                PostgresQuery::push_bind_value(&mut builder, val);
            }

            if values.contains_key(database_core::FIELD_PERMISSIONS) {
                return Err(DatabaseError::Other(
                    "update_many does not support updating permissions".into(),
                ));
            }

            let mut has_where = false;
            PostgresQuery::push_document_action_condition(
                &mut builder,
                &context,
                schema,
                "main",
                PermissionEnum::Update,
                &mut has_where,
            )?;
            PostgresQuery::push_filters(&mut builder, schema, &query, &mut has_where)?;

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
            let table = PostgresUtils::qualified_table_name(&context, schema.id)?;
            let mut builder =
                QueryBuilder::<Postgres>::new(format!("DELETE FROM {table} AS main WHERE "));
            builder.push(format!(
                "main.{} = ",
                PostgresUtils::quote_identifier(database_core::COLUMN_ID)?
            ));
            builder.push_bind(id);
            let mut has_where = true;
            PostgresQuery::push_document_action_condition(
                &mut builder,
                &context,
                schema,
                "main",
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
            let table = PostgresUtils::qualified_table_name(&context, schema.id)?;
            let mut builder = QueryBuilder::<Postgres>::new(format!("DELETE FROM {table} AS main"));
            let mut has_where = false;
            PostgresQuery::push_document_action_condition(
                &mut builder,
                &context,
                schema,
                "main",
                PermissionEnum::Delete,
                &mut has_where,
            )?;
            PostgresQuery::push_filters(&mut builder, schema, &query, &mut has_where)?;
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
            let table = PostgresUtils::qualified_table_name(&context, schema.id)?;
            let select = PostgresUtils::select_columns(schema)?;
            let mut builder =
                QueryBuilder::<Postgres>::new(format!("SELECT {select} FROM {table} AS main"));
            let mut has_where = false;
            PostgresQuery::push_document_action_condition(
                &mut builder,
                &context,
                schema,
                "main",
                PermissionEnum::Read,
                &mut has_where,
            )?;
            PostgresQuery::push_filters(&mut builder, schema, &query, &mut has_where)?;
            PostgresQuery::push_sorts(&mut builder, schema, &query)?;
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
            rows.into_iter()
                .map(|r| PostgresUtils::row_to_record_internal(&r, schema))
                .collect()
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
            let base_table = PostgresUtils::qualified_table_name(&context, schema.id)?;
            let related_table = PostgresUtils::qualified_table_name(&context, related_schema.id)?;
            let base_select = PostgresUtils::select_columns_for_alias(schema, "main", "base")?;
            let related_select =
                PostgresUtils::select_columns_for_alias(related_schema, "rel", "rel")?;
            let use_base_subquery = matches!(
                relation.kind,
                RelationshipKind::OneToMany | RelationshipKind::ManyToMany
            );

            let mut builder = QueryBuilder::<Postgres>::new(format!(
                "SELECT {base_select}, {related_select} FROM "
            ));
            if use_base_subquery {
                builder.push("(SELECT ");
                builder.push(PostgresUtils::select_columns(schema)?);
                builder.push(format!(" FROM {base_table} AS main"));
                let mut has_where = false;
                PostgresQuery::push_document_action_condition(
                    &mut builder,
                    &context,
                    schema,
                    "main",
                    PermissionEnum::Read,
                    &mut has_where,
                )?;
                PostgresQuery::push_filters_for_alias(
                    &mut builder,
                    schema,
                    &query,
                    Some("main"),
                    &mut has_where,
                )?;
                PostgresQuery::push_sorts_for_alias(&mut builder, schema, &query, Some("main"))?;
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
                    let base_join_column = PostgresUtils::qualified_column_for_field(
                        schema,
                        relation.local_field,
                        Some("main"),
                    )?;
                    let related_join_column = PostgresUtils::qualified_column_for_field(
                        related_schema,
                        relation.remote_field,
                        Some("rel"),
                    )?;
                    builder.push(format!(
                        "LEFT JOIN {related_table} AS rel ON {base_join_column} = {related_join_column} AND "
                    ));
                    PostgresQuery::push_document_action_expression(
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
                        PostgresUtils::qualified_table_name(&context, through.schema.id)?;
                    let base_join_column = PostgresUtils::qualified_column_for_field(
                        schema,
                        relation.local_field,
                        Some("main"),
                    )?;
                    let through_local_column = PostgresUtils::qualified_column_for_field(
                        through.schema,
                        through.local_field,
                        Some("jt"),
                    )?;
                    let through_remote_column = PostgresUtils::qualified_column_for_field(
                        through.schema,
                        through.remote_field,
                        Some("jt"),
                    )?;
                    let related_join_column = PostgresUtils::qualified_column_for_field(
                        related_schema,
                        relation.remote_field,
                        Some("rel"),
                    )?;
                    builder.push(format!(
                        "LEFT JOIN {through_table} AS jt ON {base_join_column} = {through_local_column} AND "
                    ));
                    PostgresQuery::push_document_action_expression(
                        &mut builder,
                        &context,
                        through.schema,
                        "jt",
                        PermissionEnum::Read,
                    )?;
                    builder.push(format!(
                        " LEFT JOIN {related_table} AS rel ON {through_remote_column} = {related_join_column} AND "
                    ));
                    PostgresQuery::push_document_action_expression(
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
                PostgresQuery::push_document_action_condition(
                    &mut builder,
                    &context,
                    schema,
                    "main",
                    PermissionEnum::Read,
                    &mut has_where,
                )?;
                PostgresQuery::push_filters_for_alias(
                    &mut builder,
                    schema,
                    &query,
                    Some("main"),
                    &mut has_where,
                )?;
            }

            if !query.sorts().is_empty() {
                PostgresQuery::push_sorts_for_alias(&mut builder, schema, &query, Some("main"))?;
                if use_base_subquery {
                    builder.push(", ");
                    builder.push(format!(
                        "rel.{} ASC",
                        PostgresUtils::quote_identifier(database_core::COLUMN_SEQUENCE)?
                    ));
                }
            } else if use_base_subquery {
                builder.push(" ORDER BY ");
                builder.push(format!(
                    "main.{} ASC, rel.{} ASC",
                    PostgresUtils::quote_identifier(database_core::COLUMN_SEQUENCE)?,
                    PostgresUtils::quote_identifier(database_core::COLUMN_SEQUENCE)?,
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

            let rows = builder
                .build()
                .fetch_all(&pool)
                .await
                .map_err(|e| DatabaseError::Other(e.to_string()))?;

            let mut joined = Vec::with_capacity(rows.len());
            for row in rows {
                let Some(base) =
                    PostgresUtils::row_to_record_internal_prefixed(&row, schema, Some("base"))?
                else {
                    continue;
                };
                let related = PostgresUtils::row_to_record_internal_prefixed(
                    &row,
                    related_schema,
                    Some("rel"),
                )?;
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

            let base_table = PostgresUtils::qualified_table_name(&context, schema.id)?;
            let mut select_parts = vec![PostgresUtils::select_columns_for_alias(
                schema, "main", "base",
            )?];
            for (index, populate) in populates.iter().enumerate() {
                let alias = format!("rel_{index}");
                select_parts.push(PostgresUtils::select_columns_for_alias(
                    populate.schema,
                    &alias,
                    &alias,
                )?);
            }

            let use_base_subquery = populates.iter().any(|populate| {
                matches!(
                    populate.relation.kind,
                    RelationshipKind::OneToMany | RelationshipKind::ManyToMany
                )
            });

            let mut builder =
                QueryBuilder::<Postgres>::new(format!("SELECT {} FROM ", select_parts.join(", ")));
            if use_base_subquery {
                builder.push("(SELECT ");
                builder.push(PostgresUtils::select_columns(schema)?);
                builder.push(format!(" FROM {base_table} AS main"));
                let mut has_where = false;
                PostgresQuery::push_document_action_condition(
                    &mut builder,
                    &context,
                    schema,
                    "main",
                    PermissionEnum::Read,
                    &mut has_where,
                )?;
                PostgresQuery::push_filters_for_alias(
                    &mut builder,
                    schema,
                    &query,
                    Some("main"),
                    &mut has_where,
                )?;
                PostgresQuery::push_sorts_for_alias(&mut builder, schema, &query, Some("main"))?;
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
                let related_table =
                    PostgresUtils::qualified_table_name(&context, populate.schema.id)?;
                let relation_alias = format!("rel_{index}");

                match relation.kind {
                    RelationshipKind::ManyToOne
                    | RelationshipKind::OneToOne
                    | RelationshipKind::OneToMany => {
                        let base_join_column = PostgresUtils::qualified_column_for_field(
                            schema,
                            relation.local_field,
                            Some("main"),
                        )?;
                        let related_join_column = PostgresUtils::qualified_column_for_field(
                            populate.schema,
                            relation.remote_field,
                            Some(&relation_alias),
                        )?;
                        builder.push(format!(
                            "LEFT JOIN {related_table} AS {relation_alias} ON {base_join_column} = {related_join_column} AND "
                        ));
                        PostgresQuery::push_document_action_expression(
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
                            PostgresUtils::qualified_table_name(&context, through.schema.id)?;
                        let base_join_column = PostgresUtils::qualified_column_for_field(
                            schema,
                            relation.local_field,
                            Some("main"),
                        )?;
                        let through_local_column = PostgresUtils::qualified_column_for_field(
                            through.schema,
                            through.local_field,
                            Some(&through_alias),
                        )?;
                        let through_remote_column = PostgresUtils::qualified_column_for_field(
                            through.schema,
                            through.remote_field,
                            Some(&through_alias),
                        )?;
                        let related_join_column = PostgresUtils::qualified_column_for_field(
                            populate.schema,
                            relation.remote_field,
                            Some(&relation_alias),
                        )?;
                        builder.push(format!(
                            "LEFT JOIN {through_table} AS {through_alias} ON {base_join_column} = {through_local_column} AND "
                        ));
                        PostgresQuery::push_document_action_expression(
                            &mut builder,
                            &context,
                            through.schema,
                            &through_alias,
                            PermissionEnum::Read,
                        )?;
                        builder.push(format!(
                            " LEFT JOIN {related_table} AS {relation_alias} ON {through_remote_column} = {related_join_column} AND "
                        ));
                        PostgresQuery::push_document_action_expression(
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
                PostgresQuery::push_document_action_condition(
                    &mut builder,
                    &context,
                    schema,
                    "main",
                    PermissionEnum::Read,
                    &mut has_where,
                )?;
                PostgresQuery::push_filters_for_alias(
                    &mut builder,
                    schema,
                    &query,
                    Some("main"),
                    &mut has_where,
                )?;
            }

            if !query.sorts().is_empty() {
                PostgresQuery::push_sorts_for_alias(&mut builder, schema, &query, Some("main"))?;
                if use_base_subquery {
                    for index in 0..populates.len() {
                        builder.push(", ");
                        builder.push(format!(
                            "rel_{index}.{} ASC",
                            PostgresUtils::quote_identifier(database_core::COLUMN_SEQUENCE)?,
                        ));
                    }
                }
            } else if use_base_subquery {
                builder.push(" ORDER BY ");
                builder.push(format!(
                    "main.{} ASC",
                    PostgresUtils::quote_identifier(database_core::COLUMN_SEQUENCE)?,
                ));
                for index in 0..populates.len() {
                    builder.push(", ");
                    builder.push(format!(
                        "rel_{index}.{} ASC",
                        PostgresUtils::quote_identifier(database_core::COLUMN_SEQUENCE)?,
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

            let rows = builder
                .build()
                .fetch_all(&pool)
                .await
                .map_err(|e| DatabaseError::Other(e.to_string()))?;

            let mut populated_rows = Vec::with_capacity(rows.len());
            for row in rows {
                let Some(base) =
                    PostgresUtils::row_to_record_internal_prefixed(&row, schema, Some("base"))?
                else {
                    continue;
                };
                let mut related = std::collections::BTreeMap::new();
                for (index, populate) in populates.iter().enumerate() {
                    let prefix = format!("rel_{index}");
                    let record = PostgresUtils::row_to_record_internal_prefixed(
                        &row,
                        populate.schema,
                        Some(&prefix),
                    )?;
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
            let table = PostgresUtils::qualified_table_name(&context, schema.id)?;
            let mut builder =
                QueryBuilder::<Postgres>::new(format!("SELECT COUNT(*) FROM {table} AS main"));
            let mut has_where = false;
            PostgresQuery::push_document_action_condition(
                &mut builder,
                &context,
                schema,
                "main",
                PermissionEnum::Read,
                &mut has_where,
            )?;
            PostgresQuery::push_filters(&mut builder, schema, &query, &mut has_where)?;
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
