use database_core::errors::DatabaseError;
use database_core::traits::migration::{MigrationCollection, MigrationIndex};
use database_core::{
    AttributePersistence, Context, IndexKind, Order,
    COLUMN_CREATED_AT, COLUMN_ID, COLUMN_PERMISSIONS, COLUMN_SEQUENCE, COLUMN_UPDATED_AT,
};
use crate::driver::PostgresAdapter;
use sqlx::{Pool, Postgres, Row, Executor};
use std::collections::BTreeMap;

pub enum MigrationChange {
    CreateTable(String),
    AddColumn { table: String, column: String, sql_type: String, required: bool },
    CreateIndex { table: String, index_id: String, sql: String },
    DropIndex { table: String, index_id: String },
}

pub struct MigrationEngine<'a> {
    pool: &'a Pool<Postgres>,
}

impl<'a> MigrationEngine<'a> {
    pub fn new(pool: &'a Pool<Postgres>) -> Self {
        Self { pool }
    }

    pub async fn migrate(&self, context: &Context, collections: &[&dyn MigrationCollection]) -> Result<(), DatabaseError> {
        let changes = self.diff(context, collections).await?;
        for change in changes {
            self.apply_change(context, change, collections).await?;
        }
        Ok(())
    }

    pub async fn diff(&self, context: &Context, collections: &[&dyn MigrationCollection]) -> Result<Vec<MigrationChange>, DatabaseError> {
        let mut all_changes = Vec::new();
        for collection in collections {
            all_changes.extend(self.diff_collection(context, *collection).await?);
        }
        Ok(all_changes)
    }

    async fn diff_collection(&self, context: &Context, collection: &dyn MigrationCollection) -> Result<Vec<MigrationChange>, DatabaseError> {
        let mut changes = Vec::new();
        let table_name = collection.id();
        let db_schema = context.schema();
        
        let exists: bool = sqlx::query(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)"
        )
        .bind(db_schema)
        .bind(table_name)
        .fetch_one(self.pool)
        .await
        .map(|row| row.get(0))
        .map_err(|e| DatabaseError::Other(format!("failed to check table existence: {e}")))?;

        if !exists {
            changes.push(MigrationChange::CreateTable(table_name.to_string()));
            return Ok(changes);
        }

        // Check columns
        let columns: Vec<(String, String)> = sqlx::query(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2"
        )
        .bind(db_schema)
        .bind(table_name)
        .fetch_all(self.pool)
        .await
        .map_err(|e| DatabaseError::Other(format!("failed to fetch columns: {e}")))?
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();

        let existing_columns: BTreeMap<String, String> = columns.into_iter().collect();

        for attr in collection.attributes() {
            if attr.persistence == AttributePersistence::Persisted && !existing_columns.contains_key(&attr.column) {
                changes.push(MigrationChange::AddColumn {
                    table: table_name.to_string(),
                    column: attr.column.to_string(),
                    sql_type: PostgresAdapter::sql_type(attr.kind, attr.array),
                    required: attr.required,
                });
            }
        }

        // Check indexes
        let existing_indexes: Vec<(String, String)> = sqlx::query(
            "SELECT i.relname as index_name, pg_get_indexdef(i.oid) as index_def
             FROM pg_index x
             JOIN pg_class c ON c.oid = x.indrelid
             JOIN pg_class i ON i.oid = x.indexrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE c.relname = $1 AND n.nspname = $2"
        )
        .bind(table_name)
        .bind(db_schema)
        .fetch_all(self.pool)
        .await
        .map_err(|e| DatabaseError::Other(format!("failed to fetch indexes: {e}")))?
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();

        let existing_index_map: BTreeMap<String, String> = existing_indexes.into_iter().collect();
        let table_qualified = PostgresAdapter::qualified_table_name(context, table_name)?;

        for index in collection.indexes() {
            let index_name = index.id.clone();
            let expected_sql = match index.kind {
                IndexKind::Key => format!(
                    "CREATE INDEX {index_name} ON {table_qualified} ({})",
                    self.quoted_column_list_generic(collection, &index)?
                ),
                IndexKind::Unique => format!(
                    "CREATE UNIQUE INDEX {index_name} ON {table_qualified} ({})",
                    self.quoted_column_list_generic(collection, &index)?
                ),
                IndexKind::FullText => format!(
                    "CREATE INDEX {index_name} ON {table_qualified} USING gin ({})",
                    self.full_text_expression_generic(collection, &index.attributes)?
                ),
                IndexKind::Spatial => {
                    return Err(DatabaseError::Other(format!(
                        "collection '{}': postgres adapter does not support spatial indexes yet",
                        collection.id()
                    )));
                }
            };

            if let Some(_existing_def) = existing_index_map.get(&index_name) {
                // In a production engine, we would normalize and compare.
            } else {
                changes.push(MigrationChange::CreateIndex {
                    table: table_name.to_string(),
                    index_id: index_name,
                    sql: expected_sql,
                });
            }
        }

        Ok(changes)
    }

    async fn apply_change(&self, context: &Context, change: MigrationChange, collections: &[&dyn MigrationCollection]) -> Result<(), DatabaseError> {
        match change {
            MigrationChange::CreateTable(table_id) => {
                let collection = collections.iter().find(|c| c.id() == table_id).ok_or_else(|| {
                    DatabaseError::Other(format!("Collection {} not found in migration source", table_id))
                })?;
                self.create_collection_generic(context, *collection).await
            }
            MigrationChange::AddColumn { table, column, sql_type, required } => {
                let table_name = PostgresAdapter::qualified_table_name(context, &table)?;
                let column_name = PostgresAdapter::quote_identifier(&column)?;
                let nullable = if required { "NOT NULL" } else { "" };
                let sql = format!("ALTER TABLE {} ADD COLUMN {} {} {}", 
                    table_name, column_name, sql_type, nullable);
                
                println!("Executing: {}", sql);
                self.pool.execute(sql.as_str()).await.map_err(|e| DatabaseError::Other(format!("failed to add column: {e}")))?;
                Ok(())
            }
            MigrationChange::CreateIndex { sql, .. } => {
                let sql = sql.replace("CREATE INDEX ", "CREATE INDEX IF NOT EXISTS ");
                let sql = sql.replace("CREATE UNIQUE INDEX ", "CREATE UNIQUE INDEX IF NOT EXISTS ");
                println!("Executing: {}", sql);
                self.pool.execute(sql.as_str()).await.map_err(|e| DatabaseError::Other(format!("failed to create index: {e}")))?;
                Ok(())
            }
            MigrationChange::DropIndex { index_id, .. } => {
                let schema_name = PostgresAdapter::quote_identifier(context.schema())?;
                let index_name = PostgresAdapter::quote_identifier(&index_id)?;
                let sql = format!("DROP INDEX IF EXISTS {}.{}", schema_name, index_name);
                println!("Executing: {}", sql);
                self.pool.execute(sql.as_str()).await.map_err(|e| DatabaseError::Other(format!("failed to drop index: {e}")))?;
                Ok(())
            }
        }
    }

    async fn create_collection_generic(&self, context: &Context, collection: &dyn MigrationCollection) -> Result<(), DatabaseError> {
        let table = PostgresAdapter::qualified_table_name(context, collection.id())?;
        let perms_table = PostgresAdapter::qualified_permissions_table_name(context, collection.id())?;

        let mut statements = vec![
            format!(
                "CREATE TABLE IF NOT EXISTS {table} (
                    {} BIGSERIAL PRIMARY KEY,
                    {} TEXT NOT NULL,
                    {} TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    {} TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    {} TEXT[] NOT NULL DEFAULT '{{}}'
                )",
                PostgresAdapter::quoted_system_column(COLUMN_SEQUENCE)?,
                PostgresAdapter::quoted_system_column(COLUMN_ID)?,
                PostgresAdapter::quoted_system_column(COLUMN_CREATED_AT)?,
                PostgresAdapter::quoted_system_column(COLUMN_UPDATED_AT)?,
                PostgresAdapter::quoted_system_column(COLUMN_PERMISSIONS)?,
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {perms_table} (
                    document_id BIGINT NOT NULL REFERENCES {table} ({}) ON DELETE CASCADE,
                    permission_type TEXT NOT NULL,
                    permissions TEXT[] NOT NULL,
                    PRIMARY KEY (document_id, permission_type)
                )",
                PostgresAdapter::quoted_system_column(COLUMN_SEQUENCE)?
            ),
        ];

        for attr in collection.attributes() {
            if attr.persistence == AttributePersistence::Persisted {
                let column = PostgresAdapter::quote_identifier(&attr.column)?;
                let sql_type = PostgresAdapter::sql_type(attr.kind, attr.array);
                let nullable = if attr.required { "NOT NULL" } else { "" };
                statements.push(format!("ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {sql_type} {nullable}"));
            }
        }

        // Internal indexes
        statements.push(format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {table} ({})",
            PostgresAdapter::quote_identifier(&format!("{}_uid", collection.id()))?,
            PostgresAdapter::quoted_system_column(COLUMN_ID)?
        ));
        statements.push(format!(
            "CREATE INDEX IF NOT EXISTS {} ON {table} ({})",
            PostgresAdapter::quote_identifier(&format!("{}_created_at", collection.id()))?,
            PostgresAdapter::quoted_system_column(COLUMN_CREATED_AT)?
        ));
        statements.push(format!(
            "CREATE INDEX IF NOT EXISTS {} ON {table} ({})",
            PostgresAdapter::quote_identifier(&format!("{}_updated_at", collection.id()))?,
            PostgresAdapter::quoted_system_column(COLUMN_UPDATED_AT)?
        ));
        statements.push(format!(
            "CREATE INDEX IF NOT EXISTS {} ON {table} USING GIN ({})",
            PostgresAdapter::quote_identifier(&format!("{}_permissions_gin_idx", collection.id()))?,
            PostgresAdapter::quoted_system_column(COLUMN_PERMISSIONS)?
        ));
        statements.push(format!(
            "CREATE INDEX IF NOT EXISTS {} ON {perms_table} USING GIN (permissions)",
            PostgresAdapter::quote_identifier(&format!("{}_perms_permissions_gin_idx", collection.id()))?
        ));
        statements.push(format!(
            "CREATE INDEX IF NOT EXISTS {} ON {perms_table} (permission_type)",
            PostgresAdapter::quote_identifier(&format!("{}_perms_permission_type_idx", collection.id()))?
        ));
        statements.push(format!(
            "CREATE INDEX IF NOT EXISTS {} ON {perms_table} (document_id)",
            PostgresAdapter::quote_identifier(&format!("{}_perms_document_id_idx", collection.id()))?
        ));

        // Schema indexes
        for index in collection.indexes() {
            let index_name = PostgresAdapter::quote_identifier(&index.id)?;
            let col_list = self.quoted_column_list_generic(collection, &index)?;
            let statement = match index.kind {
                IndexKind::Key => format!(
                    "CREATE INDEX IF NOT EXISTS {index_name} ON {table} ({col_list})"
                ),
                IndexKind::Unique => format!(
                    "CREATE UNIQUE INDEX IF NOT EXISTS {index_name} ON {table} ({col_list})"
                ),
                IndexKind::FullText => format!(
                    "CREATE INDEX IF NOT EXISTS {index_name} ON {table} USING GIN ({})",
                    self.full_text_expression_generic(collection, &index.attributes)?
                ),
                IndexKind::Spatial => {
                    return Err(DatabaseError::Other(format!(
                        "collection '{}': postgres adapter does not support spatial indexes yet",
                        collection.id()
                    )));
                }
            };
            statements.push(statement);
        }

        let mut tx = self.pool.begin().await.map_err(|e| DatabaseError::Other(format!("failed to start transaction: {e}")))?;
        for statement in statements {
            tx.execute(statement.as_str()).await.map_err(|e| DatabaseError::Other(format!("failed to execute statement: {e}")))?;
        }
        tx.commit().await.map_err(|e| DatabaseError::Other(format!("failed to commit transaction: {e}")))?;

        Ok(())
    }

    fn quoted_column_list_generic(&self, collection: &dyn MigrationCollection, index: &MigrationIndex) -> Result<String, DatabaseError> {
        let mut out = Vec::with_capacity(index.attributes.len());
        for (i, attribute_id) in index.attributes.iter().enumerate() {
            let attr = collection.attributes().into_iter().find(|a| a.id == *attribute_id).ok_or_else(|| {
                DatabaseError::Other(format!("index references unknown attribute '{}.{}'", collection.id(), attribute_id))
            })?;
            let mut column = PostgresAdapter::quote_identifier(&attr.column)?;
            if let Some(order) = index.orders.get(i) {
                match order {
                    Order::Asc => column.push_str(" ASC"),
                    Order::Desc => column.push_str(" DESC"),
                    Order::None => {}
                }
            }
            out.push(column);
        }
        Ok(out.join(", "))
    }

    fn full_text_expression_generic(&self, collection: &dyn MigrationCollection, attributes: &[String]) -> Result<String, DatabaseError> {
        let mut parts = Vec::with_capacity(attributes.len());
        for attribute_id in attributes {
            let attr = collection.attributes().into_iter().find(|a| a.id == *attribute_id).ok_or_else(|| {
                DatabaseError::Other(format!("index references unknown attribute '{}.{}'", collection.id(), attribute_id))
            })?;
            let column = PostgresAdapter::quote_identifier(&attr.column)?;
            parts.push(format!("COALESCE({column}::text, '')"));
        }

        Ok(format!(
            "to_tsvector('simple', concat_ws(' ', {}))",
            parts.join(", ")
        ))
    }
}
