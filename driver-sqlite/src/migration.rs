use crate::driver::SqliteAdapter;
use crate::utils::SqliteUtils;
use database_core::errors::DatabaseError;
use database_core::traits::migration::{MigrationCollection};
use database_core::{
    AttributePersistence, COLUMN_CREATED_AT, COLUMN_ID, COLUMN_PERMISSIONS, COLUMN_SEQUENCE,
    COLUMN_UPDATED_AT, Context,
};
use sqlx::{Executor, Pool, Sqlite, Row};
use std::collections::BTreeMap;

pub enum MigrationChange {
    CreateTable(String),
    AddColumn {
        table: String,
        column: String,
        sql_type: String,
        required: bool,
    },
}

pub struct MigrationEngine<'a> {
    pool: &'a Pool<Sqlite>,
}

impl<'a> MigrationEngine<'a> {
    pub fn new(pool: &'a Pool<Sqlite>) -> Self {
        Self { pool }
    }

    pub async fn migrate(
        &self,
        context: &Context,
        collections: &[&dyn MigrationCollection],
    ) -> Result<(), DatabaseError> {
        let changes = self.diff(context, collections).await?;
        for change in changes {
            self.apply_change(context, change, collections).await?;
        }
        Ok(())
    }

    pub async fn diff(
        &self,
        context: &Context,
        collections: &[&dyn MigrationCollection],
    ) -> Result<Vec<MigrationChange>, DatabaseError> {
        let mut all_changes = Vec::new();
        for collection in collections {
            all_changes.extend(self.diff_collection(context, *collection).await?);
        }
        Ok(all_changes)
    }

    async fn diff_collection(
        &self,
        _context: &Context,
        collection: &dyn MigrationCollection,
    ) -> Result<Vec<MigrationChange>, DatabaseError> {
        let mut changes = Vec::new();
        let table_name = collection.id();

        let exists: bool = sqlx::query("SELECT name FROM sqlite_master WHERE type='table' AND name=?")
            .bind(table_name)
            .fetch_optional(self.pool)
            .await
            .map(|opt| opt.is_some())
            .map_err(|e| DatabaseError::Other(format!("failed to check table existence: {e}")))?;

        if !exists {
            changes.push(MigrationChange::CreateTable(table_name.to_string()));
            return Ok(changes);
        }

        // Fetch existing columns via PRAGMA table_info
        let columns: Vec<String> = sqlx::query(&format!("PRAGMA table_info({})", SqliteUtils::quote_identifier(table_name)))
            .fetch_all(self.pool)
            .await
            .map_err(|e| DatabaseError::Other(format!("failed to fetch columns: {e}")))?
            .into_iter()
            .map(|row| row.get::<String, _>(1))
            .collect();

        let existing_columns: BTreeMap<String, bool> = columns.into_iter().map(|c| (c, true)).collect();

        for attr in collection.attributes() {
            if attr.persistence == AttributePersistence::Persisted && !existing_columns.contains_key(&attr.column) {
                changes.push(MigrationChange::AddColumn {
                    table: table_name.to_string(),
                    column: attr.column.to_string(),
                    sql_type: SqliteAdapter::sql_type(attr.kind, attr.array).to_string(),
                    required: attr.required,
                });
            }
        }

        Ok(changes)
    }

    async fn apply_change(
        &self,
        context: &Context,
        change: MigrationChange,
        collections: &[&dyn MigrationCollection],
    ) -> Result<(), DatabaseError> {
        match change {
            MigrationChange::CreateTable(table_id) => {
                let collection = collections.iter().find(|c| c.id() == table_id)
                    .ok_or_else(|| DatabaseError::Other(format!("Collection {} not found", table_id)))?;
                
                let table = SqliteUtils::qualified_table_name(context, collection.id());
                let perms_table = SqliteUtils::qualified_permissions_table_name(context, collection.id());

                let mut cols = vec![
                    format!("{} INTEGER PRIMARY KEY AUTOINCREMENT", COLUMN_SEQUENCE),
                    format!("{} TEXT NOT NULL UNIQUE", COLUMN_ID),
                    format!("{} TEXT NOT NULL", COLUMN_CREATED_AT),
                    format!("{} TEXT NOT NULL", COLUMN_UPDATED_AT),
                    format!("{} TEXT NOT NULL DEFAULT '[]'", COLUMN_PERMISSIONS),
                ];

                for attr in collection.attributes() {
                    if attr.persistence == AttributePersistence::Persisted {
                        let st = SqliteAdapter::sql_type(attr.kind, attr.array);
                        cols.push(format!("{} {} {}", SqliteUtils::quote_identifier(&attr.column), st, if attr.required { " NOT NULL" } else { "DEFAULT NULL" }));
                    }
                }

                let sql = format!("CREATE TABLE IF NOT EXISTS {table} ({})", cols.join(", "));
                let perms_sql = format!("CREATE TABLE IF NOT EXISTS {perms_table} (document_id INTEGER NOT NULL REFERENCES {table}({COLUMN_SEQUENCE}) ON DELETE CASCADE, permission_type TEXT NOT NULL, permissions TEXT NOT NULL DEFAULT '[]', PRIMARY KEY (document_id, permission_type))");

                let mut tx = self.pool.begin().await.map_err(|e| DatabaseError::Other(e.to_string()))?;
                sqlx::query(&sql).execute(&mut *tx).await.map_err(|e| DatabaseError::Other(e.to_string()))?;
                sqlx::query(&perms_sql).execute(&mut *tx).await.map_err(|e| DatabaseError::Other(e.to_string()))?;
                tx.commit().await.map_err(|e| DatabaseError::Other(e.to_string()))?;
                Ok(())
            }
            MigrationChange::AddColumn { table, column, sql_type, required } => {
                let table_quoted = SqliteUtils::quote_identifier(&table);
                let column_quoted = SqliteUtils::quote_identifier(&column);
                let nullable = if required { "NOT NULL" } else { "DEFAULT NULL" };
                let sql = format!("ALTER TABLE {table_quoted} ADD COLUMN {column_quoted} {sql_type} {nullable}");
                
                self.pool.execute(sql.as_str()).await.map_err(|e| DatabaseError::Other(e.to_string()))?;
                Ok(())
            }
        }
    }
}
