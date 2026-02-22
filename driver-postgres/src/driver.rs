use nx_core::errors::DatabaseError;
use nx_core::traits::adapter::Adapter;
use nx_core::Context;
use sqlx::{Pool, Postgres};
use std::future::Future;

#[derive(Clone)]
pub struct PostgresAdapter<'a> {
    context: Context,
    pool: &'a Pool<Postgres>,
}

impl<'a> PostgresAdapter<'a> {
    fn is_valid_identifier(identifier: &str) -> bool {
        let mut chars = identifier.chars();
        let Some(first) = chars.next() else {
            return false;
        };

        if !(first == '_' || first.is_ascii_alphabetic()) {
            return false;
        }

        chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
    }

    fn qualified_table_name(&self, collection: &str) -> Result<String, DatabaseError> {
        let schema = self.context.schema();

        if !Self::is_valid_identifier(schema) {
            return Err(DatabaseError::Other(format!(
                "invalid schema identifier: {schema}"
            )));
        }

        if !Self::is_valid_identifier(collection) {
            return Err(DatabaseError::Other(format!(
                "invalid collection identifier: {collection}"
            )));
        }

        Ok(format!("{schema}.{collection}"))
    }
}

impl<'p> Adapter for PostgresAdapter<'p> {
    type Pool = &'p Pool<Postgres>;

    fn new(pool: Self::Pool, context: Context) -> Self {
        Self { context, pool }
    }

    fn pool(&self) -> &Self::Pool {
        &self.pool
    }

    fn context(&self) -> &Context {
        &self.context
    }

    fn ping(&self) -> impl Future<Output=Result<(), DatabaseError>> + Send {
        let pool = self.pool.clone();
        async move {
            sqlx::query_scalar::<_, i32>("SELECT 1")
                .fetch_one(&pool)
                .await
                .map(|_| ())
                .map_err(|error| DatabaseError::Other(format!("postgres ping failed: {error}")))
        }
    }

    fn create<'a>(
        &'a self,
        collection: &'a str,
        id: &'a str,
        payload: &'a str,
    ) -> impl Future<Output=Result<(), DatabaseError>> + Send + 'a {
        async move {
            let table = self.qualified_table_name(collection)?;
            let query = format!("INSERT INTO {table} (id, payload) VALUES ($1, $2::jsonb)");

            sqlx::query(&query)
                .bind(id)
                .bind(payload)
                .execute(self.pool)
                .await
                .map(|_| ())
                .map_err(|error| DatabaseError::Other(format!("postgres create failed: {error}")))
        }
    }

    fn read<'a>(
        &'a self,
        collection: &'a str,
        id: &'a str,
    ) -> impl Future<Output=Result<Option<String>, DatabaseError>> + Send + 'a {
        async move {
            let table = self.qualified_table_name(collection)?;
            let query = format!("SELECT payload::text FROM {table} WHERE id = $1");

            sqlx::query_scalar::<_, String>(&query)
                .bind(id)
                .fetch_optional(self.pool)
                .await
                .map_err(|error| DatabaseError::Other(format!("postgres read failed: {error}")))
        }
    }

    fn update<'a>(
        &'a self,
        collection: &'a str,
        id: &'a str,
        payload: &'a str,
    ) -> impl Future<Output=Result<bool, DatabaseError>> + Send + 'a {
        async move {
            let table = self.qualified_table_name(collection)?;
            let query =
                format!("UPDATE {table} SET payload = $2::jsonb, updated_at = NOW() WHERE id = $1");

            sqlx::query(&query)
                .bind(id)
                .bind(payload)
                .execute(self.pool)
                .await
                .map(|result| result.rows_affected() > 0)
                .map_err(|error| DatabaseError::Other(format!("postgres update failed: {error}")))
        }
    }

    fn delete<'a>(
        &'a self,
        collection: &'a str,
        id: &'a str,
    ) -> impl Future<Output=Result<bool, DatabaseError>> + Send + 'a {
        async move {
            let table = self.qualified_table_name(collection)?;
            let query = format!("DELETE FROM {table} WHERE id = $1");

            sqlx::query(&query)
                .bind(id)
                .execute(self.pool)
                .await
                .map(|result| result.rows_affected() > 0)
                .map_err(|error| DatabaseError::Other(format!("postgres delete failed: {error}")))
        }
    }
}
