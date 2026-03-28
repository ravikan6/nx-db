use database_core::errors::DatabaseError;
use sqlx::error::{DatabaseError as SqlxDatabaseError, ErrorKind};

pub(crate) fn map_sqlite_error(error: sqlx::Error) -> DatabaseError {
    match error {
        sqlx::Error::RowNotFound => DatabaseError::NotFound("row not found".into()),
        sqlx::Error::PoolTimedOut => {
            DatabaseError::Timeout("sqlite connection pool timed out".into())
        }
        sqlx::Error::PoolClosed => {
            DatabaseError::Unavailable("sqlite connection pool is closed".into())
        }
        sqlx::Error::Io(error) => DatabaseError::Unavailable(format!("sqlite I/O error: {error}")),
        sqlx::Error::Tls(error) => DatabaseError::Unavailable(format!("sqlite TLS error: {error}")),
        sqlx::Error::Protocol(message) => {
            DatabaseError::Storage(format!("sqlite protocol error: {message}"))
        }
        sqlx::Error::Configuration(message) => {
            DatabaseError::Validation(format!("sqlite configuration error: {message}"))
        }
        sqlx::Error::Database(error) => map_sqlite_database_error(error.as_ref()),
        other => DatabaseError::Storage(format!("sqlite error: {other}")),
    }
}

pub(crate) fn map_sqlite_error_with_context(
    context: impl AsRef<str>,
    error: sqlx::Error,
) -> DatabaseError {
    map_sqlite_error(error).with_context(context)
}

fn map_sqlite_database_error(error: &(dyn SqlxDatabaseError + 'static)) -> DatabaseError {
    let message = describe_database_error(error);
    match error.kind() {
        ErrorKind::UniqueViolation => DatabaseError::Duplicate(message),
        ErrorKind::ForeignKeyViolation => DatabaseError::ForeignKeyViolation(message),
        ErrorKind::NotNullViolation | ErrorKind::CheckViolation => {
            DatabaseError::ConstraintViolation(message)
        }
        ErrorKind::Other => match error.code().as_deref() {
            Some("5") | Some("6") => DatabaseError::Retryable(message),
            Some("1555") | Some("2067") => DatabaseError::Duplicate(message),
            Some("787") => DatabaseError::ForeignKeyViolation(message),
            Some("275") | Some("1299") | Some("19") => DatabaseError::ConstraintViolation(message),
            _ => DatabaseError::Storage(message),
        },
        _ => DatabaseError::Storage(message),
    }
}

fn describe_database_error(error: &(dyn SqlxDatabaseError + 'static)) -> String {
    let mut parts = vec![error.message().to_string()];
    if let Some(code) = error.code() {
        parts.push(format!("code={code}"));
    }
    if let Some(constraint) = error.constraint() {
        parts.push(format!("constraint={constraint}"));
    }
    parts.join(", ")
}

#[cfg(test)]
mod tests {
    use super::map_sqlite_error;
    use database_core::errors::DatabaseError;
    use sqlx::sqlite::SqlitePoolOptions;

    #[tokio::test]
    async fn maps_duplicate_foreign_key_and_constraint_errors() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("sqlite pool should connect");

        sqlx::query("PRAGMA foreign_keys = ON")
            .execute(&pool)
            .await
            .expect("foreign keys should enable");
        sqlx::query("CREATE TABLE parents (id TEXT PRIMARY KEY)")
            .execute(&pool)
            .await
            .expect("parents table should create");
        sqlx::query(
            "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id TEXT NOT NULL REFERENCES parents(id), name TEXT NOT NULL)",
        )
        .execute(&pool)
        .await
        .expect("children table should create");

        sqlx::query("INSERT INTO parents (id) VALUES (?)")
            .bind("parent-1")
            .execute(&pool)
            .await
            .expect("first parent insert should succeed");

        let duplicate = sqlx::query("INSERT INTO parents (id) VALUES (?)")
            .bind("parent-1")
            .execute(&pool)
            .await
            .expect_err("duplicate insert should fail");
        assert!(matches!(
            map_sqlite_error(duplicate),
            DatabaseError::Duplicate(_)
        ));

        let foreign_key = sqlx::query("INSERT INTO children (parent_id, name) VALUES (?, ?)")
            .bind("missing-parent")
            .bind("child")
            .execute(&pool)
            .await
            .expect_err("foreign key insert should fail");
        assert!(matches!(
            map_sqlite_error(foreign_key),
            DatabaseError::ForeignKeyViolation(_)
        ));

        let not_null = sqlx::query("INSERT INTO children (parent_id, name) VALUES (?, ?)")
            .bind("parent-1")
            .bind(Option::<String>::None)
            .execute(&pool)
            .await
            .expect_err("not-null insert should fail");
        assert!(matches!(
            map_sqlite_error(not_null),
            DatabaseError::ConstraintViolation(_)
        ));
    }
}
