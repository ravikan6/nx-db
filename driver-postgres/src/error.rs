use database_core::errors::DatabaseError;
use sqlx::error::{DatabaseError as SqlxDatabaseError, ErrorKind};

pub(crate) fn map_postgres_error(error: sqlx::Error) -> DatabaseError {
    match error {
        sqlx::Error::RowNotFound => DatabaseError::NotFound("row not found".into()),
        sqlx::Error::PoolTimedOut => {
            DatabaseError::Timeout("postgres connection pool timed out".into())
        }
        sqlx::Error::PoolClosed => {
            DatabaseError::Unavailable("postgres connection pool is closed".into())
        }
        sqlx::Error::Io(error) => {
            DatabaseError::Unavailable(format!("postgres I/O error: {error}"))
        }
        sqlx::Error::Tls(error) => {
            DatabaseError::Unavailable(format!("postgres TLS error: {error}"))
        }
        sqlx::Error::Protocol(message) => {
            DatabaseError::Storage(format!("postgres protocol error: {message}"))
        }
        sqlx::Error::Configuration(message) => {
            DatabaseError::Validation(format!("postgres configuration error: {message}"))
        }
        sqlx::Error::Database(error) => map_postgres_database_error(error.as_ref()),
        other => DatabaseError::Storage(format!("postgres error: {other}")),
    }
}

pub(crate) fn map_postgres_error_with_context(
    context: impl AsRef<str>,
    error: sqlx::Error,
) -> DatabaseError {
    map_postgres_error(error).with_context(context)
}

fn map_postgres_database_error(error: &(dyn SqlxDatabaseError + 'static)) -> DatabaseError {
    let message = describe_database_error(error);
    match error.kind() {
        ErrorKind::UniqueViolation => DatabaseError::Duplicate(message),
        ErrorKind::ForeignKeyViolation => DatabaseError::ForeignKeyViolation(message),
        ErrorKind::NotNullViolation | ErrorKind::CheckViolation => {
            DatabaseError::ConstraintViolation(message)
        }
        ErrorKind::Other => match error.code().as_deref() {
            Some("40001") | Some("40P01") => DatabaseError::Retryable(message),
            Some("57014") => DatabaseError::Timeout(message),
            Some(code) if code.starts_with("08") => DatabaseError::Unavailable(message),
            Some(code) if code.starts_with("22") => DatabaseError::Validation(message),
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
