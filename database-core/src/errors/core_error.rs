use super::{AuthorizationError, PermissionError, RoleError};
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DatabaseError {
    #[error("role error: {0}")]
    Role(#[from] RoleError),

    #[error("permission error: {0}")]
    Permission(#[from] PermissionError),

    #[error("authorization error: {0}")]
    Authorization(#[from] AuthorizationError),

    #[error("validation error: {0}")]
    Validation(String),

    #[error("resource not found: {0}")]
    NotFound(String),

    #[error("duplicate resource: {0}")]
    Duplicate(String),

    #[error("resource conflict: {0}")]
    Conflict(String),

    #[error("foreign key violation: {0}")]
    ForeignKeyViolation(String),

    #[error("constraint violation: {0}")]
    ConstraintViolation(String),

    #[error("retryable storage error: {0}")]
    Retryable(String),

    #[error("storage timeout: {0}")]
    Timeout(String),

    #[error("storage unavailable: {0}")]
    Unavailable(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("{0}")]
    Other(String),
}

impl DatabaseError {
    pub fn with_context(self, context: impl AsRef<str>) -> Self {
        let context = context.as_ref();
        if context.is_empty() {
            return self;
        }

        match self {
            Self::Role(error) => Self::Other(format!("{context}: {error}")),
            Self::Permission(error) => Self::Other(format!("{context}: {error}")),
            Self::Authorization(error) => Self::Other(format!("{context}: {error}")),
            Self::Validation(message) => Self::Validation(format!("{context}: {message}")),
            Self::NotFound(message) => Self::NotFound(format!("{context}: {message}")),
            Self::Duplicate(message) => Self::Duplicate(format!("{context}: {message}")),
            Self::Conflict(message) => Self::Conflict(format!("{context}: {message}")),
            Self::ForeignKeyViolation(message) => {
                Self::ForeignKeyViolation(format!("{context}: {message}"))
            }
            Self::ConstraintViolation(message) => {
                Self::ConstraintViolation(format!("{context}: {message}"))
            }
            Self::Retryable(message) => Self::Retryable(format!("{context}: {message}")),
            Self::Timeout(message) => Self::Timeout(format!("{context}: {message}")),
            Self::Unavailable(message) => Self::Unavailable(format!("{context}: {message}")),
            Self::Storage(message) => Self::Storage(format!("{context}: {message}")),
            Self::Other(message) => Self::Other(format!("{context}: {message}")),
        }
    }

    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Retryable(_) | Self::Timeout(_) | Self::Unavailable(_)
        )
    }
}
