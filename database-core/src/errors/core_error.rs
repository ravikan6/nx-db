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

    #[error("resource conflict: {0}")]
    Conflict(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("{0}")]
    Other(String),
}
