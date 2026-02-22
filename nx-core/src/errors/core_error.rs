use std::error::Error;
use std::fmt::{Display, Formatter};

use super::{AuthorizationError, PermissionError, RoleError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabaseError {
    Role(RoleError),
    Permission(PermissionError),
    Authorization(AuthorizationError),
    Other(String),
}

impl Display for DatabaseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DatabaseError::Role(error) => write!(f, "{error}"),
            DatabaseError::Permission(error) => write!(f, "{error}"),
            DatabaseError::Authorization(error) => write!(f, "{error}"),
            DatabaseError::Other(message) => write!(f, "{message}"),
        }
    }
}

impl Error for DatabaseError {}

impl From<RoleError> for DatabaseError {
    fn from(value: RoleError) -> Self {
        DatabaseError::Role(value)
    }
}

impl From<PermissionError> for DatabaseError {
    fn from(value: PermissionError) -> Self {
        DatabaseError::Permission(value)
    }
}

impl From<AuthorizationError> for DatabaseError {
    fn from(value: AuthorizationError) -> Self {
        DatabaseError::Authorization(value)
    }
}
