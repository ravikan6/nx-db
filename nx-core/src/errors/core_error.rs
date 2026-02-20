use std::error::Error;
use std::fmt::{Display, Formatter};

use super::{AuthorizationError, PermissionError, RoleError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoreError {
    Role(RoleError),
    Permission(PermissionError),
    Authorization(AuthorizationError),
    Other(String),
}

impl Display for CoreError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CoreError::Role(error) => write!(f, "{error}"),
            CoreError::Permission(error) => write!(f, "{error}"),
            CoreError::Authorization(error) => write!(f, "{error}"),
            CoreError::Other(message) => write!(f, "{message}"),
        }
    }
}

impl Error for CoreError {}

impl From<RoleError> for CoreError {
    fn from(value: RoleError) -> Self {
        CoreError::Role(value)
    }
}

impl From<PermissionError> for CoreError {
    fn from(value: PermissionError) -> Self {
        CoreError::Permission(value)
    }
}

impl From<AuthorizationError> for CoreError {
    fn from(value: AuthorizationError) -> Self {
        CoreError::Authorization(value)
    }
}
