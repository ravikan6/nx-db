use std::error::Error;
use std::fmt::{Display, Formatter};

use super::RoleError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PermissionErrorKind {
    InvalidPermissionType { permission: String },
    InvalidPermissionStringFormat { input: String },
    RoleParsingFailed { source: RoleError },
    Other { message: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionError {
    kind: PermissionErrorKind,
}

impl PermissionError {
    pub fn kind(&self) -> &PermissionErrorKind {
        &self.kind
    }

    pub fn from_kind(kind: PermissionErrorKind) -> Self {
        Self { kind }
    }

    pub fn new(message: impl Into<String>) -> Self {
        Self::from_kind(PermissionErrorKind::Other {
            message: message.into(),
        })
    }

    pub fn invalid_permission_type(permission: &str) -> Self {
        Self::from_kind(PermissionErrorKind::InvalidPermissionType {
            permission: permission.to_string(),
        })
    }

    pub fn invalid_permission_string_format(input: &str) -> Self {
        Self::from_kind(PermissionErrorKind::InvalidPermissionStringFormat {
            input: input.to_string(),
        })
    }

    pub fn role_parsing_failed(source: RoleError) -> Self {
        Self::from_kind(PermissionErrorKind::RoleParsingFailed { source })
    }
}

impl Display for PermissionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let message = match &self.kind {
            PermissionErrorKind::InvalidPermissionType { permission } => {
                format!("Invalid permission type: \"{permission}\".")
            }
            PermissionErrorKind::InvalidPermissionStringFormat { input } => format!(
                "Invalid permission string format: \"{input}\". Expected \"permission(\\\"role:id/dim\\\")\"."
            ),
            PermissionErrorKind::RoleParsingFailed { source } => {
                format!("Failed to parse role from permission string: {source}")
            }
            PermissionErrorKind::Other { message } => message.clone(),
        };

        write!(f, "{message}")
    }
}

impl Error for PermissionError {}

impl From<RoleError> for PermissionError {
    fn from(value: RoleError) -> Self {
        Self::role_parsing_failed(value)
    }
}
