use std::error::Error;
use std::fmt::{Display, Formatter};

use crate::auth::{PermissionEnum, Role};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorizationErrorKind {
    NoPermissionsProvided {
        action: String,
    },
    MissingPermission {
        action: String,
        last_permission: String,
        allowed_scopes: Vec<String>,
        authorized_roles: Vec<Role>,
    },
    Other {
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationError {
    kind: AuthorizationErrorKind,
}

impl AuthorizationError {
    pub fn kind(&self) -> &AuthorizationErrorKind {
        &self.kind
    }

    pub fn from_kind(kind: AuthorizationErrorKind) -> Self {
        Self { kind }
    }

    pub fn new(message: impl Into<String>) -> Self {
        Self::from_kind(AuthorizationErrorKind::Other {
            message: message.into(),
        })
    }

    pub fn no_permissions_provided(action: PermissionEnum) -> Self {
        Self::from_kind(AuthorizationErrorKind::NoPermissionsProvided {
            action: action.to_string(),
        })
    }

    pub fn missing_permission(
        action: PermissionEnum,
        last_permission: String,
        allowed_scopes: Vec<String>,
        authorized_roles: Vec<Role>,
    ) -> Self {
        Self::from_kind(AuthorizationErrorKind::MissingPermission {
            action: action.to_string(),
            last_permission,
            allowed_scopes,
            authorized_roles,
        })
    }
}

impl Display for AuthorizationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let message = match &self.kind {
            AuthorizationErrorKind::NoPermissionsProvided { action } => {
                format!("No permissions provided for action '{action}'.")
            }
            AuthorizationErrorKind::MissingPermission {
                action,
                last_permission,
                allowed_scopes,
                authorized_roles,
            } => format!(
                "Missing \"{action}\" permission for role \"{last_permission}\". Only {:?} scopes are allowed and {:?} was given.",
                allowed_scopes, authorized_roles
            ),
            AuthorizationErrorKind::Other { message } => message.clone(),
        };

        write!(f, "{message}")
    }
}

impl Error for AuthorizationError {}
