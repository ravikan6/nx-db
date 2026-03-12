use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoleErrorKind {
    EmptyRoleString,
    TooManyColons,
    TooManySlashesInIdentifierDimension,
    TooManySlashesInRoleName,
    DimensionAfterEmptyIdentifier,
    InvalidRoleName { role_name: String },
    AnyOrGuestsCannotHaveIdentifierOrDimension { role_name: String },
    MissingIdentifier { role_name: String },
    Other { message: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleError {
    kind: RoleErrorKind,
}

impl RoleError {
    pub fn kind(&self) -> &RoleErrorKind {
        &self.kind
    }

    pub fn from_kind(kind: RoleErrorKind) -> Self {
        Self { kind }
    }

    pub fn new(message: impl Into<String>) -> Self {
        Self::from_kind(RoleErrorKind::Other {
            message: message.into(),
        })
    }

    pub fn empty_role_string() -> Self {
        Self::from_kind(RoleErrorKind::EmptyRoleString)
    }

    pub fn too_many_colons() -> Self {
        Self::from_kind(RoleErrorKind::TooManyColons)
    }

    pub fn too_many_slashes_in_identifier_dimension() -> Self {
        Self::from_kind(RoleErrorKind::TooManySlashesInIdentifierDimension)
    }

    pub fn too_many_slashes_in_role_name() -> Self {
        Self::from_kind(RoleErrorKind::TooManySlashesInRoleName)
    }

    pub fn dimension_after_empty_identifier() -> Self {
        Self::from_kind(RoleErrorKind::DimensionAfterEmptyIdentifier)
    }

    pub fn invalid_role_name(role_name: &str) -> Self {
        Self::from_kind(RoleErrorKind::InvalidRoleName {
            role_name: role_name.to_string(),
        })
    }

    pub fn any_or_guests_cannot_have_identifier_or_dimension(role_name: &str) -> Self {
        Self::from_kind(RoleErrorKind::AnyOrGuestsCannotHaveIdentifierOrDimension {
            role_name: role_name.to_string(),
        })
    }

    pub fn missing_identifier(role_name: &str) -> Self {
        Self::from_kind(RoleErrorKind::MissingIdentifier {
            role_name: role_name.to_string(),
        })
    }
}

impl Display for RoleError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let message = match &self.kind {
            RoleErrorKind::EmptyRoleString => "Role string cannot be empty.".to_string(),
            RoleErrorKind::TooManyColons => "Invalid role format: too many colons.".to_string(),
            RoleErrorKind::TooManySlashesInIdentifierDimension => {
                "Invalid role format: too many slashes in identifier/dimension section.".to_string()
            }
            RoleErrorKind::TooManySlashesInRoleName => {
                "Invalid role format: too many slashes in roleName section.".to_string()
            }
            RoleErrorKind::DimensionAfterEmptyIdentifier => {
                "Invalid role format: dimension cannot follow an empty identifier with a colon."
                    .to_string()
            }
            RoleErrorKind::InvalidRoleName { role_name } => {
                format!("Invalid role name: \"{role_name}\".")
            }
            RoleErrorKind::AnyOrGuestsCannotHaveIdentifierOrDimension { role_name } => {
                format!("Role \"{role_name}\" cannot have an identifier or dimension.")
            }
            RoleErrorKind::MissingIdentifier { role_name } => {
                format!("{role_name} role must have an identifier.")
            }
            RoleErrorKind::Other { message } => message.clone(),
        };

        write!(f, "{message}")
    }
}

impl Error for RoleError {}
