
use std::fmt::{Display, Formatter};
use std::str::FromStr;

use crate::errors::RoleError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoleName {
    Any,
    Guests,
    Users,
    User,
    Team,
    Member,
    Label,
}

impl Display for RoleName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            RoleName::Any => "any",
            RoleName::Guests => "guests",
            RoleName::Users => "users",
            RoleName::User => "user",
            RoleName::Team => "team",
            RoleName::Member => "member",
            RoleName::Label => "label",
        };

        write!(f, "{value}")
    }
}

impl FromStr for RoleName {
    type Err = RoleError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "any" => Ok(RoleName::Any),
            "guests" => Ok(RoleName::Guests),
            "users" => Ok(RoleName::Users),
            "user" => Ok(RoleName::User),
            "team" => Ok(RoleName::Team),
            "member" => Ok(RoleName::Member),
            "label" => Ok(RoleName::Label),
            _ => Err(RoleError::invalid_role_name(s)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserDimension {
    Verified,
    Unverified,
}

impl Display for UserDimension {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            UserDimension::Verified => "verified",
            UserDimension::Unverified => "unverified",
        };

        write!(f, "{value}")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Role {
    role: RoleName,
    identifier: Option<String>,
    dimension: Option<String>,
}

impl Role {
    pub fn new(role: RoleName, identifier: Option<String>, dimension: Option<String>) -> Self {
        let identifier = identifier.filter(|value| !value.is_empty());
        let dimension = dimension.filter(|value| !value.is_empty());

        Self {
            role,
            identifier,
            dimension,
        }
    }

    pub fn role(&self) -> RoleName {
        self.role
    }

    pub fn identifier(&self) -> Option<&str> {
        self.identifier.as_deref()
    }

    pub fn dimension(&self) -> Option<&str> {
        self.dimension.as_deref()
    }

    pub fn to_object(&self) -> String {
        self.to_string()
    }

    pub fn to_json(&self) -> String {
        self.to_string()
    }

    pub fn parse(role_string: &str) -> Result<Self, RoleError> {
        if role_string.trim().is_empty() {
            return Err(RoleError::empty_role_string());
        }

        let colon_split: Vec<&str> = role_string.split(':').collect();
        if colon_split.len() > 2 {
            return Err(RoleError::too_many_colons());
        }

        let mut role_name_part = colon_split[0];
        let mut identifier_part: Option<String> = None;
        let mut dimension_part: Option<String> = None;

        if colon_split.len() == 2 {
            let identifier_or_dimension_part = colon_split[1];
            let slash_split: Vec<&str> = identifier_or_dimension_part.split('/').collect();
            if slash_split.len() > 2 {
                return Err(RoleError::too_many_slashes_in_identifier_dimension());
            }

            if slash_split.len() == 2 {
                identifier_part = Some(slash_split[0].to_string());
                dimension_part = Some(slash_split[1].to_string());
            } else {
                identifier_part = Some(slash_split[0].to_string());
            }
        } else {
            let slash_split: Vec<&str> = role_name_part.split('/').collect();
            if slash_split.len() > 2 {
                return Err(RoleError::too_many_slashes_in_role_name());
            }

            if slash_split.len() == 2 {
                role_name_part = slash_split[0];
                dimension_part = Some(slash_split[1].to_string());
            }
        }

        if identifier_part.as_deref() == Some("") {
            identifier_part = None;
        }

        if dimension_part.as_deref() == Some("") {
            dimension_part = None;
        }

        if dimension_part.is_some() && identifier_part.is_none() && colon_split.len() == 2 {
            return Err(RoleError::dimension_after_empty_identifier());
        }

        let role_name = RoleName::from_str(role_name_part)?;
        if (role_name == RoleName::Any || role_name == RoleName::Guests)
            && (identifier_part.is_some() || dimension_part.is_some())
        {
            return Err(RoleError::any_or_guests_cannot_have_identifier_or_dimension(
                &role_name.to_string(),
            ));
        }

        Ok(Self::new(role_name, identifier_part, dimension_part))
    }

    pub fn user(identifier: impl Into<String>, status: Option<UserDimension>) -> Result<Self, RoleError> {
        let identifier = identifier.into();
        if identifier.is_empty() {
            return Err(RoleError::missing_identifier("User"));
        }

        Ok(Self::new(
            RoleName::User,
            Some(identifier),
            status.map(|value| value.to_string()),
        ))
    }

    pub fn users(status: Option<UserDimension>) -> Self {
        Self::new(RoleName::Users, None, status.map(|value| value.to_string()))
    }

    pub fn team(identifier: impl Into<String>, dimension: Option<String>) -> Result<Self, RoleError> {
        let identifier = identifier.into();
        if identifier.is_empty() {
            return Err(RoleError::missing_identifier("Team"));
        }

        Ok(Self::new(RoleName::Team, Some(identifier), dimension))
    }

    pub fn label(identifier: impl Into<String>) -> Result<Self, RoleError> {
        let identifier = identifier.into();
        if identifier.is_empty() {
            return Err(RoleError::missing_identifier("Label"));
        }

        Ok(Self::new(RoleName::Label, Some(identifier), None))
    }

    pub fn any() -> Self {
        Self::new(RoleName::Any, None, None)
    }

    pub fn guests() -> Self {
        Self::new(RoleName::Guests, None, None)
    }

    pub fn member(identifier: impl Into<String>) -> Result<Self, RoleError> {
        let identifier = identifier.into();
        if identifier.is_empty() {
            return Err(RoleError::missing_identifier("Member"));
        }

        Ok(Self::new(RoleName::Member, Some(identifier), None))
    }

    pub fn custom(role_name: RoleName, identifier: Option<String>, dimension: Option<String>) -> Self {
        Self::new(role_name, identifier, dimension)
    }
}

impl Display for Role {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.role)?;

        if let Some(identifier) = self.identifier() {
            write!(f, ":{identifier}")?;
        }

        if let Some(dimension) = self.dimension() {
            write!(f, "/{dimension}")?;
        }

        Ok(())
    }
}

impl FromStr for Role {
    type Err = RoleError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::{Role, RoleName, UserDimension};

    #[test]
    fn formats_roles() {
        assert_eq!(Role::any().to_string(), "any");
        assert_eq!(Role::users(Some(UserDimension::Verified)).to_string(), "users/verified");
        assert_eq!(
            Role::team("team_1", Some("admin".to_string()))
                .expect("team constructor should work")
                .to_string(),
            "team:team_1/admin"
        );
    }

    #[test]
    fn parses_roles() {
        let role = Role::parse("user:abc/verified").expect("role should parse");
        assert_eq!(role.role(), RoleName::User);
        assert_eq!(role.identifier(), Some("abc"));
        assert_eq!(role.dimension(), Some("verified"));

        let users = Role::parse("users/unverified").expect("role should parse");
        assert_eq!(users.role(), RoleName::Users);
        assert_eq!(users.identifier(), None);
        assert_eq!(users.dimension(), Some("unverified"));
    }

    #[test]
    fn rejects_invalid_roles() {
        assert!(Role::parse("").is_err());
        assert!(Role::parse("nope").is_err());
        assert!(Role::parse("any:value").is_err());
        assert!(Role::parse("user:/verified").is_err());
    }
}
