use std::fmt::{Display, Formatter};
use std::str::FromStr;

use crate::errors::PermissionError;
use super::{Role, RoleName};

pub const PERMISSIONS: [PermissionEnum; 5] = [
    PermissionEnum::Read,
    PermissionEnum::Create,
    PermissionEnum::Update,
    PermissionEnum::Delete,
    PermissionEnum::Write,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PermissionEnum {
    Read,
    Create,
    Update,
    Delete,
    Write,
}

impl Display for PermissionEnum {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            PermissionEnum::Read => "read",
            PermissionEnum::Create => "create",
            PermissionEnum::Update => "update",
            PermissionEnum::Delete => "delete",
            PermissionEnum::Write => "write",
        };

        write!(f, "{value}")
    }
}

impl FromStr for PermissionEnum {
    type Err = PermissionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "read" => Ok(PermissionEnum::Read),
            "create" => Ok(PermissionEnum::Create),
            "update" => Ok(PermissionEnum::Update),
            "delete" => Ok(PermissionEnum::Delete),
            "write" => Ok(PermissionEnum::Write),
            _ => Err(PermissionError::invalid_permission_type(s)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Permission {
    permission: PermissionEnum,
    role: Role,
}

impl Permission {
    pub fn new(permission: PermissionEnum, role: Role) -> Self {
        Self { permission, role }
    }

    pub fn permission(&self) -> PermissionEnum {
        self.permission
    }

    pub fn role_instance(&self) -> &Role {
        &self.role
    }

    pub fn role(&self) -> RoleName {
        self.role.role()
    }

    pub fn identifier(&self) -> Option<&str> {
        self.role.identifier()
    }

    pub fn dimension(&self) -> Option<&str> {
        self.role.dimension()
    }

    pub fn to_object(&self) -> String {
        self.to_string()
    }

    pub fn to_json(&self) -> String {
        self.to_string()
    }

    pub fn parse(permission_string: &str) -> Result<Self, PermissionError> {
        if !(permission_string.ends_with("\")") && permission_string.contains("(\"")) {
            return Err(PermissionError::invalid_permission_string_format(
                permission_string,
            ));
        }

        let Some(open) = permission_string.find("(\"") else {
            return Err(PermissionError::invalid_permission_string_format(
                permission_string,
            ));
        };

        if permission_string.len() < open + 4 {
            return Err(PermissionError::invalid_permission_string_format(
                permission_string,
            ));
        }

        let permission_name = &permission_string[..open];
        let role_string = &permission_string[(open + 2)..(permission_string.len() - 2)];

        let permission = PermissionEnum::from_str(permission_name)?;
        if !Self::is_valid_permission(permission) {
            return Err(PermissionError::invalid_permission_type(permission_name));
        }

        let role = Role::parse(role_string)?;
        Ok(Self::new(permission, role))
    }

    pub fn aggregate(
        permissions: Option<&[String]>,
        allowed: &[PermissionEnum],
    ) -> Option<Vec<String>> {
        let permissions = permissions?;

        let mut aggregated_permissions = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for permission in permissions {
            let Ok(parsed_permission) = Self::parse(permission) else {
                continue;
            };

            let permission_name = parsed_permission.permission();
            let role = parsed_permission.role_instance().clone();

            if let Some(sub_types) = Self::aggregate_types(permission_name) {
                for sub_type in sub_types {
                    if allowed.contains(sub_type) {
                        let value = Permission::new(*sub_type, role.clone()).to_string();
                        if seen.insert(value.clone()) {
                            aggregated_permissions.push(value);
                        }
                    }
                }
            } else {
                let value = parsed_permission.to_string();
                if seen.insert(value.clone()) {
                    aggregated_permissions.push(value);
                }
            }
        }

        Some(aggregated_permissions)
    }

    pub fn aggregate_default(permissions: Option<&[String]>) -> Option<Vec<String>> {
        Self::aggregate(permissions, &PERMISSIONS)
    }

    pub fn read(role: Role) -> Self {
        Self::new(PermissionEnum::Read, role)
    }

    pub fn create(role: Role) -> Self {
        Self::new(PermissionEnum::Create, role)
    }

    pub fn update(role: Role) -> Self {
        Self::new(PermissionEnum::Update, role)
    }

    pub fn delete(role: Role) -> Self {
        Self::new(PermissionEnum::Delete, role)
    }

    pub fn write(role: Role) -> Self {
        Self::new(PermissionEnum::Write, role)
    }

    fn is_valid_permission(permission: PermissionEnum) -> bool {
        PERMISSIONS.contains(&permission) || Self::aggregate_types(permission).is_some()
    }

    fn aggregate_types(permission: PermissionEnum) -> Option<&'static [PermissionEnum]> {
        const WRITE_PERMS: [PermissionEnum; 3] = [
            PermissionEnum::Create,
            PermissionEnum::Update,
            PermissionEnum::Delete,
        ];

        match permission {
            PermissionEnum::Write => Some(&WRITE_PERMS),
            _ => None,
        }
    }
}

impl Display for Permission {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}(\"{}\")", self.permission, self.role)
    }
}

impl FromStr for Permission {
    type Err = PermissionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::{Permission, PermissionEnum};
    use crate::auth::Role;

    #[test]
    fn formats_permissions() {
        let role = Role::parse("user:abc").expect("role should parse");
        let permission = Permission::read(role);
        assert_eq!(permission.to_string(), "read(\"user:abc\")");
    }

    #[test]
    fn parses_permissions() {
        let permission = Permission::parse("write(\"team:engineering/admin\")")
            .expect("permission should parse");
        assert_eq!(permission.permission(), PermissionEnum::Write);
        assert_eq!(permission.role().to_string(), "team");
        assert_eq!(permission.identifier(), Some("engineering"));
        assert_eq!(permission.dimension(), Some("admin"));
    }

    #[test]
    fn aggregates_permissions() {
        let list = vec![
            "write(\"users/verified\")".to_string(),
            "read(\"users/verified\")".to_string(),
        ];

        let aggregated = Permission::aggregate_default(Some(&list)).expect("should aggregate");
        assert!(aggregated.contains(&"create(\"users/verified\")".to_string()));
        assert!(aggregated.contains(&"update(\"users/verified\")".to_string()));
        assert!(aggregated.contains(&"delete(\"users/verified\")".to_string()));
        assert!(aggregated.contains(&"read(\"users/verified\")".to_string()));
    }
}
