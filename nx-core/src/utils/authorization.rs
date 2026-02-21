use std::collections::HashSet;

use crate::errors::AuthorizationError;

use super::{PermissionEnum, Role};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationContext {
    roles: HashSet<Role>,
    status: bool,
}

impl AuthorizationContext {
    pub fn new<I, S>(roles: I, status: bool) -> Self
    where
        I: IntoIterator<Item=S>,
        S: Into<Role>,
    {
        let mut roles: HashSet<Role> = roles.into_iter().map(Into::into).collect();
        roles.insert(Role::any());
        Self { roles, status }
    }

    pub fn enabled<I, S>(roles: I) -> Self
    where
        I: IntoIterator<Item=S>,
        S: Into<Role>,
    {
        Self::new(roles, true)
    }

    pub fn disabled<I, S>(roles: I) -> Self
    where
        I: IntoIterator<Item=S>,
        S: Into<Role>,
    {
        Self::new(roles, false)
    }

    pub fn empty() -> Self {
        Self::enabled(std::iter::empty::<Role>())
    }

    pub fn with_role(mut self, role: Role) -> Self {
        self.roles.insert(role);
        self
    }

    pub fn without_role(mut self, role: &Role) -> Self {
        if role != &Role::any() {
            self.roles.remove(role);
        }
        self
    }

    pub fn with_status(mut self, status: bool) -> Self {
        self.status = status;
        self
    }

    pub fn is_role(&self, role: &Role) -> bool {
        self.roles.contains(role)
    }

    pub fn roles(&self) -> Vec<Role> {
        let mut roles: Vec<_> = self.roles.iter().cloned().collect();
        roles.sort();
        roles
    }

    pub fn status(&self) -> bool {
        self.status
    }
}

#[derive(Debug, Clone)]
pub struct Authorization<'a> {
    action: PermissionEnum,
    context: &'a AuthorizationContext,
}

impl<'a> Authorization<'a> {
    pub fn new(action: PermissionEnum, context: &'a AuthorizationContext) -> Self {
        Self { action, context }
    }

    pub fn validate(&self, permissions: &[Role]) -> Result<(), AuthorizationError> {
        if !self.context.status() {
            return Ok(());
        }

        if permissions.is_empty() {
            return Err(AuthorizationError::no_permissions_provided(self.action));
        }

        for permission in permissions {
            if self.context.is_role(permission) {
                return Ok(());
            }
        }

        let mut allowed_scopes = permissions.to_vec();
        allowed_scopes.sort();

        let configured_roles = self.context.roles();
        let last_permission = permissions.last().cloned().unwrap();

        Err(AuthorizationError::missing_permission(
            self.action,
            last_permission,
            allowed_scopes,
            configured_roles,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{Authorization, AuthorizationContext};
    use crate::errors::AuthorizationErrorKind;
    use crate::utils::{PermissionEnum, Role};

    #[test]
    fn context_authorizes() {
        let ctx = AuthorizationContext::empty()
            .with_role(Role::user("abc", None).unwrap())
            .with_status(true);

        let auth = Authorization::new(PermissionEnum::Read, &ctx);
        let ok = auth.validate(&[Role::user("abc", None).unwrap()]);
        assert!(ok.is_ok());
    }

    #[test]
    fn contexts_are_isolated() {
        let ctx_one = AuthorizationContext::empty().with_role(Role::label("ctx-role").unwrap());
        let ctx_two = AuthorizationContext::empty().with_role(Role::label("other-role").unwrap());

        let auth = Authorization::new(PermissionEnum::Read, &ctx_one);
        assert!(auth.validate(&[Role::label("ctx-role").unwrap()]).is_ok());
        assert!(
            auth.validate(&[Role::label("other-role").unwrap()])
                .is_err()
        );

        let auth_two = Authorization::new(PermissionEnum::Read, &ctx_two);
        assert!(
            auth_two
                .validate(&[Role::label("other-role").unwrap()])
                .is_ok()
        );
    }

    #[test]
    fn role_struct_is_supported_for_with_role() {
        let role = Role::parse("user:alice").expect("role should parse");
        let ctx = AuthorizationContext::empty().with_role(role);
        let roles: Vec<String> = ctx.roles().iter().map(|i| i.to_string()).collect();
        assert_eq!(roles, vec!["any".to_string(), "user:alice".to_string()]);
    }

    #[test]
    fn validate_returns_no_permissions_error() {
        let ctx = AuthorizationContext::empty().with_role(Role::user("abc", None).unwrap());
        let auth = Authorization::new(PermissionEnum::Read, &ctx);
        let err = auth.validate(&[]).expect_err("should fail");

        assert!(matches!(
            err.kind(),
            AuthorizationErrorKind::NoPermissionsProvided { .. }
        ));
    }

    #[test]
    fn any_role_is_always_present() {
        let ctx = AuthorizationContext::new([Role::user("abc", None).unwrap()], true)
            .without_role(&Role::user("abc", None).unwrap());
        assert!(ctx.is_role(&Role::any()));
    }
}
