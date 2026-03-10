use crate::utils::Role;
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Context {
    namespace: String,
    database: String,
    schema: String,
    shared_tables: bool,
    tenant_id: Option<String>,
    tenant_per_document: bool,
    authorization_enabled: bool,
    roles: BTreeSet<Role>,
}

impl Context {
    pub fn new(
        namespace: impl Into<String>,
        database: impl Into<String>,
        schema: impl Into<String>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            database: database.into(),
            schema: schema.into(),
            shared_tables: false,
            tenant_id: None,
            tenant_per_document: false,
            authorization_enabled: true,
            roles: BTreeSet::new(),
        }
    }

    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = database.into();
        self
    }

    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = schema.into();
        self
    }

    pub fn with_shared_tables(mut self, shared_tables: bool) -> Self {
        self.shared_tables = shared_tables;
        self
    }

    pub fn with_tenant_id(mut self, tenant_id: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant_id.into());
        self
    }

    pub fn without_tenant_id(mut self) -> Self {
        self.tenant_id = None;
        self
    }

    pub fn with_tenant_per_document(mut self, tenant_per_document: bool) -> Self {
        self.tenant_per_document = tenant_per_document;
        self
    }

    pub fn with_authorization(mut self, enabled: bool) -> Self {
        self.authorization_enabled = enabled;
        self
    }

    pub fn with_role(mut self, role: Role) -> Self {
        self.roles.insert(role);
        self
    }

    pub fn with_roles<I>(mut self, roles: I) -> Self
    where
        I: IntoIterator<Item = Role>,
    {
        self.roles.extend(roles);
        self
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub fn schema(&self) -> &str {
        &self.schema
    }

    pub fn shared_tables(&self) -> bool {
        self.shared_tables
    }

    pub fn tenant_id(&self) -> Option<&str> {
        self.tenant_id.as_deref()
    }

    pub fn tenant_per_document(&self) -> bool {
        self.tenant_per_document
    }

    pub fn authorization_enabled(&self) -> bool {
        self.authorization_enabled
    }

    pub fn roles(&self) -> impl Iterator<Item = &Role> {
        self.roles.iter()
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::new("", "", "public")
    }
}
