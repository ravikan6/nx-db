#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Context {
    namespace: String,
    database: String,
    schema: String,
    shared_tables: bool,
    tenant_id: Option<String>,
    tenant_per_document: bool,

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
        }
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
}
