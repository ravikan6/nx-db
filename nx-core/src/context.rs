#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Context {
    namespace: String,
    database: String,
    schema: String,
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
