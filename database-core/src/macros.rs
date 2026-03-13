#[macro_export]
macro_rules! db_context {
    (schema: $schema:expr) => {
        $crate::Context::default().with_schema($schema)
    };
    (role: $role:expr) => {
        $crate::Context::default().with_role($role)
    };
    (schema: $schema:expr, role: $role:expr) => {
        $crate::Context::default()
            .with_schema($schema)
            .with_role($role)
    };
    (tenant: $tenant:expr) => {
        $crate::Context::default().with_tenant_id($tenant)
    };
}

#[macro_export]
macro_rules! db_registry {
    ($($schema:expr),* $(,)?) => {
        {
            let mut registry = $crate::StaticRegistry::new();
            $(
                registry = registry.register($schema).expect("failed to register schema");
            )*
            registry
        }
    };
}

#[macro_export]
macro_rules! db_query {
    () => {
        $crate::QuerySpec::new()
    };
    ($($key:ident : $val:expr),* $(,)?) => {
        {
            let mut query = $crate::QuerySpec::new();
            $(
                query = $crate::db_query!(@apply query, $key, $val);
            )*
            query
        }
    };
    (@apply $query:ident, filter, $val:expr) => {
        $query.filter($val.into())
    };
    (@apply $query:ident, sort, $val:expr) => {
        $query.sort($val)
    };
    (@apply $query:ident, limit, $val:expr) => {
        $query.limit($val)
    };
    (@apply $query:ident, offset, $val:expr) => {
        $query.offset($val)
    };
}

#[macro_export]
macro_rules! db_insert {
    ($model:ty { $($field:ident : $val:expr),* $(,)? }) => {
        {
            use $crate::Model;
            // This assumes Create struct fields match rust_field_name results
            type Create = <$model as Model>::Create;
            Create {
                $($field: $val,)*
            }
        }
    };
}

#[macro_export]
macro_rules! db_update {
    ($model:ty { $($field:ident : $val:expr),* $(,)? }) => {
        {
            use $crate::{Model, Patch};
            type Update = <$model as Model>::Update;
            Update {
                $($field: Patch::Set($val),)*
                ..Default::default()
            }
        }
    };
}
