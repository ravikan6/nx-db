use crate::utils::SqliteUtils;
use database_core::errors::DatabaseError;
use database_core::query::{Filter, FilterOp};
use database_core::traits::storage::StorageValue;
use database_core::{COLUMN_SEQUENCE, CollectionSchema};
use sqlx::{QueryBuilder, Sqlite};

pub struct SqliteQuery;

impl SqliteQuery {
    pub fn push_bind_value(builder: &mut QueryBuilder<'_, Sqlite>, value: &StorageValue) {
        match value {
            StorageValue::Null => {
                builder.push_bind(Option::<String>::None);
            }
            StorageValue::Bool(v) => {
                builder.push_bind(if *v { 1i64 } else { 0i64 });
            }
            StorageValue::Int(v) => {
                builder.push_bind(*v);
            }
            StorageValue::Float(v) => {
                builder.push_bind(*v);
            }
            StorageValue::String(v) | StorageValue::Json(v) | StorageValue::Enum(v) => {
                builder.push_bind(v.clone());
            }
            StorageValue::Timestamp(v) => {
                builder.push_bind(
                    v.format(&time::format_description::well_known::Rfc3339)
                        .unwrap(),
                );
            }
            StorageValue::BoolArray(v) => {
                builder.push_bind(serde_json::to_string(v).unwrap());
            }
            StorageValue::IntArray(v) => {
                builder.push_bind(serde_json::to_string(v).unwrap());
            }
            StorageValue::FloatArray(v) => {
                builder.push_bind(serde_json::to_string(v).unwrap());
            }
            StorageValue::StringArray(v) | StorageValue::EnumArray(v) => {
                builder.push_bind(serde_json::to_string(v).unwrap());
            }
            StorageValue::TimestampArray(v) => {
                let strings: Vec<String> = v
                    .iter()
                    .map(|dt| {
                        dt.format(&time::format_description::well_known::Rfc3339)
                            .unwrap()
                    })
                    .collect();
                builder.push_bind(serde_json::to_string(&strings).unwrap());
            }
            StorageValue::Bytes(v) => {
                builder.push_bind(v.clone());
            }
        }
    }

    pub fn push_bind_value_separated(
        sep: &mut sqlx::query_builder::Separated<'_, '_, Sqlite, &str>,
        value: &StorageValue,
    ) {
        match value {
            StorageValue::Null => {
                sep.push_bind(Option::<String>::None);
            }
            StorageValue::Bool(v) => {
                sep.push_bind(if *v { 1i64 } else { 0i64 });
            }
            StorageValue::Int(v) => {
                sep.push_bind(*v);
            }
            StorageValue::Float(v) => {
                sep.push_bind(*v);
            }
            StorageValue::String(v) | StorageValue::Json(v) | StorageValue::Enum(v) => {
                sep.push_bind(v.clone());
            }
            StorageValue::Timestamp(v) => {
                sep.push_bind(
                    v.format(&time::format_description::well_known::Rfc3339)
                        .unwrap(),
                );
            }
            StorageValue::BoolArray(v) => {
                sep.push_bind(serde_json::to_string(v).unwrap());
            }
            StorageValue::IntArray(v) => {
                sep.push_bind(serde_json::to_string(v).unwrap());
            }
            StorageValue::FloatArray(v) => {
                sep.push_bind(serde_json::to_string(v).unwrap());
            }
            StorageValue::StringArray(v) | StorageValue::EnumArray(v) => {
                sep.push_bind(serde_json::to_string(v).unwrap());
            }
            StorageValue::TimestampArray(v) => {
                let strings: Vec<String> = v
                    .iter()
                    .map(|dt| {
                        dt.format(&time::format_description::well_known::Rfc3339)
                            .unwrap()
                    })
                    .collect();
                sep.push_bind(serde_json::to_string(&strings).unwrap());
            }
            StorageValue::Bytes(v) => {
                sep.push_bind(v.clone());
            }
        }
    }

    pub fn push_filter(
        builder: &mut QueryBuilder<'_, Sqlite>,
        schema: &'static CollectionSchema,
        filter: &Filter,
    ) -> Result<(), DatabaseError> {
        Self::push_filter_for_alias(builder, schema, filter, None)
    }

    pub fn push_filter_for_alias(
        builder: &mut QueryBuilder<'_, Sqlite>,
        schema: &'static CollectionSchema,
        filter: &Filter,
        alias: Option<&str>,
    ) -> Result<(), DatabaseError> {
        match filter {
            Filter::Field { field, op } => {
                let column = SqliteUtils::qualified_column_for_field(schema, field, alias)?;

                match op {
                    FilterOp::Eq(StorageValue::Null) | FilterOp::IsNull => {
                        builder.push(format!("{column} IS NULL"));
                    }
                    FilterOp::NotEq(StorageValue::Null) | FilterOp::IsNotNull => {
                        builder.push(format!("{column} IS NOT NULL"));
                    }
                    FilterOp::Eq(v) => {
                        builder.push(format!("{column} = "));
                        Self::push_bind_value(builder, v);
                    }
                    FilterOp::NotEq(v) => {
                        builder.push(format!("{column} <> "));
                        Self::push_bind_value(builder, v);
                    }
                    FilterOp::Gt(v) => {
                        builder.push(format!("{column} > "));
                        Self::push_bind_value(builder, v);
                    }
                    FilterOp::Gte(v) => {
                        builder.push(format!("{column} >= "));
                        Self::push_bind_value(builder, v);
                    }
                    FilterOp::Lt(v) => {
                        builder.push(format!("{column} < "));
                        Self::push_bind_value(builder, v);
                    }
                    FilterOp::Lte(v) => {
                        builder.push(format!("{column} <= "));
                        Self::push_bind_value(builder, v);
                    }
                    FilterOp::In(vals) => {
                        if vals.is_empty() {
                            builder.push("0 = 1");
                        } else {
                            builder.push(format!("{column} IN ("));
                            let mut first = true;
                            for v in vals {
                                if !first {
                                    builder.push(", ");
                                }
                                first = false;
                                Self::push_bind_value(builder, v);
                            }
                            builder.push(")");
                        }
                    }
                    FilterOp::Contains(v) => {
                        builder.push(format!("{column} LIKE '%' || "));
                        Self::push_bind_value(builder, v);
                        builder.push(" || '%'");
                    }
                    FilterOp::StartsWith(v) => {
                        builder.push(format!("{column} LIKE "));
                        Self::push_bind_value(builder, v);
                        builder.push(" || '%'");
                    }
                    FilterOp::EndsWith(v) => {
                        builder.push(format!("{column} LIKE '%' || "));
                        Self::push_bind_value(builder, v);
                    }
                    FilterOp::TextSearch(StorageValue::String(value))
                    | FilterOp::TextSearch(StorageValue::Json(value)) => {
                        let terms: Vec<&str> = value.split_whitespace().collect();
                        if terms.is_empty() {
                            builder.push("1 = 1");
                        } else {
                            builder.push("(");
                            let mut first = true;
                            for term in terms {
                                if !first {
                                    builder.push(" AND ");
                                }
                                first = false;
                                builder.push(format!("{column} LIKE '%' || "));
                                builder.push_bind(term.to_string());
                                builder.push(" || '%'");
                            }
                            builder.push(")");
                        }
                    }
                    _ => {
                        return Err(DatabaseError::Other(
                            "unsupported filter op for sqlite".into(),
                        ));
                    }
                }
            }
            Filter::And(filters) => {
                if filters.is_empty() {
                    builder.push("1 = 1");
                } else {
                    builder.push("(");
                    let mut first = true;
                    for f in filters {
                        if !first {
                            builder.push(" AND ");
                        }
                        first = false;
                        Self::push_filter_for_alias(builder, schema, f, alias)?;
                    }
                    builder.push(")");
                }
            }
            Filter::Or(filters) => {
                if filters.is_empty() {
                    builder.push("0 = 1");
                } else {
                    builder.push("(");
                    let mut first = true;
                    for f in filters {
                        if !first {
                            builder.push(" OR ");
                        }
                        first = false;
                        Self::push_filter_for_alias(builder, schema, f, alias)?;
                    }
                    builder.push(")");
                }
            }
            Filter::Not(f) => {
                builder.push("NOT (");
                Self::push_filter_for_alias(builder, schema, f, alias)?;
                builder.push(")");
            }
        }
        Ok(())
    }

    pub fn push_condition_separator(
        builder: &mut QueryBuilder<'_, Sqlite>,
        has_conditions: &mut bool,
    ) {
        if *has_conditions {
            builder.push(" AND ");
        } else {
            builder.push(" WHERE ");
            *has_conditions = true;
        }
    }

    pub fn authorization_context(
        context: &database_core::Context,
    ) -> database_core::utils::AuthorizationContext {
        let roles = context
            .roles()
            .cloned()
            .collect::<Vec<database_core::utils::Role>>();
        if context.authorization_enabled() {
            database_core::utils::AuthorizationContext::enabled(roles)
        } else {
            database_core::utils::AuthorizationContext::disabled(roles)
        }
    }

    pub fn permission_roles<'p, I>(
        permissions: I,
        action: database_core::utils::PermissionEnum,
    ) -> Result<Vec<database_core::utils::Role>, DatabaseError>
    where
        I: IntoIterator<Item = &'p str>,
    {
        let mut roles = Vec::new();
        for perm_str in permissions {
            let perm = database_core::utils::Permission::parse(perm_str).map_err(|e| {
                DatabaseError::Other(format!("invalid permission '{perm_str}': {e}"))
            })?;
            let matches = match (perm.permission(), action) {
                (
                    database_core::utils::PermissionEnum::Write,
                    database_core::utils::PermissionEnum::Create,
                )
                | (
                    database_core::utils::PermissionEnum::Write,
                    database_core::utils::PermissionEnum::Update,
                )
                | (
                    database_core::utils::PermissionEnum::Write,
                    database_core::utils::PermissionEnum::Delete,
                ) => true,
                (c, t) => c == t,
            };
            if matches {
                roles.push(perm.role_instance().clone());
            }
        }
        Ok(roles)
    }

    pub fn document_action_roles(
        context: &database_core::Context,
        schema: &'static CollectionSchema,
        action: database_core::utils::PermissionEnum,
    ) -> Result<Option<Vec<String>>, DatabaseError> {
        let auth_ctx = Self::authorization_context(context);
        let collection_roles = Self::permission_roles(schema.permissions.iter().copied(), action)?;
        match database_core::utils::Authorization::new(action, &auth_ctx)
            .validate(&collection_roles)
        {
            Ok(()) => Ok(None),
            Err(e) if schema.document_security => match DatabaseError::from(e) {
                DatabaseError::Authorization(_) => Ok(Some(
                    auth_ctx
                        .roles()
                        .into_iter()
                        .map(|r| r.to_string())
                        .collect(),
                )),
                other => Err(other),
            },
            Err(e) => Err(DatabaseError::from(e)),
        }
    }

    pub fn push_document_action_condition(
        builder: &mut QueryBuilder<'_, Sqlite>,
        context: &database_core::Context,
        schema: &'static CollectionSchema,
        alias: &str,
        action: database_core::utils::PermissionEnum,
        has_conditions: &mut bool,
    ) -> Result<(), DatabaseError> {
        if Self::document_action_roles(context, schema, action)?.is_none() {
            return Ok(());
        }
        Self::push_condition_separator(builder, has_conditions);
        Self::push_document_action_expression(builder, context, schema, alias, action)?;
        Ok(())
    }

    pub fn push_document_action_expression(
        builder: &mut QueryBuilder<'_, Sqlite>,
        context: &database_core::Context,
        schema: &'static CollectionSchema,
        alias: &str,
        action: database_core::utils::PermissionEnum,
    ) -> Result<(), DatabaseError> {
        let Some(roles) = Self::document_action_roles(context, schema, action)? else {
            builder.push("1 = 1");
            return Ok(());
        };
        let perms_table = SqliteUtils::qualified_permissions_table_name(context, schema.id);
        let alias_quoted = SqliteUtils::quote_identifier(alias);

        builder.push("EXISTS (SELECT 1 FROM ");
        builder.push(perms_table);
        builder.push(" AS p WHERE p.document_id = ");
        builder.push(alias_quoted);
        builder.push(".");
        builder.push(COLUMN_SEQUENCE);
        builder.push(" AND p.permission_type = ");
        builder.push_bind(action.to_string());
        builder.push(" AND EXISTS (SELECT 1 FROM json_each(p.permissions) WHERE value IN (");

        if roles.is_empty() {
            builder.push("NULL");
        } else {
            let mut first = true;
            for role in roles {
                if !first {
                    builder.push(", ");
                }
                first = false;
                builder.push_bind(role);
            }
        }
        builder.push(")))");
        Ok(())
    }
}
