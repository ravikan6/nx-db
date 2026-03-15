use database_core::errors::DatabaseError;
use database_core::query::{Filter, FilterOp, QuerySpec, SortDirection};
use database_core::{
    CollectionSchema, Context, Order,
};
use sqlx::{Postgres, QueryBuilder};
use crate::utils::PostgresUtils;
use database_core::traits::storage::StorageValue;
use database_core::utils::PermissionEnum;

pub struct PostgresQuery;

impl PostgresQuery {
    pub fn push_filters(builder: &mut QueryBuilder<'_, Postgres>, schema: &'static CollectionSchema, query: &QuerySpec, has_conditions: &mut bool) -> Result<(), DatabaseError> {
        if query.filters().is_empty() { return Ok(()); }
        Self::push_condition_separator(builder, has_conditions);
        let mut first = true;
        for filter in query.filters() {
            if !first { builder.push(" AND "); }
            first = false;
            Self::push_filter(builder, schema, filter)?;
        }
        Ok(())
    }

    pub fn push_condition_separator(builder: &mut QueryBuilder<'_, Postgres>, has_conditions: &mut bool) {
        if *has_conditions { builder.push(" AND "); }
        else { builder.push(" WHERE "); *has_conditions = true; }
    }

    pub fn push_filter(builder: &mut QueryBuilder<'_, Postgres>, schema: &'static CollectionSchema, filter: &Filter) -> Result<(), DatabaseError> {
        match filter {
            Filter::Field { field, op } => {
                let column = PostgresUtils::column_for_field(schema, field)?;
                match op {
                    FilterOp::Eq(StorageValue::Null) | FilterOp::IsNull => { builder.push(format!("{column} IS NULL")); }
                    FilterOp::NotEq(StorageValue::Null) | FilterOp::IsNotNull => { builder.push(format!("{column} IS NOT NULL")); }
                    FilterOp::Eq(v) => { builder.push(format!("{column} = ")); Self::push_bind_value(builder, v); }
                    FilterOp::NotEq(v) => { builder.push(format!("{column} <> ")); Self::push_bind_value(builder, v); }
                    FilterOp::Gt(v) => { builder.push(format!("{column} > ")); Self::push_bind_value(builder, v); }
                    FilterOp::Gte(v) => { builder.push(format!("{column} >= ")); Self::push_bind_value(builder, v); }
                    FilterOp::Lt(v) => { builder.push(format!("{column} < ")); Self::push_bind_value(builder, v); }
                    FilterOp::Lte(v) => { builder.push(format!("{column} <= ")); Self::push_bind_value(builder, v); }
                    FilterOp::In(vals) => {
                        if vals.is_empty() { builder.push("FALSE"); }
                        else {
                            builder.push(format!("{column} IN ("));
                            let mut first = true;
                            for v in vals {
                                if !first { builder.push(", "); }
                                first = false;
                                Self::push_bind_value(builder, v);
                            }
                            builder.push(")");
                        }
                    }
                    FilterOp::Contains(v) => {
                        builder.push(format!("{column}::text LIKE '%' || "));
                        Self::push_bind_value(builder, v);
                        builder.push(" || '%'");
                    }
                    FilterOp::StartsWith(v) => {
                        builder.push(format!("{column}::text LIKE "));
                        Self::push_bind_value(builder, v);
                        builder.push(" || '%'");
                    }
                    FilterOp::EndsWith(v) => {
                        builder.push(format!("{column}::text LIKE '%' || "));
                        Self::push_bind_value(builder, v);
                    }
                    FilterOp::TextSearch(v) => {
                        builder.push(format!("to_tsvector('simple', COALESCE({column}::text, '')) @@ websearch_to_tsquery('simple', "));
                        Self::push_bind_value(builder, v);
                        builder.push(")");
                    }
                }
            }
            Filter::And(filters) => {
                if filters.is_empty() { builder.push("TRUE"); }
                else {
                    builder.push("(");
                    let mut first = true;
                    for f in filters {
                        if !first { builder.push(" AND "); }
                        first = false;
                        Self::push_filter(builder, schema, f)?;
                    }
                    builder.push(")");
                }
            }
            Filter::Or(filters) => {
                if filters.is_empty() { builder.push("FALSE"); }
                else {
                    builder.push("(");
                    let mut first = true;
                    for f in filters {
                        if !first { builder.push(" OR "); }
                        first = false;
                        Self::push_filter(builder, schema, f)?;
                    }
                    builder.push(")");
                }
            }
            Filter::Not(f) => {
                builder.push("NOT (");
                Self::push_filter(builder, schema, f)?;
                builder.push(")");
            }
        }
        Ok(())
    }

    pub fn push_bind_value(builder: &mut QueryBuilder<'_, Postgres>, value: &StorageValue) {
        match value {
            StorageValue::Null => { builder.push_bind(Option::<String>::None); }
            StorageValue::Bool(v) => { builder.push_bind(*v); }
            StorageValue::BoolArray(v) => { builder.push_bind(v.clone()); }
            StorageValue::Int(v) => { builder.push_bind(*v); }
            StorageValue::IntArray(v) => { builder.push_bind(v.clone()); }
            StorageValue::Float(v) => { builder.push_bind(*v); }
            StorageValue::FloatArray(v) => { builder.push_bind(v.clone()); }
            StorageValue::String(v) => { builder.push_bind(v.clone()); }
            StorageValue::StringArray(v) => { builder.push_bind(v.clone()); }
            StorageValue::Bytes(v) => { builder.push_bind(v.clone()); }
            StorageValue::Timestamp(v) => { builder.push_bind(*v); }
            StorageValue::TimestampArray(v) => { builder.push_bind(v.clone()); }
            StorageValue::Json(v) => { builder.push_bind(sqlx::types::Json(v.clone())); }
        }
    }

    pub fn push_bind_value_separated(sep: &mut sqlx::query_builder::Separated<'_, '_, Postgres, &str>, value: &StorageValue) {
        match value {
            StorageValue::Null => { sep.push_bind(Option::<String>::None); }
            StorageValue::Bool(v) => { sep.push_bind(*v); }
            StorageValue::BoolArray(v) => { sep.push_bind(v.clone()); }
            StorageValue::Int(v) => { sep.push_bind(*v); }
            StorageValue::IntArray(v) => { sep.push_bind(v.clone()); }
            StorageValue::Float(v) => { sep.push_bind(*v); }
            StorageValue::FloatArray(v) => { sep.push_bind(v.clone()); }
            StorageValue::String(v) => { sep.push_bind(v.clone()); }
            StorageValue::StringArray(v) => { sep.push_bind(v.clone()); }
            StorageValue::Bytes(v) => { sep.push_bind(v.clone()); }
            StorageValue::Timestamp(v) => { sep.push_bind(*v); }
            StorageValue::TimestampArray(v) => { sep.push_bind(v.clone()); }
            StorageValue::Json(v) => { sep.push_bind(sqlx::types::Json(v.clone())); }
        }
    }

    pub fn push_sorts(builder: &mut QueryBuilder<'_, Postgres>, schema: &'static CollectionSchema, query: &QuerySpec) -> Result<(), DatabaseError> {
        if query.sorts().is_empty() { return Ok(()); }
        builder.push(" ORDER BY ");
        let mut first = true;
        for sort in query.sorts() {
            if !first { builder.push(", "); }
            first = false;
            let column = PostgresUtils::column_for_field(schema, &sort.field)?;
            let direction = match sort.direction { SortDirection::Asc => "ASC", SortDirection::Desc => "DESC" };
            builder.push(format!("{column} {direction}"));
        }
        Ok(())
    }

    pub fn authorization_context(context: &Context) -> database_core::utils::AuthorizationContext {
        let roles = context.roles().cloned().collect::<Vec<database_core::utils::Role>>();
        if context.authorization_enabled() { database_core::utils::AuthorizationContext::enabled(roles) }
        else { database_core::utils::AuthorizationContext::disabled(roles) }
    }

    pub fn permission_roles<'p, I>(permissions: I, action: PermissionEnum) -> Result<Vec<database_core::utils::Role>, DatabaseError>
    where I: IntoIterator<Item = &'p str>
    {
        let mut roles = Vec::new();
        for perm_str in permissions {
            let perm = database_core::utils::Permission::parse(perm_str).map_err(|e| DatabaseError::Other(format!("invalid permission '{perm_str}': {e}")))?;
            let matches = match (perm.permission(), action) {
                (PermissionEnum::Write, PermissionEnum::Create) | (PermissionEnum::Write, PermissionEnum::Update) | (PermissionEnum::Write, PermissionEnum::Delete) => true,
                (c, t) => c == t,
            };
            if matches { roles.push(perm.role_instance().clone()); }
        }
        Ok(roles)
    }

    pub fn document_action_roles(context: &Context, schema: &'static CollectionSchema, action: PermissionEnum) -> Result<Option<Vec<String>>, DatabaseError> {
        let auth_ctx = Self::authorization_context(context);
        let collection_roles = Self::permission_roles(schema.permissions.iter().copied(), action)?;
        match database_core::utils::Authorization::new(action, &auth_ctx).validate(&collection_roles) {
            Ok(()) => Ok(None),
            Err(e) if schema.document_security => {
                match DatabaseError::from(e) {
                    DatabaseError::Authorization(_) => Ok(Some(auth_ctx.roles().into_iter().map(|r| r.to_string()).collect())),
                    other => Err(other),
                }
            }
            Err(e) => Err(DatabaseError::from(e)),
        }
    }

    pub fn push_document_action_condition(
        builder: &mut QueryBuilder<'_, Postgres>,
        context: &Context,
        schema: &'static CollectionSchema,
        alias: &str,
        action: PermissionEnum,
        has_conditions: &mut bool,
    ) -> Result<(), DatabaseError> {
        let Some(roles) = Self::document_action_roles(context, schema, action)? else { return Ok(()); };
        let perms_table = PostgresUtils::qualified_permissions_table_name(context, schema.id)?;
        let alias_quoted = PostgresUtils::quote_identifier(alias)?;
        let seq_col = PostgresUtils::quote_identifier(database_core::COLUMN_SEQUENCE)?;

        Self::push_condition_separator(builder, has_conditions);
        builder.push("EXISTS (SELECT 1 FROM ");
        builder.push(perms_table);
        builder.push(" AS p WHERE p.document_id = ");
        builder.push(alias_quoted);
        builder.push(".");
        builder.push(seq_col);
        builder.push(" AND p.permission_type = ");
        builder.push_bind(action.to_string());
        builder.push(" AND p.permissions && ");
        builder.push_bind(roles);
        builder.push(")");
        Ok(())
    }
}
