use database_core::errors::DatabaseError;
use database_core::query::{Filter, FilterOp, QuerySpec, SortDirection};
use database_core::traits::storage::{AdapterFuture, StorageAdapter, StorageRecord, StorageValue};
use database_core::utils::{Authorization, AuthorizationContext, Permission, PermissionEnum, Role};
use database_core::{
    AttributeKind, COLUMN_CREATED_AT, COLUMN_ID, COLUMN_PERMISSIONS, COLUMN_SEQUENCE,
    COLUMN_UPDATED_AT, CollectionSchema, Context, FIELD_CREATED_AT, FIELD_ID, FIELD_PERMISSIONS,
    FIELD_SEQUENCE, FIELD_UPDATED_AT, IndexKind, Order,
};
use sqlx::types::time::OffsetDateTime;
use sqlx::{Pool, Postgres, QueryBuilder, Row};
use std::collections::BTreeMap;

#[derive(Clone)]
pub struct PostgresAdapter<'a> {
    pool: &'a Pool<Postgres>,
}

impl<'a> PostgresAdapter<'a> {
    pub fn new(pool: &'a Pool<Postgres>) -> Self {
        Self { pool }
    }

    fn is_valid_identifier(identifier: &str) -> bool {
        let mut chars = identifier.chars();
        let Some(first) = chars.next() else {
            return false;
        };

        if !(first == '_' || first.is_ascii_alphabetic()) {
            return false;
        }

        chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
    }

    pub fn quote_identifier(identifier: &str) -> Result<String, DatabaseError> {
        if !Self::is_valid_identifier(identifier) {
            return Err(DatabaseError::Other(format!(
                "invalid postgres identifier: {identifier}"
            )));
        }

        Ok(format!("\"{identifier}\""))
    }

    pub(crate) fn qualified_table_name(
        context: &Context,
        collection: &str,
    ) -> Result<String, DatabaseError> {
        let schema = Self::quote_identifier(context.schema())?;
        let collection = Self::quote_identifier(collection)?;
        Ok(format!("{schema}.{collection}"))
    }

    pub(crate) fn qualified_permissions_table_name(
        context: &Context,
        collection: &str,
    ) -> Result<String, DatabaseError> {
        Self::qualified_table_name(context, &format!("{collection}_perms"))
    }

    pub(crate) fn quoted_system_column(identifier: &str) -> Result<String, DatabaseError> {
        Self::quote_identifier(identifier)
    }

    pub(crate) fn sql_type(kind: AttributeKind, array: bool) -> String {
        let base = match kind {
            AttributeKind::String | AttributeKind::Relationship | AttributeKind::Virtual => "TEXT",
            AttributeKind::Integer => "BIGINT",
            AttributeKind::Float => "DOUBLE PRECISION",
            AttributeKind::Boolean => "BOOLEAN",
            AttributeKind::Timestamp => "TIMESTAMPTZ",
            AttributeKind::Json => "JSONB",
        };

        if array {
            format!("{base}[]")
        } else {
            base.to_string()
        }
    }

    fn persisted_attributes(
        schema: &'static CollectionSchema,
    ) -> Vec<&'static database_core::AttributeSchema> {
        schema.persisted_attributes().collect()
    }

    fn column_for_field(
        schema: &'static CollectionSchema,
        field: &str,
    ) -> Result<String, DatabaseError> {
        let column = match field {
            FIELD_ID => COLUMN_ID,
            FIELD_SEQUENCE => COLUMN_SEQUENCE,
            FIELD_CREATED_AT => COLUMN_CREATED_AT,
            FIELD_UPDATED_AT => COLUMN_UPDATED_AT,
            FIELD_PERMISSIONS => COLUMN_PERMISSIONS,
            other => schema
                .attribute(other)
                .map(|attribute| attribute.column)
                .ok_or_else(|| {
                    DatabaseError::Other(format!(
                        "collection '{}': unknown query field '{}'",
                        schema.id, other
                    ))
                })?,
        };

        Self::quote_identifier(column)
    }

    fn select_columns(schema: &'static CollectionSchema) -> Result<String, DatabaseError> {
        let mut columns = vec![
            Self::quoted_system_column(COLUMN_SEQUENCE)?,
            Self::quoted_system_column(COLUMN_ID)?,
            Self::quoted_system_column(COLUMN_CREATED_AT)?,
            Self::quoted_system_column(COLUMN_UPDATED_AT)?,
            Self::quoted_system_column(COLUMN_PERMISSIONS)?,
        ];

        for attribute in Self::persisted_attributes(schema) {
            columns.push(Self::quote_identifier(attribute.column)?);
        }

        Ok(columns.join(", "))
    }

    pub(crate) fn quoted_column_list(
        schema: &'static CollectionSchema,
        attributes: &[&str],
        orders: &[Order],
    ) -> Result<String, DatabaseError> {
        let mut out = Vec::with_capacity(attributes.len());
        for (index, attribute_id) in attributes.iter().enumerate() {
            let attribute = schema.attribute(attribute_id).ok_or_else(|| {
                DatabaseError::Other(format!(
                    "index references unknown attribute '{}.{}'",
                    schema.id, attribute_id
                ))
            })?;
            let mut column = Self::quote_identifier(attribute.column)?;
            if let Some(order) = orders.get(index) {
                match order {
                    Order::Asc => column.push_str(" ASC"),
                    Order::Desc => column.push_str(" DESC"),
                    Order::None => {}
                }
            }
            out.push(column);
        }
        Ok(out.join(", "))
    }

    pub(crate) fn full_text_expression(
        schema: &'static CollectionSchema,
        attributes: &[&str],
    ) -> Result<String, DatabaseError> {
        let mut parts = Vec::with_capacity(attributes.len());
        for attribute_id in attributes {
            let attribute = schema.attribute(attribute_id).ok_or_else(|| {
                DatabaseError::Other(format!(
                    "index references unknown attribute '{}.{}'",
                    schema.id, attribute_id
                ))
            })?;
            let column = Self::quote_identifier(attribute.column)?;
            parts.push(format!("COALESCE({column}::text, '')"));
        }

        Ok(format!(
            "to_tsvector('simple', {})",
            parts.join(" || ' ' || ")
        ))
    }

    pub(crate) fn internal_index_statements(
        schema: &'static CollectionSchema,
        table: &str,
        perms_table: &str,
    ) -> Result<Vec<String>, DatabaseError> {
        Ok(vec![
            format!(
                "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {table} ({})",
                Self::quote_identifier(&format!("{}_uid", schema.id))?,
                Self::quoted_system_column(COLUMN_ID)?
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS {} ON {table} ({})",
                Self::quote_identifier(&format!("{}_created_at", schema.id))?,
                Self::quoted_system_column(COLUMN_CREATED_AT)?
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS {} ON {table} ({})",
                Self::quote_identifier(&format!("{}_updated_at", schema.id))?,
                Self::quoted_system_column(COLUMN_UPDATED_AT)?
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS {} ON {table} USING GIN ({})",
                Self::quote_identifier(&format!("{}_permissions_gin_idx", schema.id))?,
                Self::quoted_system_column(COLUMN_PERMISSIONS)?
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS {} ON {perms_table} USING GIN (\"permissions\")",
                Self::quote_identifier(&format!("{}_perms_permissions_gin_idx", schema.id))?
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS {} ON {perms_table} (\"permission_type\")",
                Self::quote_identifier(&format!("{}_perms_permission_type_idx", schema.id))?
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS {} ON {perms_table} (\"document_id\")",
                Self::quote_identifier(&format!("{}_perms_document_id_idx", schema.id))?
            ),
        ])
    }

    pub(crate) fn schema_index_statements(
        schema: &'static CollectionSchema,
        table: &str,
    ) -> Result<Vec<String>, DatabaseError> {
        let mut statements = Vec::with_capacity(schema.indexes.len());

        for index in schema.indexes {
            let index_name = Self::quote_identifier(index.id)?;
            let statement = match index.kind {
                IndexKind::Key => format!(
                    "CREATE INDEX IF NOT EXISTS {index_name} ON {table} ({})",
                    Self::quoted_column_list(schema, index.attributes, index.orders)?
                ),
                IndexKind::Unique => format!(
                    "CREATE UNIQUE INDEX IF NOT EXISTS {index_name} ON {table} ({})",
                    Self::quoted_column_list(schema, index.attributes, index.orders)?
                ),
                IndexKind::FullText => format!(
                    "CREATE INDEX IF NOT EXISTS {index_name} ON {table} USING GIN ({})",
                    Self::full_text_expression(schema, index.attributes)?
                ),
                IndexKind::Spatial => {
                    return Err(DatabaseError::Other(format!(
                        "collection '{}': postgres adapter does not support spatial indexes yet",
                        schema.id
                    )));
                }
            };
            statements.push(statement);
        }

        Ok(statements)
    }

    fn extract_string(values: &StorageRecord, key: &str) -> Result<String, DatabaseError> {
        match values.get(key) {
            Some(StorageValue::String(value)) => Ok(value.clone()),
            Some(_) => Err(DatabaseError::Other(format!(
                "record field '{key}' must be a string"
            ))),
            None => Err(DatabaseError::Other(format!(
                "record field '{key}' is required"
            ))),
        }
    }

    fn extract_optional_string_array(
        values: &StorageRecord,
        key: &str,
    ) -> Result<Vec<String>, DatabaseError> {
        match values.get(key) {
            Some(StorageValue::StringArray(value)) => Ok(value.clone()),
            Some(StorageValue::Null) | None => Ok(Vec::new()),
            Some(_) => Err(DatabaseError::Other(format!(
                "record field '{key}' must be a string array"
            ))),
        }
    }

    fn extract_optional_timestamp(
        values: &StorageRecord,
        key: &str,
    ) -> Result<Option<OffsetDateTime>, DatabaseError> {
        match values.get(key) {
            Some(StorageValue::Timestamp(value)) => Ok(Some(*value)),
            Some(StorageValue::Null) | None => Ok(None),
            Some(_) => Err(DatabaseError::Other(format!(
                "record field '{key}' must be a timestamp"
            ))),
        }
    }

    fn permission_rows(
        permissions: &[String],
    ) -> Result<BTreeMap<String, Vec<String>>, DatabaseError> {
        let mut rows: BTreeMap<String, Vec<String>> = BTreeMap::new();

        for value in permissions {
            let permission = Permission::parse(value).map_err(|error| {
                DatabaseError::Other(format!("invalid permission '{value}': {error}"))
            })?;

            let permission_types = match permission.permission() {
                PermissionEnum::Write => vec![
                    PermissionEnum::Write,
                    PermissionEnum::Create,
                    PermissionEnum::Update,
                    PermissionEnum::Delete,
                ],
                other => vec![other],
            };

            for permission_type in permission_types {
                rows.entry(permission_type.to_string())
                    .or_default()
                    .push(permission.role_instance().to_string());
            }
        }

        Ok(rows)
    }

    fn validate_context_support(context: &Context) -> Result<(), DatabaseError> {
        if context.shared_tables() {
            return Err(DatabaseError::Other(
                "shared table layout is not implemented in the postgres adapter yet".into(),
            ));
        }

        if context.tenant_id().is_some() {
            return Err(DatabaseError::Other(
                "tenant-scoped contexts are not implemented in the postgres adapter yet".into(),
            ));
        }

        if context.tenant_per_document() {
            return Err(DatabaseError::Other(
                "per-document tenancy is not implemented in the postgres adapter yet".into(),
            ));
        }

        Ok(())
    }

    fn append_bind(
        separated: &mut sqlx::query_builder::Separated<'_, '_, Postgres, &'static str>,
        value: &StorageValue,
    ) -> Result<(), DatabaseError> {
        match value {
            StorageValue::Null => {
                separated.push_bind(Option::<String>::None);
            }
            StorageValue::Bool(value) => {
                separated.push_bind(*value);
            }
            StorageValue::BoolArray(value) => {
                separated.push_bind(value.clone());
            }
            StorageValue::Int(value) => {
                separated.push_bind(*value);
            }
            StorageValue::IntArray(value) => {
                separated.push_bind(value.clone());
            }
            StorageValue::Float(value) => {
                separated.push_bind(*value);
            }
            StorageValue::FloatArray(value) => {
                separated.push_bind(value.clone());
            }
            StorageValue::String(value) => {
                separated.push_bind(value.clone());
            }
            StorageValue::StringArray(value) => {
                separated.push_bind(value.clone());
            }
            StorageValue::Bytes(value) => {
                separated.push_bind(value.clone());
            }
            StorageValue::Timestamp(value) => {
                separated.push_bind(*value);
            }
            StorageValue::TimestampArray(value) => {
                separated.push_bind(value.clone());
            }
            StorageValue::Json(value) => {
                separated.push_bind(sqlx::types::Json(value.clone()));
            }
        }

        Ok(())
    }

    fn push_bind_value(
        builder: &mut QueryBuilder<'_, Postgres>,
        value: &StorageValue,
    ) -> Result<(), DatabaseError> {
        match value {
            StorageValue::Null => {
                builder.push_bind(Option::<String>::None);
            }
            StorageValue::Bool(value) => {
                builder.push_bind(*value);
            }
            StorageValue::BoolArray(value) => {
                builder.push_bind(value.clone());
            }
            StorageValue::Int(value) => {
                builder.push_bind(*value);
            }
            StorageValue::IntArray(value) => {
                builder.push_bind(value.clone());
            }
            StorageValue::Float(value) => {
                builder.push_bind(*value);
            }
            StorageValue::FloatArray(value) => {
                builder.push_bind(value.clone());
            }
            StorageValue::String(value) => {
                builder.push_bind(value.clone());
            }
            StorageValue::StringArray(value) => {
                builder.push_bind(value.clone());
            }
            StorageValue::Bytes(value) => {
                builder.push_bind(value.clone());
            }
            StorageValue::Timestamp(value) => {
                builder.push_bind(*value);
            }
            StorageValue::TimestampArray(value) => {
                builder.push_bind(value.clone());
            }
            StorageValue::Json(value) => {
                builder.push_bind(sqlx::types::Json(value.clone()));
            }
        }

        Ok(())
    }

    fn push_filters(
        builder: &mut QueryBuilder<'_, Postgres>,
        schema: &'static CollectionSchema,
        query: &QuerySpec,
        has_conditions: &mut bool,
    ) -> Result<(), DatabaseError> {
        if query.filters().is_empty() {
            return Ok(());
        }

        Self::push_condition_separator(builder, has_conditions);
        let mut first = true;
        for filter in query.filters() {
            if !first {
                builder.push(" AND ");
            }
            first = false;
            Self::push_filter(builder, schema, filter)?;
        }

        Ok(())
    }

    fn push_condition_separator(
        builder: &mut QueryBuilder<'_, Postgres>,
        has_conditions: &mut bool,
    ) {
        if *has_conditions {
            builder.push(" AND ");
        } else {
            builder.push(" WHERE ");
            *has_conditions = true;
        }
    }

    fn authorization_context(context: &Context) -> AuthorizationContext {
        let roles = context.roles().cloned().collect::<Vec<Role>>();
        if context.authorization_enabled() {
            AuthorizationContext::enabled(roles)
        } else {
            AuthorizationContext::disabled(roles)
        }
    }

    fn permission_roles<'p, I>(
        permissions: I,
        action: PermissionEnum,
    ) -> Result<Vec<Role>, DatabaseError>
    where
        I: IntoIterator<Item = &'p str>,
    {
        let mut roles = Vec::new();

        for permission in permissions {
            let permission = Permission::parse(permission).map_err(|error| {
                DatabaseError::Other(format!("invalid permission '{permission}': {error}"))
            })?;

            let matches = match (permission.permission(), action) {
                (PermissionEnum::Write, PermissionEnum::Create)
                | (PermissionEnum::Write, PermissionEnum::Update)
                | (PermissionEnum::Write, PermissionEnum::Delete) => true,
                (current, target) => current == target,
            };

            if matches {
                roles.push(permission.role_instance().clone());
            }
        }

        Ok(roles)
    }

    fn document_action_roles(
        context: &Context,
        schema: &'static CollectionSchema,
        action: PermissionEnum,
    ) -> Result<Option<Vec<String>>, DatabaseError> {
        let authorization_context = Self::authorization_context(context);
        let collection_roles = Self::permission_roles(schema.permissions.iter().copied(), action)?;

        match Authorization::new(action, &authorization_context).validate(&collection_roles) {
            Ok(()) => Ok(None),
            Err(error) if schema.document_security => match DatabaseError::from(error.clone()) {
                DatabaseError::Authorization(_) => Ok(Some(
                    authorization_context
                        .roles()
                        .into_iter()
                        .map(|role| role.to_string())
                        .collect(),
                )),
                other => Err(other),
            },
            Err(error) => Err(DatabaseError::from(error)),
        }
    }

    fn push_document_action_condition(
        builder: &mut QueryBuilder<'_, Postgres>,
        context: &Context,
        schema: &'static CollectionSchema,
        alias: &str,
        action: PermissionEnum,
        has_conditions: &mut bool,
    ) -> Result<(), DatabaseError> {
        let Some(roles) = Self::document_action_roles(context, schema, action)? else {
            return Ok(());
        };

        let perms_table = Self::qualified_permissions_table_name(context, schema.id)?;
        let alias = Self::quote_identifier(alias)?;
        let document_id = Self::quote_identifier("document_id")?;
        let permission_type = Self::quote_identifier("permission_type")?;
        let permissions = Self::quote_identifier("permissions")?;
        let sequence = Self::quote_identifier(COLUMN_SEQUENCE)?;

        Self::push_condition_separator(builder, has_conditions);
        builder.push("EXISTS (SELECT 1 FROM ");
        builder.push(perms_table);
        builder.push(" AS p WHERE p.");
        builder.push(document_id);
        builder.push(" = ");
        builder.push(alias);
        builder.push(".");
        builder.push(sequence);
        builder.push(" AND p.");
        builder.push(permission_type);
        builder.push(" = ");
        builder.push_bind(action.to_string());
        builder.push(" AND p.");
        builder.push(permissions);
        builder.push(" && ");
        builder.push_bind(roles);
        builder.push(")");

        Ok(())
    }

    fn push_filter(
        builder: &mut QueryBuilder<'_, Postgres>,
        schema: &'static CollectionSchema,
        filter: &Filter,
    ) -> Result<(), DatabaseError> {
        match filter {
            Filter::Field { field, op } => {
                let column = Self::column_for_field(schema, field)?;
                match op {
                    FilterOp::Eq(StorageValue::Null) | FilterOp::IsNull => {
                        builder.push(format!("{column} IS NULL"));
                    }
                    FilterOp::NotEq(StorageValue::Null) | FilterOp::IsNotNull => {
                        builder.push(format!("{column} IS NOT NULL"));
                    }
                    FilterOp::Eq(value) => {
                        builder.push(format!("{column} = "));
                        Self::push_bind_value(builder, value)?;
                    }
                    FilterOp::NotEq(value) => {
                        builder.push(format!("{column} <> "));
                        Self::push_bind_value(builder, value)?;
                    }
                    FilterOp::Gt(value) => {
                        builder.push(format!("{column} > "));
                        Self::push_bind_value(builder, value)?;
                    }
                    FilterOp::Gte(value) => {
                        builder.push(format!("{column} >= "));
                        Self::push_bind_value(builder, value)?;
                    }
                    FilterOp::Lt(value) => {
                        builder.push(format!("{column} < "));
                        Self::push_bind_value(builder, value)?;
                    }
                    FilterOp::Lte(value) => {
                        builder.push(format!("{column} <= "));
                        Self::push_bind_value(builder, value)?;
                    }
                    FilterOp::Contains(value) => {
                        builder.push(format!("{column}::text LIKE '%' || "));
                        Self::push_bind_value(builder, value)?;
                        builder.push(" || '%'");
                    }
                    FilterOp::StartsWith(value) => {
                        builder.push(format!("{column}::text LIKE "));
                        Self::push_bind_value(builder, value)?;
                        builder.push(" || '%'");
                    }
                    FilterOp::EndsWith(value) => {
                        builder.push(format!("{column}::text LIKE '%' || "));
                        Self::push_bind_value(builder, value)?;
                    }
                    FilterOp::TextSearch(value) => {
                        builder.push(format!(
                            "to_tsvector('simple', COALESCE({column}::text, '')) @@ websearch_to_tsquery('simple', "
                        ));
                        Self::push_bind_value(builder, value)?;
                        builder.push(")");
                    }
                    FilterOp::In(values) => {
                        if values.is_empty() {
                            builder.push("FALSE");
                        } else {
                            builder.push(format!("{column} IN ("));
                            let mut first = true;
                            for value in values {
                                if !first {
                                    builder.push(", ");
                                }
                                first = false;
                                Self::push_bind_value(builder, value)?;
                            }
                            builder.push(")");
                        }
                    }
                }
            }
            Filter::And(filters) => {
                if filters.is_empty() {
                    builder.push("TRUE");
                } else {
                    builder.push("(");
                    let mut first = true;
                    for f in filters {
                        if !first {
                            builder.push(" AND ");
                        }
                        first = false;
                        Self::push_filter(builder, schema, f)?;
                    }
                    builder.push(")");
                }
            }
            Filter::Or(filters) => {
                if filters.is_empty() {
                    builder.push("FALSE");
                } else {
                    builder.push("(");
                    let mut first = true;
                    for f in filters {
                        if !first {
                            builder.push(" OR ");
                        }
                        first = false;
                        Self::push_filter(builder, schema, f)?;
                    }
                    builder.push(")");
                }
            }
            Filter::Not(filter) => {
                builder.push("NOT (");
                Self::push_filter(builder, schema, filter)?;
                builder.push(")");
            }
        }

        Ok(())
    }

    fn push_sorts(
        builder: &mut QueryBuilder<'_, Postgres>,
        schema: &'static CollectionSchema,
        query: &QuerySpec,
    ) -> Result<(), DatabaseError> {
        if query.sorts().is_empty() {
            return Ok(());
        }

        builder.push(" ORDER BY ");
        let mut first = true;
        for sort in query.sorts() {
            if !first {
                builder.push(", ");
            }
            first = false;
            let column = Self::column_for_field(schema, &sort.field)?;
            let direction = match sort.direction {
                SortDirection::Asc => "ASC",
                SortDirection::Desc => "DESC",
            };
            builder.push(format!("{column} {direction}"));
        }

        Ok(())
    }

    fn push_update_assignments(
        builder: &mut QueryBuilder<'_, Postgres>,
        schema: &'static CollectionSchema,
        values: &StorageRecord,
        permissions_update: bool,
        permissions: &[String],
        attributes: &[&'static database_core::AttributeSchema],
    ) -> Result<(), DatabaseError> {
        let mut first = true;

        if permissions_update {
            builder.push(Self::quoted_system_column(COLUMN_PERMISSIONS)?);
            builder.push(" = ");
            builder.push_bind(permissions.to_vec());
            first = false;
        }

        for attribute in attributes {
            if !first {
                builder.push(", ");
            }
            first = false;

            builder.push(Self::quote_identifier(attribute.column)?);
            builder.push(" = ");
            let value = values.get(attribute.id).ok_or_else(|| {
                DatabaseError::Other(format!(
                    "collection '{}': missing update value for '{}'",
                    schema.id, attribute.id
                ))
            })?;
            Self::push_bind_value(builder, value)?;
            if attribute.kind == AttributeKind::Json {
                builder.push("::jsonb");
            }
        }

        Ok(())
    }

    fn row_to_record(
        row: &sqlx::postgres::PgRow,
        schema: &'static CollectionSchema,
    ) -> Result<StorageRecord, DatabaseError> {
        let mut record = StorageRecord::new();

        let sequence = row.try_get::<i64, _>(COLUMN_SEQUENCE).map_err(|error| {
            DatabaseError::Other(format!("postgres failed decoding sequence: {error}"))
        })?;
        let uid = row.try_get::<String, _>(COLUMN_ID).map_err(|error| {
            DatabaseError::Other(format!("postgres failed decoding id: {error}"))
        })?;
        let created_at = row
            .try_get::<Option<OffsetDateTime>, _>(COLUMN_CREATED_AT)
            .map_err(|error| {
                DatabaseError::Other(format!("postgres failed decoding createdAt: {error}"))
            })?;
        let updated_at = row
            .try_get::<Option<OffsetDateTime>, _>(COLUMN_UPDATED_AT)
            .map_err(|error| {
                DatabaseError::Other(format!("postgres failed decoding updatedAt: {error}"))
            })?;
        let permissions = row
            .try_get::<Option<Vec<String>>, _>(COLUMN_PERMISSIONS)
            .map_err(|error| {
                DatabaseError::Other(format!("postgres failed decoding permissions: {error}"))
            })?;

        record.insert(FIELD_SEQUENCE.to_string(), StorageValue::Int(sequence));
        record.insert(FIELD_ID.to_string(), StorageValue::String(uid));
        record.insert(
            FIELD_CREATED_AT.to_string(),
            created_at
                .map(StorageValue::Timestamp)
                .unwrap_or(StorageValue::Null),
        );
        record.insert(
            FIELD_UPDATED_AT.to_string(),
            updated_at
                .map(StorageValue::Timestamp)
                .unwrap_or(StorageValue::Null),
        );
        record.insert(
            FIELD_PERMISSIONS.to_string(),
            permissions
                .map(StorageValue::StringArray)
                .unwrap_or_else(|| StorageValue::StringArray(Vec::new())),
        );

        for attribute in Self::persisted_attributes(schema) {
            let key = attribute.column;
            let value = if attribute.array {
                match attribute.kind {
                    AttributeKind::String
                    | AttributeKind::Relationship
                    | AttributeKind::Virtual => {
                        row.try_get::<Option<Vec<String>>, _>(key).map(|value| {
                            value
                                .map(StorageValue::StringArray)
                                .unwrap_or(StorageValue::Null)
                        })
                    }
                    AttributeKind::Integer => {
                        row.try_get::<Option<Vec<i64>>, _>(key).map(|value| {
                            value
                                .map(StorageValue::IntArray)
                                .unwrap_or(StorageValue::Null)
                        })
                    }
                    AttributeKind::Float => row.try_get::<Option<Vec<f64>>, _>(key).map(|value| {
                        value
                            .map(StorageValue::FloatArray)
                            .unwrap_or(StorageValue::Null)
                    }),
                    AttributeKind::Boolean => {
                        row.try_get::<Option<Vec<bool>>, _>(key).map(|value| {
                            value
                                .map(StorageValue::BoolArray)
                                .unwrap_or(StorageValue::Null)
                        })
                    }
                    AttributeKind::Timestamp => row
                        .try_get::<Option<Vec<OffsetDateTime>>, _>(key)
                        .map(|value: Option<Vec<OffsetDateTime>>| {
                            value
                                .map(StorageValue::TimestampArray)
                                .unwrap_or(StorageValue::Null)
                        }),
                    AttributeKind::Json => row
                        .try_get::<Option<Vec<sqlx::types::Json<serde_json::Value>>>, _>(key)
                        .map(|value| {
                            value
                                .map(|values| {
                                    StorageValue::StringArray(
                                        values.into_iter().map(|v| v.0.to_string()).collect(),
                                    )
                                })
                                .unwrap_or(StorageValue::Null)
                        }),
                }
            } else {
                match attribute.kind {
                    AttributeKind::String
                    | AttributeKind::Relationship
                    | AttributeKind::Virtual => {
                        row.try_get::<Option<String>, _>(key).map(|value| {
                            value
                                .map(StorageValue::String)
                                .unwrap_or(StorageValue::Null)
                        })
                    }
                    AttributeKind::Integer => row
                        .try_get::<Option<i64>, _>(key)
                        .map(|value| value.map(StorageValue::Int).unwrap_or(StorageValue::Null)),
                    AttributeKind::Float => row
                        .try_get::<Option<f64>, _>(key)
                        .map(|value| value.map(StorageValue::Float).unwrap_or(StorageValue::Null)),
                    AttributeKind::Boolean => row
                        .try_get::<Option<bool>, _>(key)
                        .map(|value| value.map(StorageValue::Bool).unwrap_or(StorageValue::Null)),
                    AttributeKind::Timestamp => row.try_get::<Option<OffsetDateTime>, _>(key).map(
                        |value: Option<OffsetDateTime>| {
                            value
                                .map(StorageValue::Timestamp)
                                .unwrap_or(StorageValue::Null)
                        },
                    ),
                    AttributeKind::Json => row
                        .try_get::<Option<sqlx::types::Json<serde_json::Value>>, _>(key)
                        .map(|value| {
                            value
                                .map(|json| StorageValue::Json(json.0.to_string()))
                                .unwrap_or(StorageValue::Null)
                        }),
                }
            }
            .map_err(|error| {
                DatabaseError::Other(format!(
                    "postgres failed decoding '{}.{}': {error}",
                    schema.id, attribute.id
                ))
            })?;

            record.insert(attribute.id.to_string(), value);
        }

        Ok(record)
    }
}

impl<'a> StorageAdapter for PostgresAdapter<'a> {
    fn enforces_document_filtering(&self, action: PermissionEnum) -> bool {
        matches!(
            action,
            PermissionEnum::Read | PermissionEnum::Update | PermissionEnum::Delete
        )
    }

    fn ping(&self, context: &Context) -> AdapterFuture<'_, Result<(), DatabaseError>> {
        if let Err(error) = Self::validate_context_support(context) {
            return Box::pin(async move { Err(error) });
        }

        let pool = self.pool;
        Box::pin(async move {
            sqlx::query_scalar::<_, i32>("SELECT 1")
                .fetch_one(pool)
                .await
                .map(|_| ())
                .map_err(|error| DatabaseError::Other(format!("postgres ping failed: {error}")))
        })
    }

    fn create_collection(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
    ) -> AdapterFuture<'_, Result<(), DatabaseError>> {
        let pool = self.pool;
        let context = context.clone();

        Box::pin(async move {
            Self::validate_context_support(&context)?;

            let schema_name = Self::quote_identifier(context.schema())?;
            let table = Self::qualified_table_name(&context, schema.id)?;
            let perms_table = Self::qualified_permissions_table_name(&context, schema.id)?;

            let mut columns = Vec::new();
            columns.push(format!(
                "{} BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY",
                Self::quoted_system_column(COLUMN_SEQUENCE)?
            ));
            columns.push(format!(
                "{} VARCHAR(255) NOT NULL",
                Self::quoted_system_column(COLUMN_ID)?
            ));
            columns.push(format!(
                "{} TIMESTAMPTZ DEFAULT NULL",
                Self::quoted_system_column(COLUMN_CREATED_AT)?
            ));
            columns.push(format!(
                "{} TIMESTAMPTZ DEFAULT NULL",
                Self::quoted_system_column(COLUMN_UPDATED_AT)?
            ));
            columns.push(format!(
                "{} TEXT[] DEFAULT '{{}}'",
                Self::quoted_system_column(COLUMN_PERMISSIONS)?
            ));

            for attribute in Self::persisted_attributes(schema) {
                let column = Self::quote_identifier(attribute.column)?;
                let sql_type = Self::sql_type(attribute.kind, attribute.array);
                let required = if attribute.required { " NOT NULL" } else { "" };

                columns.push(format!("{column} {sql_type}{required}"));
            }

            let create_schema = format!("CREATE SCHEMA IF NOT EXISTS {schema_name}");
            let create_table = format!(
                "CREATE TABLE IF NOT EXISTS {table} ({}, PRIMARY KEY ({}))",
                columns.join(", "),
                Self::quoted_system_column(COLUMN_SEQUENCE)?
            );
            let create_permissions = format!(
                "CREATE TABLE IF NOT EXISTS {perms_table} (document_id BIGINT NOT NULL, permission_type TEXT NOT NULL, permissions TEXT[] NOT NULL DEFAULT '{{}}', PRIMARY KEY (document_id, permission_type), FOREIGN KEY (document_id) REFERENCES {table}({}) ON DELETE CASCADE)",
                Self::quoted_system_column(COLUMN_SEQUENCE)?
            );
            let mut index_statements =
                Self::internal_index_statements(schema, &table, &perms_table)?;
            index_statements.extend(Self::schema_index_statements(schema, &table)?);

            sqlx::query(&create_schema)
                .execute(pool)
                .await
                .map_err(|error| {
                    DatabaseError::Other(format!("postgres schema create failed: {error}"))
                })?;

            sqlx::query(&create_table)
                .execute(pool)
                .await
                .map_err(|error| {
                    DatabaseError::Other(format!("postgres table create failed: {error}"))
                })?;

            sqlx::query(&create_permissions)
                .execute(pool)
                .await
                .map_err(|error| {
                    DatabaseError::Other(format!(
                        "postgres permissions table create failed: {error}"
                    ))
                })?;

            for statement in index_statements {
                sqlx::query(&statement)
                    .execute(pool)
                    .await
                    .map_err(|error| {
                        DatabaseError::Other(format!("postgres index create failed: {error}"))
                    })?;
            }

            Ok(())
        })
    }

    fn insert(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        values: StorageRecord,
    ) -> AdapterFuture<'_, Result<StorageRecord, DatabaseError>> {
        let pool = self.pool;
        let context = context.clone();

        Box::pin(async move {
            Self::validate_context_support(&context)?;

            let table = Self::qualified_table_name(&context, schema.id)?;
            let uid = Self::extract_string(&values, FIELD_ID)?;
            let created_at = Self::extract_optional_timestamp(&values, FIELD_CREATED_AT)?;
            let updated_at = Self::extract_optional_timestamp(&values, FIELD_UPDATED_AT)?;
            let permissions = Self::extract_optional_string_array(&values, FIELD_PERMISSIONS)?;
            let attributes: Vec<_> = Self::persisted_attributes(schema)
                .into_iter()
                .filter(|attribute| values.contains_key(attribute.id))
                .collect();

            if attributes.is_empty() {
                return Err(DatabaseError::Other(format!(
                    "collection '{}': insert received no persisted values",
                    schema.id
                )));
            }

            let mut builder = QueryBuilder::<Postgres>::new("WITH inserted AS (");
            builder.push(format!("INSERT INTO {table} ("));
            {
                let mut separated = builder.separated(", ");
                separated.push(Self::quoted_system_column(COLUMN_ID)?);
                separated.push(Self::quoted_system_column(COLUMN_CREATED_AT)?);
                separated.push(Self::quoted_system_column(COLUMN_UPDATED_AT)?);
                separated.push(Self::quoted_system_column(COLUMN_PERMISSIONS)?);
                for attribute in &attributes {
                    separated.push(Self::quote_identifier(attribute.column)?);
                }
            }
            builder.push(") VALUES (");
            {
                let mut separated = builder.separated(", ");
                separated.push_bind(uid);
                separated.push_bind(created_at);
                separated.push_bind(updated_at);
                separated.push_bind(permissions.clone());
                for attribute in &attributes {
                    let value = values.get(attribute.id).ok_or_else(|| {
                        DatabaseError::Other(format!(
                            "collection '{}': missing value for '{}'",
                            schema.id, attribute.id
                        ))
                    })?;
                    Self::append_bind(&mut separated, value)?;
                    if attribute.kind == AttributeKind::Json {
                        separated.push_unseparated("::jsonb");
                    }
                }
            }
            builder.push(format!(
                ") RETURNING {} ",
                Self::quoted_system_column(COLUMN_SEQUENCE)?
            ));
            builder.push(")");

            let perms_table = Self::qualified_permissions_table_name(&context, schema.id)?;
            let grouped_perms = Self::permission_rows(&permissions)?;

            if !grouped_perms.is_empty() {
                builder.push(format!(", sync_perms AS (INSERT INTO {perms_table} (document_id, permission_type, permissions) "));
                let mut first = true;
                for (pt, pv) in grouped_perms {
                    if !first {
                        builder.push(" UNION ALL ");
                    }
                    builder.push(format!(
                        "SELECT (SELECT {} FROM inserted), ",
                        Self::quoted_system_column(COLUMN_SEQUENCE)?
                    ));
                    builder.push_bind(pt);
                    builder.push(", ");
                    builder.push_bind(pv);
                    first = false;
                }
                builder.push(") ");
            }

            builder.push("SELECT * FROM inserted");

            let row = builder.build().fetch_one(pool).await.map_err(|error| {
                DatabaseError::Other(format!("postgres single-roundtrip insert failed: {error}"))
            })?;

            let sequence = row.try_get::<i64, _>(0).map_err(|error| {
                DatabaseError::Other(format!("postgres sequence read failed: {error}"))
            })?;

            let mut values = values;
            values.insert(FIELD_SEQUENCE.to_string(), StorageValue::Int(sequence));
            values.insert(
                FIELD_PERMISSIONS.to_string(),
                StorageValue::StringArray(permissions),
            );

            Ok(values)
        })
    }

    fn insert_many(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        values: Vec<StorageRecord>,
    ) -> AdapterFuture<'_, Result<Vec<StorageRecord>, DatabaseError>> {
        let pool = self.pool;
        let context = context.clone();

        Box::pin(async move {
            Self::validate_context_support(&context)?;

            if values.is_empty() {
                return Ok(Vec::new());
            }

            let table = Self::qualified_table_name(&context, schema.id)?;
            let mut results = Vec::with_capacity(values.len());

            // Group by attribute keys to handle records with different partial fields
            let mut groups: BTreeMap<Vec<String>, Vec<StorageRecord>> = BTreeMap::new();
            for record in values {
                let mut keys: Vec<String> = record.keys().cloned().collect();
                keys.sort();
                groups.entry(keys).or_default().push(record);
            }

            for (keys, group_records) in groups {
                let mut builder = QueryBuilder::<Postgres>::new("WITH inserted AS (");
                builder.push(format!("INSERT INTO {table} ("));

                let attributes: Vec<_> = Self::persisted_attributes(schema)
                    .into_iter()
                    .filter(|a| keys.iter().any(|k| k == a.id))
                    .collect();

                {
                    let mut separated = builder.separated(", ");
                    separated.push(Self::quoted_system_column(COLUMN_ID)?);
                    separated.push(Self::quoted_system_column(COLUMN_CREATED_AT)?);
                    separated.push(Self::quoted_system_column(COLUMN_UPDATED_AT)?);
                    separated.push(Self::quoted_system_column(COLUMN_PERMISSIONS)?);
                    for attribute in &attributes {
                        separated.push(Self::quote_identifier(attribute.column)?);
                    }
                }

                builder.push(") ");

                let mut row_data = Vec::with_capacity(group_records.len());

                builder.push_values(group_records, |mut separated, record| {
                    let uid = Self::extract_string(&record, FIELD_ID).unwrap_or_default();
                    let created_at = Self::extract_optional_timestamp(&record, FIELD_CREATED_AT)
                        .unwrap_or_default();
                    let updated_at = Self::extract_optional_timestamp(&record, FIELD_UPDATED_AT)
                        .unwrap_or_default();
                    let permissions =
                        Self::extract_optional_string_array(&record, FIELD_PERMISSIONS)
                            .unwrap_or_default();

                    separated.push_bind(uid);
                    separated.push_bind(created_at);
                    separated.push_bind(updated_at);
                    separated.push_bind(permissions.clone());

                    for attribute in &attributes {
                        let value = record.get(attribute.id).unwrap(); // Guaranteed by grouping
                        Self::append_bind(&mut separated, value).unwrap();
                        if attribute.kind == AttributeKind::Json {
                            separated.push_unseparated("::jsonb");
                        }
                    }

                    row_data.push((permissions, record));
                });

                builder.push(format!(
                    " RETURNING {}, {} ",
                    Self::quoted_system_column(COLUMN_SEQUENCE)?,
                    Self::quoted_system_column(COLUMN_ID)?
                ));
                builder.push(")");

                let perms_table = Self::qualified_permissions_table_name(&context, schema.id)?;

                let mut perm_data_count = 0;

                for (permissions, record) in &row_data {
                    let uid = Self::extract_string(record, FIELD_ID).unwrap_or_default();
                    let grouped = Self::permission_rows(permissions)?;
                    for (pt, pv) in grouped {
                        if perm_data_count > 0 {
                            builder.push(" UNION ALL ");
                        } else {
                            builder.push(", perm_data AS (");
                        }
                        builder.push("SELECT ");
                        builder.push_bind(uid.clone());
                        builder.push(" as uid, ");
                        builder.push_bind(pt);
                        builder.push(" as pt, ");
                        builder.push_bind(pv);
                        builder.push(" as pv");
                        perm_data_count += 1;
                    }
                }

                if perm_data_count > 0 {
                    builder.push(") ");
                    builder.push(format!(", sync_perms AS (INSERT INTO {perms_table} (document_id, permission_type, permissions) "));
                    builder.push("SELECT inserted.");
                    builder.push(Self::quoted_system_column(COLUMN_SEQUENCE)?);
                    builder.push(", p.pt, p.pv FROM inserted ");
                    builder.push("JOIN perm_data p ON inserted.");
                    builder.push(Self::quoted_system_column(COLUMN_ID)?);
                    builder.push(" = p.uid) ");
                }

                builder.push("SELECT * FROM inserted ORDER BY ");
                builder.push(Self::quoted_system_column(COLUMN_SEQUENCE)?);
                builder.push(" ASC");

                let rows = builder.build().fetch_all(pool).await.map_err(|error| {
                    DatabaseError::Other(format!(
                        "postgres single-roundtrip insert_many failed: {error}"
                    ))
                })?;

                for (row, (_permissions, mut record)) in rows.into_iter().zip(row_data.into_iter())
                {
                    let sequence = row.try_get::<i64, _>(COLUMN_SEQUENCE).map_err(|error| {
                        DatabaseError::Other(format!("postgres sequence read failed: {error}"))
                    })?;

                    record.insert(FIELD_SEQUENCE.to_string(), StorageValue::Int(sequence));
                    results.push(record);
                }
            }

            Ok(results)
        })
    }

    fn get(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        id: &str,
    ) -> AdapterFuture<'_, Result<Option<StorageRecord>, DatabaseError>> {
        let pool = self.pool;
        let context = context.clone();
        let id = id.to_string();

        Box::pin(async move {
            Self::validate_context_support(&context)?;

            let table = Self::qualified_table_name(&context, schema.id)?;
            let select_columns = Self::select_columns(schema)?;
            let mut builder = QueryBuilder::<Postgres>::new(format!(
                "SELECT {select_columns} FROM {table} AS main"
            ));
            let mut has_conditions = false;
            Self::push_condition_separator(&mut builder, &mut has_conditions);
            builder.push(format!(
                "main.{} = ",
                Self::quoted_system_column(COLUMN_ID)?
            ));
            builder.push_bind(id);
            Self::push_document_action_condition(
                &mut builder,
                &context,
                schema,
                "main",
                PermissionEnum::Read,
                &mut has_conditions,
            )?;

            let row = builder
                .build()
                .fetch_optional(pool)
                .await
                .map_err(|error| {
                    DatabaseError::Other(format!("postgres select failed: {error}"))
                })?;

            row.map(|row| Self::row_to_record(&row, schema)).transpose()
        })
    }

    fn update(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        id: &str,
        values: StorageRecord,
    ) -> AdapterFuture<'_, Result<Option<StorageRecord>, DatabaseError>> {
        let pool = self.pool;
        let context = context.clone();
        let id = id.to_string();

        Box::pin(async move {
            Self::validate_context_support(&context)?;

            let table = Self::qualified_table_name(&context, schema.id)?;
            let permissions_update = values.contains_key(FIELD_PERMISSIONS);
            let permissions = Self::extract_optional_string_array(&values, FIELD_PERMISSIONS)?;
            let attributes: Vec<_> = Self::persisted_attributes(schema)
                .into_iter()
                .filter(|attribute| values.contains_key(attribute.id))
                .collect();

            let select_columns = Self::select_columns(schema)?;

            if attributes.is_empty() && !permissions_update {
                return self.get(&context, schema, &id).await;
            }

            let mut builder = QueryBuilder::<Postgres>::new("WITH updated AS (");
            builder.push(format!("UPDATE {table} AS main SET "));
            Self::push_update_assignments(
                &mut builder,
                schema,
                &values,
                permissions_update,
                &permissions,
                &attributes,
            )?;
            let mut has_conditions = false;
            Self::push_condition_separator(&mut builder, &mut has_conditions);
            builder.push(format!(
                "main.{} = ",
                Self::quoted_system_column(COLUMN_ID)?
            ));
            builder.push_bind(id);
            Self::push_document_action_condition(
                &mut builder,
                &context,
                schema,
                "main",
                PermissionEnum::Update,
                &mut has_conditions,
            )?;
            builder.push(format!(" RETURNING {select_columns})"));

            if permissions_update {
                let perms_table = Self::qualified_permissions_table_name(&context, schema.id)?;
                let grouped_perms = Self::permission_rows(&permissions)?;

                builder.push(format!(", clear_perms AS (DELETE FROM {perms_table} WHERE document_id = (SELECT _sequence FROM updated))"));

                if !grouped_perms.is_empty() {
                    builder.push(format!(", sync_perms AS (INSERT INTO {perms_table} (document_id, permission_type, permissions) "));
                    let mut first = true;
                    for (pt, pv) in grouped_perms {
                        if !first {
                            builder.push(" UNION ALL ");
                        }
                        builder.push("SELECT (SELECT _sequence FROM updated), ");
                        builder.push_bind(pt);
                        builder.push(", ");
                        builder.push_bind(pv);
                        first = false;
                    }
                    builder.push(") ");
                }
            }

            builder.push("SELECT * FROM updated");

            let row = builder
                .build()
                .fetch_optional(pool)
                .await
                .map_err(|error| {
                    DatabaseError::Other(format!(
                        "postgres single-roundtrip update failed: {error}"
                    ))
                })?;

            let Some(row) = row else {
                return Ok(None);
            };

            Ok(Some(Self::row_to_record(&row, schema)?))
        })
    }

    fn update_many(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        query: &QuerySpec,
        values: StorageRecord,
    ) -> AdapterFuture<'_, Result<u64, DatabaseError>> {
        let pool = self.pool;
        let context = context.clone();
        let query = query.clone();

        Box::pin(async move {
            Self::validate_context_support(&context)?;

            let table = Self::qualified_table_name(&context, schema.id)?;
            let permissions_update = values.contains_key(FIELD_PERMISSIONS);
            let permissions = Self::extract_optional_string_array(&values, FIELD_PERMISSIONS)?;
            let attributes: Vec<_> = Self::persisted_attributes(schema)
                .into_iter()
                .filter(|attribute| values.contains_key(attribute.id))
                .collect();

            if attributes.is_empty() && !permissions_update {
                return Ok(0);
            }

            let mut builder = QueryBuilder::<Postgres>::new("WITH updated AS (");
            builder.push(format!("UPDATE {table} AS main SET "));
            Self::push_update_assignments(
                &mut builder,
                schema,
                &values,
                permissions_update,
                &permissions,
                &attributes,
            )?;

            let mut has_conditions = false;
            Self::push_document_action_condition(
                &mut builder,
                &context,
                schema,
                "main",
                PermissionEnum::Update,
                &mut has_conditions,
            )?;

            for filter in query.filters() {
                Self::push_condition_separator(&mut builder, &mut has_conditions);
                Self::push_filter(&mut builder, schema, filter)?;
            }

            builder.push(format!(
                " RETURNING main.{}",
                Self::quoted_system_column(COLUMN_SEQUENCE)?
            ));
            builder.push(")");

            if permissions_update {
                let perms_table = Self::qualified_permissions_table_name(&context, schema.id)?;
                let grouped_perms = Self::permission_rows(&permissions)?;

                builder.push(format!(", clear_perms AS (DELETE FROM {perms_table} WHERE document_id IN (SELECT _sequence FROM updated))"));

                if !grouped_perms.is_empty() {
                    builder.push(format!(", sync_perms AS (INSERT INTO {perms_table} (document_id, permission_type, permissions) "));
                    builder.push("SELECT updated._sequence, p.pt, p.pv FROM updated, ");
                    builder.push(" (");
                    let mut first = true;
                    for (pt, pv) in grouped_perms {
                        if !first {
                            builder.push(" UNION ALL ");
                        }
                        builder.push("SELECT ");
                        builder.push_bind(pt);
                        builder.push(" as pt, ");
                        builder.push_bind(pv);
                        builder.push(" as pv");
                        first = false;
                    }
                    builder.push(") AS p) ");
                }
            }

            builder.push("SELECT COUNT(*) FROM updated");

            let row = builder.build().fetch_one(pool).await.map_err(|error| {
                DatabaseError::Other(format!(
                    "postgres single-roundtrip update_many failed: {error}"
                ))
            })?;

            Ok(row.try_get::<i64, _>(0).unwrap_or(0) as u64)
        })
    }

    fn delete(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        id: &str,
    ) -> AdapterFuture<'_, Result<bool, DatabaseError>> {
        let pool = self.pool;
        let context = context.clone();
        let id = id.to_string();

        Box::pin(async move {
            Self::validate_context_support(&context)?;

            let table = Self::qualified_table_name(&context, schema.id)?;
            let mut builder = QueryBuilder::<Postgres>::new(format!("DELETE FROM {table} AS main"));
            let mut has_conditions = false;
            Self::push_condition_separator(&mut builder, &mut has_conditions);
            builder.push(format!(
                "main.{} = ",
                Self::quoted_system_column(COLUMN_ID)?
            ));
            builder.push_bind(id);
            Self::push_document_action_condition(
                &mut builder,
                &context,
                schema,
                "main",
                PermissionEnum::Delete,
                &mut has_conditions,
            )?;

            builder
                .build()
                .execute(pool)
                .await
                .map(|result| result.rows_affected() > 0)
                .map_err(|error| DatabaseError::Other(format!("postgres delete failed: {error}")))
        })
    }

    fn delete_many(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        query: &QuerySpec,
    ) -> AdapterFuture<'_, Result<u64, DatabaseError>> {
        let pool = self.pool;
        let context = context.clone();
        let query = query.clone();

        Box::pin(async move {
            Self::validate_context_support(&context)?;

            let table = Self::qualified_table_name(&context, schema.id)?;
            let mut builder = QueryBuilder::<Postgres>::new(format!("DELETE FROM {table} AS main"));

            let mut has_conditions = false;
            Self::push_document_action_condition(
                &mut builder,
                &context,
                schema,
                "main",
                PermissionEnum::Delete,
                &mut has_conditions,
            )?;

            for filter in query.filters() {
                Self::push_condition_separator(&mut builder, &mut has_conditions);
                Self::push_filter(&mut builder, schema, filter)?;
            }

            builder
                .build()
                .execute(pool)
                .await
                .map(|result| result.rows_affected())
                .map_err(|error| {
                    DatabaseError::Other(format!("postgres delete_many failed: {error}"))
                })
        })
    }

    fn find(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        query: &QuerySpec,
    ) -> AdapterFuture<'_, Result<Vec<StorageRecord>, DatabaseError>> {
        let pool = self.pool;
        let context = context.clone();
        let query = query.clone();

        Box::pin(async move {
            Self::validate_context_support(&context)?;

            let table = Self::qualified_table_name(&context, schema.id)?;
            let select_columns = Self::select_columns(schema)?;
            let mut builder = QueryBuilder::<Postgres>::new(format!(
                "SELECT {select_columns} FROM {table} AS main"
            ));
            let mut has_conditions = false;

            Self::push_document_action_condition(
                &mut builder,
                &context,
                schema,
                "main",
                PermissionEnum::Read,
                &mut has_conditions,
            )?;
            Self::push_filters(&mut builder, schema, &query, &mut has_conditions)?;
            Self::push_sorts(&mut builder, schema, &query)?;

            if let Some(limit) = query.limit_value() {
                builder.push(" LIMIT ");
                builder.push_bind(limit as i64);
            }

            if let Some(offset) = query.offset_value() {
                builder.push(" OFFSET ");
                builder.push_bind(offset as i64);
            }

            let rows =
                builder.build().fetch_all(pool).await.map_err(|error| {
                    DatabaseError::Other(format!("postgres find failed: {error}"))
                })?;

            rows.iter()
                .map(|row| Self::row_to_record(row, schema))
                .collect()
        })
    }

    fn count(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        query: &QuerySpec,
    ) -> AdapterFuture<'_, Result<u64, DatabaseError>> {
        let pool = self.pool;
        let context = context.clone();
        let query = query.clone();

        Box::pin(async move {
            Self::validate_context_support(&context)?;

            let table = Self::qualified_table_name(&context, schema.id)?;
            let mut builder = QueryBuilder::<Postgres>::new(format!(
                "SELECT COUNT(*) as count FROM {table} AS main"
            ));
            let mut has_conditions = false;

            Self::push_document_action_condition(
                &mut builder,
                &context,
                schema,
                "main",
                PermissionEnum::Read,
                &mut has_conditions,
            )?;
            Self::push_filters(&mut builder, schema, &query, &mut has_conditions)?;

            let row =
                builder.build().fetch_one(pool).await.map_err(|error| {
                    DatabaseError::Other(format!("postgres count failed: {error}"))
                })?;

            row.try_get::<i64, _>("count")
                .map(|value| value as u64)
                .map_err(|error| {
                    DatabaseError::Other(format!("postgres count decode failed: {error}"))
                })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::PostgresAdapter;
    use database_core::errors::DatabaseError;
    use database_core::utils::{PermissionEnum, Role};
    use database_core::{
        AttributeKind, AttributePersistence, AttributeSchema, CollectionSchema, Context,
    };
    use sqlx::{Execute, Postgres, QueryBuilder};

    const ATTRIBUTES: &[AttributeSchema] = &[AttributeSchema {
        id: "name",
        column: "name",
        kind: AttributeKind::String,
        required: true,
        array: false,
        persistence: AttributePersistence::Persisted,
        filters: &[],
        relationship: None,
    }];

    static RESTRICTED_USERS: CollectionSchema = CollectionSchema {
        id: "restricted_users",
        name: "RestrictedUsers",
        document_security: true,
        enabled: true,
        permissions: &["read(\"user:admin\")"],
        attributes: ATTRIBUTES,
        indexes: &[],
    };

    #[test]
    fn validates_identifiers() {
        assert!(PostgresAdapter::is_valid_identifier("users"));
        assert!(PostgresAdapter::is_valid_identifier("_tenant"));
        assert!(!PostgresAdapter::is_valid_identifier("1users"));
        assert!(!PostgresAdapter::is_valid_identifier("users-perms"));
    }

    #[test]
    fn document_action_roles_use_fallback_only_when_collection_action_is_denied() {
        let admin_context =
            Context::default().with_role(Role::user("admin", None).expect("role should parse"));
        let reader_context =
            Context::default().with_role(Role::user("reader", None).expect("role should parse"));

        let admin_roles = PostgresAdapter::document_action_roles(
            &admin_context,
            &RESTRICTED_USERS,
            PermissionEnum::Read,
        )
        .expect("roles");
        let reader_roles = PostgresAdapter::document_action_roles(
            &reader_context,
            &RESTRICTED_USERS,
            PermissionEnum::Read,
        )
        .expect("roles");

        assert!(admin_roles.is_none());
        assert_eq!(
            reader_roles,
            Some(vec!["any".to_string(), "user:reader".to_string()])
        );
    }

    #[test]
    fn builds_document_read_sql_condition() {
        let context =
            Context::default().with_role(Role::user("reader", None).expect("role should parse"));
        let mut builder = QueryBuilder::<Postgres>::new("SELECT 1 FROM users AS main");
        let mut has_conditions = false;

        PostgresAdapter::push_document_action_condition(
            &mut builder,
            &context,
            &RESTRICTED_USERS,
            "main",
            PermissionEnum::Read,
            &mut has_conditions,
        )
        .expect("condition should build");

        let query = builder.build();
        let sql = query.sql();

        assert!(sql.contains("EXISTS (SELECT 1 FROM \"public\".\"restricted_users_perms\" AS p"));
        assert!(sql.contains("p.\"document_id\" = \"main\".\"_id\""));
        assert!(sql.contains("p.\"permission_type\" = "));
        assert!(sql.contains("p.\"permissions\" && "));
    }

    #[test]
    fn write_permissions_expand_to_update_and_delete_rows() {
        let rows = PostgresAdapter::permission_rows(&["write(\"user:editor\")".to_string()])
            .expect("permission rows should build");

        assert_eq!(rows.get("write"), Some(&vec!["user:editor".to_string()]));
        assert_eq!(rows.get("create"), Some(&vec!["user:editor".to_string()]));
        assert_eq!(rows.get("update"), Some(&vec!["user:editor".to_string()]));
        assert_eq!(rows.get("delete"), Some(&vec!["user:editor".to_string()]));
    }

    #[test]
    fn unsupported_tenant_modes_fail_fast() {
        let shared_tables =
            PostgresAdapter::validate_context_support(&Context::default().with_shared_tables(true))
                .expect_err("shared tables should fail fast");
        assert!(matches!(shared_tables, DatabaseError::Other(_)));

        let tenant_context =
            PostgresAdapter::validate_context_support(&Context::default().with_tenant_id("acme"))
                .expect_err("tenant id should fail fast");
        assert!(matches!(tenant_context, DatabaseError::Other(_)));

        let tenant_per_document = PostgresAdapter::validate_context_support(
            &Context::default().with_tenant_per_document(true),
        )
        .expect_err("tenant-per-document should fail fast");
        assert!(matches!(tenant_per_document, DatabaseError::Other(_)));
    }
}
