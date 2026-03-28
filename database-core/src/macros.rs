/// Combine multiple [`Filter`]s with logical AND.
#[macro_export]
macro_rules! and {
    ($($filter:expr),* $(,)?) => {
        $crate::Filter::and(vec![$($filter),*])
    };
}

/// Combine multiple [`Filter`]s with logical OR.
#[macro_export]
macro_rules! or {
    ($($filter:expr),* $(,)?) => {
        $crate::Filter::or(vec![$($filter),*])
    };
}

/// Negate a [`Filter`].
#[macro_export]
macro_rules! not {
    ($filter:expr) => {
        $crate::Filter::not($filter)
    };
}

/// Build a [`Context`] with common options.
///
/// ```rust,ignore
/// let ctx = db_context!(schema: "myapp", role: Role::any());
/// ```
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

/// Build a [`StaticRegistry`] from a list of collection schema references.
///
/// ```rust,ignore
/// let registry = db_registry!(&USERS_SCHEMA, &POSTS_SCHEMA);
/// ```
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

/// Build a [`QuerySpec`] using named keyword arguments.
///
/// ```rust,ignore
/// let q = db_query!(filter: User::NAME.eq("Ravi"), limit: 10);
/// ```
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
    (@apply $query:ident, select, $val:expr) => {
        $query.select($val)
    };
    (@apply $query:ident, include, $val:expr) => {
        $query.include($val)
    };
    (@apply $query:ident, limit, $val:expr) => {
        $query.limit($val)
    };
    (@apply $query:ident, offset, $val:expr) => {
        $query.offset($val)
    };
    (@apply $query:ident, and, $val:expr) => {
        $query.filter($crate::Filter::And($val))
    };
    (@apply $query:ident, or, $val:expr) => {
        $query.filter($crate::Filter::Or($val))
    };
    (@apply $query:ident, not, $val:expr) => {
        $query.filter($crate::Filter::Not(Box::new($val)))
    };
}

/// Generate a type-safe enum for a collection attribute.
///
/// This macro implements common traits needed for database integration:
/// `Display`, `FromStr`, `Serialize`, `Deserialize`, `IntoStorage`, and `FromStorage`.
#[macro_export]
macro_rules! impl_enum {
    (
        name: $name:ident,
        variants: { $($variant:ident => $string:expr),* $(,)? }
    ) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, ::serde::Serialize, ::serde::Deserialize)]
        #[serde(rename_all = "lowercase")]
        pub enum $name {
            $($variant),*
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    $(Self::$variant => write!(f, $string)),*
                }
            }
        }

        impl std::str::FromStr for $name {
            type Err = $crate::errors::DatabaseError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s {
                    $($string => Ok(Self::$variant),)*
                    _ => Err($crate::errors::DatabaseError::Other(format!("invalid enum value for {}: {}", stringify!($name), s))),
                }
            }
        }

        impl $crate::IntoStorage for $name {
            fn into_storage(self) -> $crate::traits::storage::StorageValue {
                $crate::traits::storage::StorageValue::Enum(self.to_string())
            }
        }

        impl $crate::FromStorage for $name {
            fn from_storage(value: $crate::traits::storage::StorageValue) -> Result<Self, $crate::errors::DatabaseError> {
                match value {
                    $crate::traits::storage::StorageValue::Enum(s) => s.parse(),
                    $crate::traits::storage::StorageValue::String(s) => s.parse(),
                    _ => Err($crate::errors::DatabaseError::Other(format!("expected enum storage value, got {:?}", value))),
                }
            }
        }
    };
}

/// Generate builder helpers for a generated `Create*` payload.
///
/// This keeps generated code compact while preserving required constructor
/// arguments and chaining for optional/system fields.
#[macro_export]
macro_rules! impl_create_builder {
    (
        create: $create_name:ident,
        id: $id_name:ident,
        required: { $($required_field:ident : $required_ty:ty),* $(,)? },
        optional: { $($optional_field:ident : $optional_ty:ty),* $(,)? }
    ) => {
        impl $create_name {
            pub fn new($($required_field: $required_ty),*) -> Self {
                Self {
                    id: None,
                    $($required_field,)*
                    $($optional_field: Default::default(),)*
                    permissions: Vec::new(),
                }
            }

            pub fn builder($($required_field: $required_ty),*) -> Self {
                Self::new($($required_field),*)
            }

            pub fn with_id(mut self, id: $id_name) -> Self {
                self.id = Some(id);
                self
            }

            pub fn id(self, id: $id_name) -> Self {
                self.with_id(id)
            }

            pub fn with_permissions(mut self, permissions: Vec<String>) -> Self {
                self.permissions = permissions;
                self
            }

            pub fn permissions(self, permissions: Vec<String>) -> Self {
                self.with_permissions(permissions)
            }

            $(
                pub fn $optional_field(mut self, value: $optional_ty) -> Self {
                    self.$optional_field = value;
                    self
                }
            )*
        }
    };
}

/// Generate top-level and associated field constants for a model.
#[macro_export]
macro_rules! impl_model_fields {
    (
        model: $model_name:ident,
        plain: { $($plain_top:ident => $plain_assoc:ident : $plain_ty:ty = $plain_name:expr),* $(,)? },
        encoded: { $($encoded_top:ident => $encoded_assoc:ident : $encoded_ty:ty = $encoded_name:expr => $encoded_fn:expr),* $(,)? }
    ) => {
        impl $model_name {
            $(
                pub const $plain_assoc: $crate::Field<$model_name, $plain_ty> =
                    $crate::Field::new($plain_name);
            )*

            $(
                pub const $encoded_assoc: $crate::EncodedField<$model_name, $encoded_ty> =
                    $crate::EncodedField::new($encoded_name, $encoded_fn);
            )*
        }

        $(
            pub const $plain_top: $crate::Field<$model_name, $plain_ty> =
                $crate::Field::new($plain_name);
        )*

        $(
            pub const $encoded_top: $crate::EncodedField<$model_name, $encoded_ty> =
                $crate::EncodedField::new($encoded_name, $encoded_fn);
        )*
    };
}

/// Declare a model marker, singleton constant, field constants, and record
/// bridge in one place.
#[macro_export]
macro_rules! declare_model {
    (
        name: $model_name:ident,
        const: $model_const:ident,
        entity: $entity_name:ty,
        create: $create_name:ty,
        update: $update_name:ty,
        schema: $schema_const:ident,
        plain: { $($plain_top:ident => $plain_assoc:ident : $plain_ty:ty = $plain_name:expr),* $(,)? },
        encoded: { $($encoded_top:ident => $encoded_assoc:ident : $encoded_ty:ty = $encoded_name:expr => $encoded_fn:expr),* $(,)? }
    ) => {
        #[derive(Debug, Clone, Copy)]
        pub struct $model_name;

        pub const $model_const: $model_name = $model_name;

        $crate::impl_model_fields! {
            model: $model_name,
            plain: { $($plain_top => $plain_assoc : $plain_ty = $plain_name),* },
            encoded: { $($encoded_top => $encoded_assoc : $encoded_ty = $encoded_name => $encoded_fn),* }
        }

        $crate::impl_model_record_bridge! {
            name: $model_name,
            entity: $entity_name,
            create: $create_name,
            update: $update_name,
            schema: $schema_const
        }
    };
}

/// Generate the helper functions and populate descriptor for a to-one relation.
#[macro_export]
macro_rules! impl_populate_one {
    (
        const: $const_name:ident,
        local_fn: $local_fn:ident,
        remote_fn: $remote_fn:ident,
        set_fn: $set_fn:ident,
        model: $model_name:ident,
        related_model: $related_model_name:ident,
        entity: $entity_name:ident,
        related_entity: $related_entity_name:ident,
        rel: $rel_const:ident,
        field: $field_name:ident,
        local_key: |$local_entity:ident| $local_key:expr,
        remote_key: |$remote_entity:ident| $remote_key:expr
    ) => {
        fn $local_fn($local_entity: &$entity_name) -> Option<String> {
            $local_key
        }

        fn $remote_fn($remote_entity: &$related_entity_name) -> Option<String> {
            $remote_key
        }

        fn $set_fn(entity: &mut $entity_name, value: $crate::RelationOne<$related_entity_name>) {
            entity.$field_name = value;
        }

        pub const $const_name: $crate::PopulateOne<$model_name, $related_model_name> =
            $crate::PopulateOne::new($rel_const, $local_fn, $remote_fn, $set_fn);
    };
}

/// Generate a relation descriptor plus the helper functions and populate
/// descriptor for a to-one relation.
#[macro_export]
macro_rules! impl_relation_one {
    (
        rel_const: $rel_const:ident,
        populate_const: $populate_const:ident,
        rel_expr: $rel_expr:expr,
        local_fn: $local_fn:ident,
        remote_fn: $remote_fn:ident,
        set_fn: $set_fn:ident,
        model: $model_name:ident,
        related_model: $related_model_name:ident,
        entity: $entity_name:ident,
        related_entity: $related_entity_name:ident,
        field: $field_name:ident,
        local_key: |$local_entity:ident| $local_key:expr,
        remote_key: |$remote_entity:ident| $remote_key:expr
    ) => {
        pub const $rel_const: $crate::Rel<$model_name, $related_model_name> = $rel_expr;

        $crate::impl_populate_one! {
            const: $populate_const,
            local_fn: $local_fn,
            remote_fn: $remote_fn,
            set_fn: $set_fn,
            model: $model_name,
            related_model: $related_model_name,
            entity: $entity_name,
            related_entity: $related_entity_name,
            rel: $rel_const,
            field: $field_name,
            local_key: |$local_entity| $local_key,
            remote_key: |$remote_entity| $remote_key
        }
    };
}

/// Generate the helper functions and populate descriptor for a to-many relation.
#[macro_export]
macro_rules! impl_populate_many {
    (
        const: $const_name:ident,
        local_fn: $local_fn:ident,
        remote_fn: $remote_fn:ident,
        set_fn: $set_fn:ident,
        model: $model_name:ident,
        related_model: $related_model_name:ident,
        entity: $entity_name:ident,
        related_entity: $related_entity_name:ident,
        rel: $rel_const:ident,
        field: $field_name:ident,
        local_key: |$local_entity:ident| $local_key:expr,
        remote_key: |$remote_entity:ident| $remote_key:expr
    ) => {
        fn $local_fn($local_entity: &$entity_name) -> String {
            $local_key
        }

        fn $remote_fn($remote_entity: &$related_entity_name) -> Option<String> {
            $remote_key
        }

        fn $set_fn(entity: &mut $entity_name, value: $crate::RelationMany<$related_entity_name>) {
            entity.$field_name = value;
        }

        pub const $const_name: $crate::PopulateMany<$model_name, $related_model_name> =
            $crate::PopulateMany::new($rel_const, $local_fn, $remote_fn, $set_fn);
    };
}

/// Generate a relation descriptor plus the helper functions and populate
/// descriptor for a to-many relation.
#[macro_export]
macro_rules! impl_relation_many {
    (
        rel_const: $rel_const:ident,
        populate_const: $populate_const:ident,
        rel_expr: $rel_expr:expr,
        local_fn: $local_fn:ident,
        remote_fn: $remote_fn:ident,
        set_fn: $set_fn:ident,
        model: $model_name:ident,
        related_model: $related_model_name:ident,
        entity: $entity_name:ident,
        related_entity: $related_entity_name:ident,
        field: $field_name:ident,
        local_key: |$local_entity:ident| $local_key:expr,
        remote_key: |$remote_entity:ident| $remote_key:expr
    ) => {
        pub const $rel_const: $crate::Rel<$model_name, $related_model_name> = $rel_expr;

        $crate::impl_populate_many! {
            const: $populate_const,
            local_fn: $local_fn,
            remote_fn: $remote_fn,
            set_fn: $set_fn,
            model: $model_name,
            related_model: $related_model_name,
            entity: $entity_name,
            related_entity: $related_entity_name,
            rel: $rel_const,
            field: $field_name,
            local_key: |$local_entity| $local_key,
            remote_key: |$remote_entity| $remote_key
        }
    };
}

/// Bridge derive-generated record mappings into a full [`Model`] impl.
#[macro_export]
macro_rules! impl_model_record_bridge {
    (
        name: $model_name:ident,
        entity: $entity_name:ty,
        create: $create_name:ty,
        update: $update_name:ty,
        schema: $schema_const:ident
    ) => {
        impl $crate::Model for $model_name {
            type Id = <$entity_name as $crate::EntityRecord>::Id;
            type Entity = $entity_name;
            type Create = $create_name;
            type Update = $update_name;

            fn schema() -> &'static $crate::CollectionSchema {
                &$schema_const
            }

            fn entity_to_id(entity: &Self::Entity) -> &Self::Id {
                <$entity_name as $crate::EntityRecord>::entity_to_id(entity)
            }

            fn entity_metadata(entity: &Self::Entity) -> &$crate::Metadata {
                <$entity_name as $crate::EntityRecord>::entity_metadata(entity)
            }

            fn create_to_record(
                input: Self::Create,
                context: &$crate::Context,
            ) -> Result<$crate::traits::storage::StorageRecord, $crate::errors::DatabaseError> {
                <$create_name as $crate::CreateRecord>::create_to_record(input, context)
            }

            fn update_to_record(
                input: Self::Update,
                context: &$crate::Context,
            ) -> Result<$crate::traits::storage::StorageRecord, $crate::errors::DatabaseError> {
                <$update_name as $crate::UpdateRecord>::update_to_record(input, context)
            }

            fn entity_from_record(
                record: $crate::traits::storage::StorageRecord,
                context: &$crate::Context,
            ) -> Result<Self::Entity, $crate::errors::DatabaseError> {
                <$entity_name as $crate::EntityRecord>::from_record(record, context)
            }

            fn resolve_entity<'a>(
                entity: Self::Entity,
                context: &'a $crate::Context,
            ) -> $crate::model::ModelFuture<'a, Result<Self::Entity, $crate::errors::DatabaseError>>
            {
                <$entity_name as $crate::EntityRecord>::resolve_entity(entity, context)
            }
        }
    };
}

/// Generate a registry function from a list of schema constants.
#[macro_export]
macro_rules! impl_registry_fn {
    (
        fn: $fn_name:ident,
        schemas: [ $($schema_const:ident),* $(,)? ]
    ) => {
        pub fn $fn_name() -> Result<$crate::StaticRegistry, $crate::errors::DatabaseError> {
            $crate::StaticRegistry::new().extend([$( &$schema_const ),*])
        }
    };
}

/// Generate a full [`Model`] implementation for a model struct.
///
/// This macro wires up the `create_to_record`, `update_to_record`, and
/// `entity_from_record` methods, optionally with custom encoder/decoder
/// functions per field, virtual (computed) fields, and async resolvers.
///
/// # Syntax
/// ```rust,ignore
/// impl_model! {
///     name: MyModel,
///     id: MyId,
///     entity: MyEntity,
///     create: CreateMy,
///     update: UpdateMy,
///     schema: MY_SCHEMA,
///     fields: {
///         "col_id" => field_name : FieldType,
///         "encoded_col" => encoded_field : StoredType [encode_fn, decode_fn] :required,
///     }
///     // optional:
///     , virtuals: { computed_field }
///     , resolvers: { computed_field : resolve_computed_field }
/// }
/// ```
#[macro_export]
macro_rules! impl_model {
    (
        name: $model_name:ident,
        id: $id_name:ident,
        entity: $entity_name:ident,
        create: $create_name:ident,
        update: $update_name:ident,
        schema: $schema_const:ident,
        fields: {
            $($field_id:expr => $field_name:ident : $field_type:ty $([ $encoder:ident, $decoder:ident ])? $(:$required:ident)?),*
        }
        $(, virtuals: { $($virtual_field:ident),* })?
        $(, loaded: { $($loaded_field:ident),* })?
        $(, loaded_one: { $($loaded_one_field:ident),* })?
        $(, loaded_many: { $($loaded_many_field:ident),* })?
        $(, resolvers: { $($resolver_field:ident : $resolver_fn:path),* })?
    ) => {
        impl $crate::Model for $model_name {
            type Id = $id_name;
            type Entity = $entity_name;
            type Create = $create_name;
            type Update = $update_name;

            fn schema() -> &'static $crate::CollectionSchema {
                &$schema_const
            }

            fn entity_to_id(entity: &Self::Entity) -> &Self::Id {
                &entity.id
            }

            fn entity_metadata(entity: &Self::Entity) -> &$crate::Metadata {
                &entity._metadata
            }

            fn resolve_entity<'a>(mut entity: Self::Entity, context: &'a $crate::Context) -> $crate::model::ModelFuture<'a, Result<Self::Entity, $crate::errors::DatabaseError>> {
                Box::pin(async move {
                    $($(
                        entity.$resolver_field = Some($resolver_fn(&entity, context).await?);
                    )*)?
                    Ok(entity)
                })
            }

            fn create_to_record(input: Self::Create, _context: &$crate::Context) -> Result<$crate::traits::storage::StorageRecord, $crate::errors::DatabaseError> {
                let mut record = $crate::traits::storage::StorageRecord::new();
                let id = match input.id {
                    Some(value) => value,
                    None => <Self::Id as $crate::GenerateId>::generate()?,
                };
                $crate::insert_value(&mut record, $crate::FIELD_ID, id);
                $crate::insert_value(&mut record, $crate::FIELD_PERMISSIONS, input.permissions);
                $(
                    $crate::impl_model!(@insert_create record, $field_id, input.$field_name; $($required)?; $($encoder)?);
                )*
                Ok(record)
            }

            fn update_to_record(input: Self::Update, _context: &$crate::Context) -> Result<$crate::traits::storage::StorageRecord, $crate::errors::DatabaseError> {
                let mut record = $crate::traits::storage::StorageRecord::new();
                if let $crate::Patch::Set(value) = input.permissions {
                    $crate::insert_value(&mut record, $crate::FIELD_PERMISSIONS, value);
                }
                $(
                    $crate::impl_model!(@insert_update record, $field_id, input.$field_name $(, $encoder)?);
                )*
                Ok(record)
            }

            fn entity_from_record(record: $crate::traits::storage::StorageRecord, _context: &$crate::Context) -> Result<Self::Entity, $crate::errors::DatabaseError> {
                Ok($entity_name {
                    _metadata: $crate::Metadata {
                        sequence: $crate::get_required(&record, $crate::FIELD_SEQUENCE)?,
                        created_at: $crate::get_required(&record, $crate::FIELD_CREATED_AT)?,
                        updated_at: $crate::get_required(&record, $crate::FIELD_UPDATED_AT)?,
                        permissions: $crate::get_required(&record, $crate::FIELD_PERMISSIONS)?,
                    },
                    id: $crate::get_required(&record, $crate::FIELD_ID)?,
                    $(
                        $field_name: $crate::impl_model!(@get_field record, $field_id, $field_type $(, $decoder)? $(:$required)?),
                    )*
                    $($(
                        $virtual_field: None,
                    )*)?
                    $($(
                        $loaded_field: $crate::Populated::NotLoaded,
                    )*)?
                    $($(
                        $loaded_one_field: $crate::RelationOne::NotLoaded,
                    )*)?
                    $($(
                        $loaded_many_field: $crate::RelationMany::NotLoaded,
                    )*)?
                })
            }
        }
    };

    // Helper for create_to_record
    (@insert_create $record:ident, $id:expr, $val:expr; required;) => {
        $crate::insert_value(&mut $record, $id, $val);
    };
    (@insert_create $record:ident, $id:expr, $val:expr; required; $encoder:ident) => {
        $crate::insert_value(&mut $record, $id, $encoder($val)?);
    };
    (@insert_create $record:ident, $id:expr, $val:expr;;) => {
        if let Some(value) = $val {
            $crate::insert_value(&mut $record, $id, value);
        }
    };
    (@insert_create $record:ident, $id:expr, $val:expr;; $encoder:ident) => {
        if let Some(value) = $val {
            $crate::insert_value(&mut $record, $id, $encoder(Some(value))?);
        }
    };

    // Helper for update_to_record
    (@insert_update $record:ident, $id:expr, $val:expr) => {
        if let $crate::Patch::Set(value) = $val {
            $crate::insert_value(&mut $record, $id, value);
        }
    };
    (@insert_update $record:ident, $id:expr, $val:expr, $encoder:ident) => {
        if let $crate::Patch::Set(value) = $val {
            $crate::insert_value(&mut $record, $id, $encoder(value)?);
        }
    };

    // Helper for entity_from_record
    (@get_field $record:ident, $id:expr, $type:ty, $decoder:ident :required) => {
        $decoder($crate::get_required(&$record, $id)?)?
    };
    (@get_field $record:ident, $id:expr, $type:ty, $decoder:ident) => {
        $decoder($crate::get_optional(&$record, $id)?)?
    };
    (@get_field $record:ident, $id:expr, $type:ty :required) => {
        $crate::get_required::<$type>(&$record, $id)?
    };
    (@get_field $record:ident, $id:expr, $type:ty) => {
        $crate::get_optional::<$type>(&$record, $id)?.unwrap_or_default()
    };
}
