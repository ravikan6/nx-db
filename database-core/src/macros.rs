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
                        uid: $crate::get_required(&record, $crate::FIELD_ID)?,
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
