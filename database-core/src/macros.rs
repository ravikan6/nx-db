#[macro_export]
macro_rules! and {
    ($($filter:expr),* $(,)?) => {
        $crate::Filter::and(vec![$($filter),*])
    };
}

#[macro_export]
macro_rules! or {
    ($($filter:expr),* $(,)?) => {
        $crate::Filter::or(vec![$($filter),*])
    };
}

#[macro_export]
macro_rules! not {
    ($filter:expr) => {
        $crate::Filter::not($filter)
    };
}

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

#[macro_export]
macro_rules! db_insert {
    ($model:ty { $($field:ident : $val:expr),* $(,)? }) => {
        {
            use $crate::Model;
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
                $crate::insert_value(&mut record, $crate::FIELD_ID, input.id);
                $crate::insert_value(&mut record, $crate::FIELD_PERMISSIONS, input.permissions);
                $(
                    $crate::impl_model!(@insert_create record, $field_id, input.$field_name $(, $encoder)?);
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

            fn entity_from_record(mut record: $crate::traits::storage::StorageRecord, _context: &$crate::Context) -> Result<Self::Entity, $crate::errors::DatabaseError> {
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
                })
            }
        }
    };

    // Helper for create_to_record
    (@insert_create $record:ident, $id:expr, $val:expr) => {
        $crate::insert_value(&mut $record, $id, $val);
    };
    (@insert_create $record:ident, $id:expr, $val:expr, $encoder:ident) => {
        $crate::insert_value(&mut $record, $id, $encoder($val)?);
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
