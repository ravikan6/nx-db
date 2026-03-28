use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::{
    Data, DeriveInput, Fields, GenericArgument, Ident, LitStr, Path, PathArguments, Type,
    parse_macro_input,
};

#[proc_macro_derive(NxEntity, attributes(nx))]
pub fn derive_nx_entity(input: TokenStream) -> TokenStream {
    expand_entity(parse_macro_input!(input as DeriveInput))
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

#[proc_macro_derive(NxCreate, attributes(nx))]
pub fn derive_nx_create(input: TokenStream) -> TokenStream {
    expand_create(parse_macro_input!(input as DeriveInput))
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

#[proc_macro_derive(NxUpdate, attributes(nx))]
pub fn derive_nx_update(input: TokenStream) -> TokenStream {
    expand_update(parse_macro_input!(input as DeriveInput))
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

#[derive(Clone)]
struct ParsedField {
    ident: Ident,
    ty: Type,
    role: FieldRole,
}

#[derive(Clone)]
enum FieldRole {
    Id,
    Metadata,
    Permissions,
    Persisted {
        field: String,
        required: bool,
        encode: Option<Path>,
        decode: Option<Path>,
    },
    Virtual,
    Loaded,
    LoadedOne,
    LoadedMany,
}

fn expand_entity(input: DeriveInput) -> syn::Result<TokenStream2> {
    let name = input.ident;
    let fields = parse_struct_fields(&input.data)?;
    let parsed = parse_fields(&fields)?;

    let id_field = parsed
        .iter()
        .find(|field| matches!(field.role, FieldRole::Id))
        .ok_or_else(|| {
            syn::Error::new(Span::call_site(), "NxEntity requires one #[nx(id)] field")
        })?;
    let metadata_field = parsed
        .iter()
        .find(|field| matches!(field.role, FieldRole::Metadata))
        .ok_or_else(|| {
            syn::Error::new(
                Span::call_site(),
                "NxEntity requires one #[nx(metadata)] field",
            )
        })?;

    let id_ident = &id_field.ident;
    let id_ty = &id_field.ty;
    let metadata_ident = &metadata_field.ident;

    let mut persisted_inits = Vec::new();
    let mut virtual_inits = Vec::new();
    let mut loaded_inits = Vec::new();
    let mut loaded_one_inits = Vec::new();
    let mut loaded_many_inits = Vec::new();

    for field in &parsed {
        match &field.role {
            FieldRole::Persisted {
                field: field_name,
                required,
                decode,
                ..
            } => {
                let ident = &field.ident;
                let ty = &field.ty;
                let field_name = field_name.clone();
                let value = if *required {
                    if let Some(decode) = decode {
                        quote! { #decode(::nx_db::get_required(&record, #field_name)?)? }
                    } else {
                        quote! { ::nx_db::get_required::<#ty>(&record, #field_name)? }
                    }
                } else if let Some(decode) = decode {
                    quote! { #decode(::nx_db::get_optional(&record, #field_name)?)? }
                } else {
                    quote! { ::nx_db::get_optional::<#ty>(&record, #field_name)?.unwrap_or_default() }
                };
                persisted_inits.push(quote! { #ident: #value, });
            }
            FieldRole::Virtual => {
                let ident = &field.ident;
                virtual_inits.push(quote! { #ident: None, });
            }
            FieldRole::Loaded => {
                let ident = &field.ident;
                loaded_inits.push(quote! { #ident: ::nx_db::Populated::NotLoaded, });
            }
            FieldRole::LoadedOne => {
                let ident = &field.ident;
                loaded_one_inits.push(quote! { #ident: ::nx_db::RelationOne::NotLoaded, });
            }
            FieldRole::LoadedMany => {
                let ident = &field.ident;
                loaded_many_inits.push(quote! { #ident: ::nx_db::RelationMany::NotLoaded, });
            }
            FieldRole::Id | FieldRole::Metadata | FieldRole::Permissions => {}
        }
    }

    Ok(quote! {
        impl ::nx_db::EntityRecord for #name {
            type Id = #id_ty;

            fn entity_to_id(entity: &Self) -> &Self::Id {
                &entity.#id_ident
            }

            fn entity_metadata(entity: &Self) -> &::nx_db::Metadata {
                &entity.#metadata_ident
            }

            fn from_record(
                record: ::nx_db::traits::storage::StorageRecord,
                _context: &::nx_db::Context,
            ) -> Result<Self, ::nx_db::DatabaseError> {
                Ok(Self {
                    #metadata_ident: ::nx_db::Metadata {
                        sequence: ::nx_db::get_required(&record, ::nx_db::FIELD_SEQUENCE)?,
                        created_at: ::nx_db::get_required(&record, ::nx_db::FIELD_CREATED_AT)?,
                        updated_at: ::nx_db::get_required(&record, ::nx_db::FIELD_UPDATED_AT)?,
                        permissions: ::nx_db::get_required(&record, ::nx_db::FIELD_PERMISSIONS)?,
                    },
                    #id_ident: ::nx_db::get_required(&record, ::nx_db::FIELD_ID)?,
                    #(#persisted_inits)*
                    #(#virtual_inits)*
                    #(#loaded_inits)*
                    #(#loaded_one_inits)*
                    #(#loaded_many_inits)*
                })
            }
        }
    })
}

fn expand_create(input: DeriveInput) -> syn::Result<TokenStream2> {
    let name = input.ident;
    let fields = parse_struct_fields(&input.data)?;
    let parsed = parse_fields(&fields)?;

    let id_field = parsed
        .iter()
        .find(|field| matches!(field.role, FieldRole::Id))
        .ok_or_else(|| {
            syn::Error::new(Span::call_site(), "NxCreate requires one #[nx(id)] field")
        })?;
    let id_ident = &id_field.ident;
    let id_ty = option_inner_type(&id_field.ty).ok_or_else(|| {
        syn::Error::new_spanned(&id_field.ty, "#[nx(id)] field must be Option<Id>")
    })?;

    let permissions_field = parsed
        .iter()
        .find(|field| matches!(field.role, FieldRole::Permissions));
    let permissions_ident = permissions_field.map(|field| field.ident.clone());

    let mut required_args = Vec::new();
    let mut required_names = Vec::new();
    let mut init_fields = Vec::new();
    let mut optional_setters = Vec::new();
    let mut record_fields = Vec::new();

    for field in &parsed {
        match &field.role {
            FieldRole::Persisted {
                field: field_name,
                required,
                encode,
                ..
            } => {
                let ident = &field.ident;
                let ty = &field.ty;
                if *required {
                    required_args.push(quote! { #ident: #ty });
                    required_names.push(quote! { #ident });
                    init_fields.push(quote! { #ident, });
                    if let Some(encode) = encode {
                        record_fields.push(quote! {
                            ::nx_db::insert_value(&mut record, #field_name, #encode(self.#ident)?);
                        });
                    } else {
                        record_fields.push(quote! {
                            ::nx_db::insert_value(&mut record, #field_name, self.#ident);
                        });
                    }
                } else {
                    init_fields.push(quote! { #ident: Default::default(), });
                    optional_setters.push(quote! {
                        pub fn #ident(mut self, value: #ty) -> Self {
                            self.#ident = value;
                            self
                        }
                    });
                    if let Some(encode) = encode {
                        record_fields.push(quote! {
                            if let Some(value) = self.#ident {
                                ::nx_db::insert_value(&mut record, #field_name, #encode(Some(value))?);
                            }
                        });
                    } else {
                        record_fields.push(quote! {
                            if let Some(value) = self.#ident {
                                ::nx_db::insert_value(&mut record, #field_name, value);
                            }
                        });
                    }
                }
            }
            FieldRole::Id => {
                init_fields.push(quote! { #id_ident: None, });
            }
            FieldRole::Permissions => {
                let ident = &field.ident;
                init_fields.push(quote! { #ident: Vec::new(), });
            }
            FieldRole::Metadata
            | FieldRole::Virtual
            | FieldRole::Loaded
            | FieldRole::LoadedOne
            | FieldRole::LoadedMany => {
                return Err(syn::Error::new_spanned(
                    &field.ident,
                    "NxCreate only supports #[nx(id)], #[nx(permissions)], and persisted #[nx(field = ...)] fields",
                ));
            }
        }
    }

    let permissions_init = permissions_ident
        .clone()
        .map(|ident| quote! { self.#ident })
        .unwrap_or_else(|| quote! { Vec::<String>::new() });

    let with_permissions_method = permissions_ident.as_ref().map(|ident| {
        quote! {
            pub fn with_permissions(mut self, permissions: Vec<String>) -> Self {
                self.#ident = permissions;
                self
            }

            pub fn permissions(self, permissions: Vec<String>) -> Self {
                self.with_permissions(permissions)
            }
        }
    });

    Ok(quote! {
        impl #name {
            pub fn new(#(#required_args),*) -> Self {
                Self {
                    #(#init_fields)*
                }
            }

            pub fn builder(#(#required_args),*) -> Self {
                Self::new(#(#required_names),*)
            }

            pub fn with_id(mut self, id: #id_ty) -> Self {
                self.#id_ident = Some(id);
                self
            }

            pub fn id(self, id: #id_ty) -> Self {
                self.with_id(id)
            }

            #with_permissions_method

            #(#optional_setters)*
        }

        impl ::nx_db::CreateRecord for #name {
            type Id = #id_ty;

            fn create_to_record(
                self,
                _context: &::nx_db::Context,
            ) -> Result<::nx_db::traits::storage::StorageRecord, ::nx_db::DatabaseError> {
                let mut record = ::nx_db::traits::storage::StorageRecord::new();
                let id = match self.#id_ident {
                    Some(value) => value,
                    None => <Self::Id as ::nx_db::GenerateId>::generate()?,
                };
                ::nx_db::insert_value(&mut record, ::nx_db::FIELD_ID, id);
                ::nx_db::insert_value(&mut record, ::nx_db::FIELD_PERMISSIONS, #permissions_init);
                #(#record_fields)*
                Ok(record)
            }
        }
    })
}

fn expand_update(input: DeriveInput) -> syn::Result<TokenStream2> {
    let name = input.ident;
    let fields = parse_struct_fields(&input.data)?;
    let parsed = parse_fields(&fields)?;

    let mut update_fields = Vec::new();
    let mut permissions_field = None;

    for field in &parsed {
        match &field.role {
            FieldRole::Persisted {
                field: field_name,
                encode,
                ..
            } => {
                let ident = &field.ident;
                if let Some(encode) = encode {
                    update_fields.push(quote! {
                        if let ::nx_db::Patch::Set(value) = self.#ident {
                            ::nx_db::insert_value(&mut record, #field_name, #encode(value)?);
                        }
                    });
                } else {
                    update_fields.push(quote! {
                        if let ::nx_db::Patch::Set(value) = self.#ident {
                            ::nx_db::insert_value(&mut record, #field_name, value);
                        }
                    });
                }
            }
            FieldRole::Permissions => {
                permissions_field = Some(field.ident.clone());
            }
            FieldRole::Id
            | FieldRole::Metadata
            | FieldRole::Virtual
            | FieldRole::Loaded
            | FieldRole::LoadedOne
            | FieldRole::LoadedMany => {
                return Err(syn::Error::new_spanned(
                    &field.ident,
                    "NxUpdate only supports #[nx(permissions)] and persisted #[nx(field = ...)] fields",
                ));
            }
        }
    }

    let permissions_update = permissions_field.map(|ident| {
        quote! {
            if let ::nx_db::Patch::Set(value) = self.#ident {
                ::nx_db::insert_value(&mut record, ::nx_db::FIELD_PERMISSIONS, value);
            }
        }
    });

    Ok(quote! {
        impl ::nx_db::UpdateRecord for #name {
            fn update_to_record(
                self,
                _context: &::nx_db::Context,
            ) -> Result<::nx_db::traits::storage::StorageRecord, ::nx_db::DatabaseError> {
                let mut record = ::nx_db::traits::storage::StorageRecord::new();
                #permissions_update
                #(#update_fields)*
                Ok(record)
            }
        }
    })
}

fn parse_struct_fields(data: &Data) -> syn::Result<Vec<syn::Field>> {
    let Data::Struct(data_struct) = data else {
        return Err(syn::Error::new(
            Span::call_site(),
            "Nx derives only support structs",
        ));
    };
    let Fields::Named(fields) = &data_struct.fields else {
        return Err(syn::Error::new(
            Span::call_site(),
            "Nx derives require named struct fields",
        ));
    };
    Ok(fields.named.iter().cloned().collect())
}

fn parse_fields(fields: &[syn::Field]) -> syn::Result<Vec<ParsedField>> {
    let mut parsed = Vec::new();
    for field in fields {
        let ident = field
            .ident
            .clone()
            .ok_or_else(|| syn::Error::new_spanned(field, "expected named field"))?;
        let role = parse_field_role(field)?;
        parsed.push(ParsedField {
            ident,
            ty: field.ty.clone(),
            role,
        });
    }
    Ok(parsed)
}

fn parse_field_role(field: &syn::Field) -> syn::Result<FieldRole> {
    let mut flag_id = false;
    let mut flag_metadata = false;
    let mut flag_permissions = false;
    let mut flag_required = false;
    let mut flag_virtual = false;
    let mut flag_loaded = false;
    let mut flag_loaded_one = false;
    let mut flag_loaded_many = false;
    let mut field_name: Option<String> = None;
    let mut encode: Option<Path> = None;
    let mut decode: Option<Path> = None;

    for attr in &field.attrs {
        if !attr.path().is_ident("nx") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("id") {
                flag_id = true;
                return Ok(());
            }
            if meta.path.is_ident("metadata") {
                flag_metadata = true;
                return Ok(());
            }
            if meta.path.is_ident("permissions") {
                flag_permissions = true;
                return Ok(());
            }
            if meta.path.is_ident("required") {
                flag_required = true;
                return Ok(());
            }
            if meta.path.is_ident("virtual") {
                flag_virtual = true;
                return Ok(());
            }
            if meta.path.is_ident("loaded") {
                flag_loaded = true;
                return Ok(());
            }
            if meta.path.is_ident("loaded_one") {
                flag_loaded_one = true;
                return Ok(());
            }
            if meta.path.is_ident("loaded_many") {
                flag_loaded_many = true;
                return Ok(());
            }
            if meta.path.is_ident("field") {
                let value = meta.value()?.parse::<LitStr>()?;
                field_name = Some(value.value());
                return Ok(());
            }
            if meta.path.is_ident("encode") {
                let value = meta.value()?.parse::<LitStr>()?;
                encode = Some(syn::parse_str(&value.value())?);
                return Ok(());
            }
            if meta.path.is_ident("decode") {
                let value = meta.value()?.parse::<LitStr>()?;
                decode = Some(syn::parse_str(&value.value())?);
                return Ok(());
            }

            Err(meta.error("unsupported #[nx(...)] option"))
        })?;
    }

    let special_flags = [
        flag_id,
        flag_metadata,
        flag_permissions,
        flag_virtual,
        flag_loaded,
        flag_loaded_one,
        flag_loaded_many,
    ]
    .into_iter()
    .filter(|flag| *flag)
    .count();

    if special_flags > 1 {
        return Err(syn::Error::new_spanned(
            field,
            "field cannot combine multiple special #[nx(...)] roles",
        ));
    }

    if flag_id {
        return Ok(FieldRole::Id);
    }
    if flag_metadata {
        return Ok(FieldRole::Metadata);
    }
    if flag_permissions {
        return Ok(FieldRole::Permissions);
    }
    if flag_virtual {
        return Ok(FieldRole::Virtual);
    }
    if flag_loaded {
        return Ok(FieldRole::Loaded);
    }
    if flag_loaded_one {
        return Ok(FieldRole::LoadedOne);
    }
    if flag_loaded_many {
        return Ok(FieldRole::LoadedMany);
    }

    let Some(field) = field_name else {
        return Err(syn::Error::new_spanned(
            field,
            "persisted fields must declare #[nx(field = \"...\")] or a special role",
        ));
    };

    Ok(FieldRole::Persisted {
        field,
        required: flag_required,
        encode,
        decode,
    })
}

fn option_inner_type(ty: &Type) -> Option<Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    let segment = type_path.path.segments.last()?;
    if segment.ident != "Option" {
        return None;
    }
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    let GenericArgument::Type(inner) = arguments.args.first()? else {
        return None;
    };
    Some(inner.clone())
}
