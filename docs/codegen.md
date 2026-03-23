# Codegen Workflow

The source-of-truth for application models should stay in developer-owned schema files, and the
generated Rust code should be what applications import.

## CLI

```bash
cargo run -p database-cli -- check --input schema.json
cargo run -p database-cli -- generate --input schema.json --output src/generated/models.rs
```

The repo includes working examples:

- schema: `examples/codegen/schema.json`
- generated output: `examples/codegen/models.rs`
- filtered schema/output: `examples/codegen/filtered_schema.json` → `examples/codegen/filtered_models.rs`
- virtual schema/output: `examples/codegen/virtual_schema.json` → `examples/codegen/virtual_models.rs`

The checked-in generated files are intentional fixtures, not throwaway output. `database-cli` tests
regenerate them and fail on drift.

## Schema Shape

Current supported input is JSON:

```json
{
  "module": "app_models",
  "filters": [
    {
      "name": "displayName",
      "decodedType": "crate::DisplayName",
      "encodedType": "String",
      "encode": "crate::codecs::encode_display_name",
      "decode": "crate::codecs::decode_display_name"
    }
  ],
  "resolvers": [
    {
      "name": "profileLabel",
      "outputType": "String",
      "resolve": "crate::resolvers::resolve_profile_label"
    }
  ],
  "collections": [
    {
      "id": "users",
      "name": "Users",
      "documentSecurity": true,
      "permissions": [
        "read(\"any\")",
        "create(\"any\")",
        "update(\"any\")",
        "delete(\"any\")"
      ],
      "idMaxLength": 48,
      "attributes": [
        {
          "id": "name",
          "kind": "string",
          "required": true,
          "filters": ["displayName"]
        },
        {
          "id": "profileLabel",
          "kind": "virtual",
          "resolver": "profileLabel"
        },
        {
          "id": "email",
          "kind": "string"
        },
        {
          "id": "active",
          "kind": "boolean",
          "required": true
        }
      ],
      "indexes": [
        {
          "id": "users_email_unique",
          "kind": "unique",
          "attributes": ["email"]
        },
        {
          "id": "users_name_active_idx",
          "kind": "key",
          "attributes": ["name", "active"],
          "orders": ["asc", "desc"]
        }
      ]
    }
  ]
}
```

## Generated API

The generated module currently produces:

- a model marker for `db.repo::<User>()`
- a marker constant for `db.get_repo(USER)`
- typed ids using `Key<N>`
- entity/create/update structs
- typed query fields
- static `CollectionSchema`
- static `IndexSchema` metadata
- a `registry()` function for startup wiring

Generated `Create*` payloads now expose `CreateUser::builder(required...)` and
`CreateUser::new(required...)`, default `permissions` to an empty list, auto-generate IDs when
`id` is omitted, and skip writing `None` option fields so database/schema defaults can apply on
insert.

`Update` structs use `Patch<T>` so nullable fields do not need awkward `Option<Option<T>>`.

## Filters

Project-level `filters` let codegen emit per-attribute encode/decode helpers.

- `decodedType` is the application-facing Rust type.
- `encodedType` is the next stored/intermediate Rust type.
- `encode` and `decode` are Rust function paths returning `Result<_, DatabaseError>`.
- attribute `filters` are applied in forward order on encode and reverse order on decode.

The current generator validates that the last filter in a chain stores the collection field's
underlying database type.

## Resolvers

Project-level `resolvers` let codegen emit virtual fields and a `Model::resolve_entity` hook.

- virtual attributes stay in the entity type only
- create/update payloads do not include virtual fields
- generated entities currently expose virtual fields as `Option<T>`
- direct query/sort usage of virtual fields is rejected at runtime before adapter SQL generation

## Indexes

Collection `indexes` are now generated into `CollectionSchema` and applied by the Postgres adapter
when `create_collection()` runs.

- `key` and `unique` indexes are emitted as normal PostgreSQL indexes
- `fulltext` indexes are emitted as `GIN (to_tsvector(...))`
- `spatial` indexes are not implemented by the Postgres adapter yet

## Current Limits

- `json` attributes are not generated yet
- relationship metadata is still not generated
- spatial indexes are rejected by the Postgres adapter
