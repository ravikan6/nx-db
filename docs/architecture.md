# Rust Database Architecture

## Goals

- Keep collections static and defined in Rust code.
- Treat generated Rust code as the primary developer-facing API.
- Keep the `Database` type responsible for orchestration, validation, authorization, filters/codecs, caching, and events.
- Keep adapters thin and focused on storage/SQL concerns.
- Avoid making database metadata the source of truth for collection structure.

## Source Of Truth

Collection definitions live in Rust.

The intended workflow is:

1. Developers write or update schema definitions in Rust-friendly source files.
2. A generator CLI emits strongly typed Rust code for documents, create/update payloads, and registry wiring.
3. The generated code is compiled into the application and registered at startup.

Postgres may optionally store mirrored metadata for inspection or migration support, but it must not be the canonical schema source.

## Core Concepts

## Public API

The public API should be centered on repositories and scoped context.

Preferred usage:

```rust
let ctx = Context::default()
    .with_schema("public")
    .with_role(Role::user("user_123", None)?)
    .with_authorization(true);

let user = db
    .scope(ctx)
    .repo::<User>()
    .get(&user_id)
    .await?;
```

Supported ergonomic forms:

```rust
db.repo::<User>()
db.get_repo(USER)
db.scope(ctx).repo::<User>()
db.scope(ctx).get_repo(USER)
```

Where `User` / `USER` is generated code implementing the model contract.

### Model Contract

Generated model markers implement a trait that defines:

- collection schema
- typed id
- typed entity
- typed create payload
- typed update payload
- conversion between typed payloads and storage records

This keeps the public API strongly typed while allowing adapters to remain record-oriented internally.

### Typed IDs

IDs should be newtypes instead of raw strings.

The core library should support length-constrained key types so that:

- validation happens once at construction time
- all internal and repository operations work on validated ids
- repeated runtime validation is minimized

### Scoped Context

Context must be cheap to clone and explicit.

It carries request-specific state such as:

- schema override
- authorization enabled/disabled
- authorization roles
- tenant or namespace overrides

Context is passed into the adapter boundary, but the user-facing API should prefer `db.scope(context)` instead of repeating a context parameter on every method call.

### Collection Schema

A collection schema describes:

- collection id and display name
- whether document-level permissions are enabled
- static collection-level permissions
- persisted and virtual attributes
- attribute filters/codecs
- indexes

Each attribute is defined statically. Persisted attributes map to real Postgres columns. Virtual attributes are resolved by the runtime and are not stored directly.

### Registry

The registry owns all known static collections for a process.

Responsibilities:

- register collection schemas at startup
- resolve a collection by id
- validate collection definitions once
- expose the full set of registered collections for migration/bootstrap tooling

### Database

`Database` is the orchestration layer.

Responsibilities:

- load collection definitions from the registry
- validate input payloads against static collection schemas
- run authorization checks
- apply codecs on encode/decode
- run virtual resolvers after reads
- coordinate caching
- emit events
- delegate physical storage to the adapter

The `Database` type should not generate SQL directly.

### Storage Adapter

The storage adapter is intentionally thin.

Responsibilities:

- create/drop collection storage
- execute inserts/updates/deletes/selects
- translate logical records into SQL rows
- manage driver-specific concerns such as identifier quoting and transactions

The adapter receives already-shaped data from `Database`.

### Codecs

Codecs model value transformation.

They handle:

- application input type
- stored database type
- decoded application output type

Pipeline:

- `Input -> encode -> Stored`
- `Stored -> decode -> Output`

This is the Rust replacement for most TS filter behavior.

### Resolvers

Resolvers are separate from codecs.

They handle virtual fields or derived values that may require:

- cache reads
- additional database reads
- cross-collection lookups

This keeps "value transformation" separate from "fetch additional data".

### Events

Events are emitted by `Database` after successful mutations and reads.

Initial design constraints:

- listener registration is runtime-based
- handlers are best-effort
- the core contract is fire-and-forget oriented

The concrete execution strategy can be provided by the host application or a higher-level runtime.

## Postgres Layout

Each collection maps to two tables:

1. Main collection table
2. `<collection>_perms` table

The main table stores:

- system columns such as id and timestamps
- one column per persisted attribute

The permissions table stores document-level permissions keyed by document id and permission type.

Collection-level permissions remain in static Rust schema definitions.

## Authorization Rules

- Collection-level permissions are always defined statically.
- If `document_security` is disabled, collection-level permissions are sufficient.
- If `document_security` is enabled, document-level permissions are also considered.
- Query authorization should be pushed down into adapter query building where possible for scale, while `Database` remains the owner of the rule set.

## Non-Goals For The First Pass

- full code generation CLI
- full migration engine
- relationship runtime
- cache implementation
- complete typed query system

The first implementation pass should establish the contracts these systems plug into.
