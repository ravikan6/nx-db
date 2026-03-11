# database

Rust workspace for a typed database abstraction, code generator, cache layer, and optional Postgres adapter.

## Crates

- `database` — root crate; re-exports `nx_core`, cache helpers, and `PostgresAdapter` behind the `postgres` feature
- `database-cli` — schema validation and Rust code generation
- `database-codegen` — generator library used by the CLI
- `database-cache` — in-memory cache plus optional Redis backend via `redis`
- `driver-postgres` — SQLx-backed Postgres adapter

## Features

- `postgres` — enables `database::PostgresAdapter` and `database::postgres::*`
- `cache-redis` — forwards Redis support to `database-cache`

## Codegen workflow

```bash
cargo run -p database-cli -- check --input examples/codegen/schema.json
cargo run -p database-cli -- generate --input examples/codegen/schema.json --output examples/codegen/models.rs
```

Checked-in generated examples live in `examples/codegen/` and are verified by tests to prevent drift.

## Verification

Typical local sanity passes:

```bash
fish -lc 'cargo test'
fish -lc 'cargo check -p database --features postgres'
fish -lc 'cargo check -p database --features cache-redis'
```

## Docs

- `docs/architecture.md`
- `docs/codegen.md`
