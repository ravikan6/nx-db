# nx-db: AI-Ready Production Database Wrapper

`nx-db` is a schema-first, strongly-typed database orchestration layer for Rust. It is designed to be highly compatible with AI coding agents and automated toolchains by using a single JSON source of truth for both code generation and database migrations.

## 1. Core Philosophy: The Handshake
The "handshake" between an AI and `nx-db` is the **Schema JSON**. 
- **AI Task:** Generate or modify the `schema.json`.
- **Toolchain Task:** Run `database-cli` to sync the database and generate Rust types.

This eliminates the need for AI to write complex SQL, preventing syntax errors and ensuring production-grade indices and security are always applied.

## 2. Toolchain Commands (For AI/CI)

```bash
# Validate the schema
cargo run -p database-cli -- check --input schema.json

# Sync database schema (Safe ALTER TABLE/CREATE INDEX)
cargo run -p database-cli -- migrate --input schema.json --schema my_app --database-url $URL

# Generate strongly-typed Rust models
cargo run -p database-cli -- generate --input schema.json --output src/models.rs
```

## 3. Developer Experience (DX) Macros
We provide macros to minimize boilerplate and make the code readable for both humans and AI.

### Context & Security
```rust
use nx_db::{db_context, Role};

// Create a security context with a specific schema and role
let ctx = db_context!(schema: "prod", role: Role::any());
```

### Declarative Queries
```rust
use nx_db::db_query;

// AI can easily generate this without knowing SQL
let query = db_query!(
    filter: User::NAME.contains("Ravi"),
    sort: User::CREATED_AT.desc(),
    limit: 10
);
```

### Easy Mutations
```rust
use nx_db::{db_insert, db_update};

// Create a new model instance
let new_user = db_insert!(User {
    id: Key::new("usr_123").unwrap(),
    name: "John Doe".to_string(),
    email: "john@example.com".to_string(),
    permissions: vec!["read".into()],
});

// Partial update
let patch = db_update!(User {
    name: "John Updated".to_string(),
});
```

## 4. High-Performance Features

### Relationship Batch Loading (O(1))
To avoid N+1 query problems, use the batch loader. This is critical for AI tools generating dashboard or list views.
```rust
let posts = post_repo.find(db_query!(limit: 100)).await?;

// Efficiently fetch all authors in ONE extra query
let authors = post_repo.load_many_to_one::<User>(
    &posts, 
    |p| Some(p.author_id.clone())
).await?;
```

### Full-Text Search
```rust
let q = db_query!(filter: Post::CONTENT.text_search("rust performance"));
let results = post_repo.find(q).await?;
```

## 5. Integration Workflow for AI Agents

When an AI agent is asked to "Add a 'Category' field to posts":
1.  **Modify `schema.json`**: Add the attribute `{ "id": "category", "kind": "string" }`.
2.  **Run Migrate**: Execute the CLI to add the column to Postgres.
3.  **Run Generate**: Execute the CLI to update `models.rs`.
4.  **Use in Code**: Use `Post::CATEGORY` in queries immediately.

This workflow is 100% type-safe and prevents the AI from making structural mistakes in the database.
