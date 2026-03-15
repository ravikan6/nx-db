use nx_db::prelude::*;
use nx_db::{and, db_context, db_query, or};
use rand::Rng;
use std::time::{Duration, Instant};

mod models {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../examples/codegen/production_models.rs"
    ));
}

use models::prod_models::{registry, CreatePost, CreateUser, Post, User};

struct Stats {
    label: String,
    samples: Vec<Duration>,
    ops_per_sample: usize,
}

impl Stats {
    fn new(label: &str) -> Self {
        Self { label: label.to_string(), samples: Vec::new(), ops_per_sample: 1 }
    }
    fn with_ops(label: &str, ops_per_sample: usize) -> Self {
        Self { label: label.to_string(), samples: Vec::new(), ops_per_sample }
    }
    fn push(&mut self, d: Duration) { self.samples.push(d); }
    fn print(&mut self) {
        if self.samples.is_empty() { return; }
        self.samples.sort_unstable();
        let n = self.samples.len();
        let total: Duration = self.samples.iter().sum();
        let mean = total / n as u32;
        let total_ops = n * self.ops_per_sample;
        let ops_per_sec = total_ops as f64 / total.as_secs_f64();
        println!("  ┌─ {}", self.label);
        println!("  │  samples    : {}  ({} total ops)", n, total_ops);
        println!("  │  ops/sec    : {:.0}", ops_per_sec);
        println!("  │  mean/op    : {:>10.3}ms", mean.as_secs_f64() * 1000.0);
        println!("  └─ p99        : {:>10.3}ms", self.samples[(n * 99 / 100).min(n - 1)].as_secs_f64() * 1000.0);
        println!();
    }
}

fn separator(title: &str) {
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  {}", title);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
}

fn make_users(round: usize, count: usize) -> Vec<CreateUser> {
    (0..count).map(|i| CreateUser {
        id: Key::new(format!("r{}_user_{}", round, i)).unwrap(),
        name: format!("User {}", i),
        email: format!("r{}_user{}@example.com", round, i),
        metadata: Some(format!("{{\"index\": {}}}", i)),
        permissions: vec!["read(\"any\")".to_string()],
    }).collect()
}

fn make_posts(round: usize, users: &[models::prod_models::UserEntity], posts_per_user: usize) -> Vec<CreatePost> {
    users.iter().flat_map(|user| {
        let uid = user.id.to_string();
        (0..posts_per_user).map(move |j| CreatePost {
            id: Key::new(format!("r{}_post_{}_{}", round, uid, j)).unwrap(),
            title: format!("Post {} by {}", j, uid),
            content: Some("production grade content for benchmarking text search capabilities".to_string()),
            author: uid.clone(),
            permissions: vec!["read(\"any\")".to_string()],
        })
    }).collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    println!("Connecting to database: {}...", url);

    if url.starts_with("postgres://") {
        let pool = sqlx::PgPool::connect(&url).await?;
        let db = Database::builder()
            .with_adapter(nx_db::postgres::PostgresAdapter::new(pool.clone()))
            .with_registry(registry()?)
            .with_cache(nx_db::cache::MemoryCacheBackend::default())
            .build()?;
        run_benchmarks_pg(db, pool).await?;
    } else if url.starts_with("sqlite://") {
        #[cfg(feature = "sqlite")]
        {
            let pool = sqlx::SqlitePool::connect(&url).await?;
            let db = Database::builder()
                .with_adapter(nx_db::sqlite::SqliteAdapter::new(pool.clone()))
                .with_registry(registry()?)
                .with_cache(nx_db::cache::MemoryCacheBackend::default())
                .build()?;
            run_benchmarks_sqlite(db, pool).await?;
        }
    }
    Ok(())
}

async fn run_benchmarks_pg(db: Database<nx_db::postgres::PostgresAdapter, nx_db::StaticRegistry>, pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    run_core_benchmarks(db, |p| Box::pin(async move {
        sqlx::query("TRUNCATE TABLE nuvix_bench.users, nuvix_bench.posts, nuvix_bench.users_perms, nuvix_bench.posts_perms CASCADE").execute(&p).await?;
        Ok(())
    }), pool).await
}

#[cfg(feature = "sqlite")]
async fn run_benchmarks_sqlite(db: Database<nx_db::sqlite::SqliteAdapter, nx_db::StaticRegistry>, pool: sqlx::SqlitePool) -> Result<(), Box<dyn std::error::Error>> {
    run_core_benchmarks(db, |p| Box::pin(async move {
        sqlx::query("DELETE FROM users").execute(&p).await?;
        sqlx::query("DELETE FROM posts").execute(&p).await?;
        sqlx::query("DELETE FROM users_perms").execute(&p).await?;
        sqlx::query("DELETE FROM posts_perms").execute(&p).await?;
        Ok(())
    }), pool).await
}

async fn run_core_benchmarks<A, P, F>(db: Database<A, nx_db::StaticRegistry>, cleanup: F, pool: P) -> Result<(), Box<dyn std::error::Error>> 
where 
    A: nx_db::traits::storage::StorageAdapter + 'static,
    P: Clone + Send + Sync + 'static,
    F: Fn(P) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Box<dyn std::error::Error>>> + Send>> + Copy
{
    let ctx = db_context!(schema: "nuvix_bench", role: Role::any());
    let db_scoped = db.scope(ctx.clone());
    let user_repo = db_scoped.repo::<User>();
    let post_repo = db_scoped.repo::<Post>();

    let user_count = 100usize;
    let posts_per_user = 10usize;
    let total_posts = user_count * posts_per_user;
    let total_records = user_count + total_posts;

    separator("1. Batch Insert");
    let mut insert_stats = Stats::with_ops("insert_many", total_records);
    for round in 0..5 {
        cleanup(pool.clone()).await?;
        let start = Instant::now();
        let users: Vec<_> = user_repo.insert_many(make_users(round, user_count)).await?;
        post_repo.insert_many(make_posts(round, &users, posts_per_user)).await?;
        insert_stats.push(start.elapsed());
    }
    insert_stats.print();

    cleanup(pool.clone()).await?;
    let seed_users: Vec<_> = user_repo.insert_many(make_users(99, user_count)).await?;
    post_repo.insert_many(make_posts(99, &seed_users, posts_per_user)).await?;
    let all_posts: Vec<_> = post_repo.find(db_query!(limit: total_posts)).await?;

    separator("2. Point Lookups");
    let mut warm_stats = Stats::new("warm cache hit");
    for post in &all_posts { let _ = post_repo.get(&post.id).await?; }
    let mut rng = rand::thread_rng();
    for _ in 0..1000 {
        let id = all_posts[rng.gen_range(0..total_posts)].id.clone();
        let start = Instant::now();
        let _ = post_repo.get(&id).await?;
        warm_stats.push(start.elapsed());
    }
    warm_stats.print();

    separator("3. Relationship Loading");
    let mut rel_stats = Stats::new("load_many_to_one (100 posts)");
    for _ in 0..50 {
        let posts = post_repo.find(db_query!(limit: 100)).await?;
        let start = Instant::now();
        let _: std::collections::HashMap<String, _> = post_repo.load_many_to_one::<User>(&posts, |p| Some(p.author.clone())).await?;
        rel_stats.push(start.elapsed());
    }
    rel_stats.print();

    separator("Complete");
    Ok(())
}
