use nx_db::prelude::*;
use nx_db::{db_context, db_query, db_registry, and, or};
use rand::Rng;
use std::time::{Duration, Instant};

mod models {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../examples/codegen/production_models.rs"
    ));
}

use models::prod_models::{CreatePost, CreateUser, Post, User, registry};
// ─── Stats ────────────────────────────────────────────────────────────────────

struct Stats {
    label: String,
    samples: Vec<Duration>,
    /// If set, ops/sec uses (samples * ops_per_sample) instead of sample count
    ops_per_sample: usize,
}

impl Stats {
    fn new(label: &str) -> Self {
        Self {
            label: label.to_string(),
            samples: Vec::new(),
            ops_per_sample: 1,
        }
    }

    fn with_ops(label: &str, ops_per_sample: usize) -> Self {
        Self {
            label: label.to_string(),
            samples: Vec::new(),
            ops_per_sample,
        }
    }

    fn push(&mut self, d: Duration) {
        self.samples.push(d);
    }

    fn print(&mut self) {
        if self.samples.is_empty() {
            return;
        }
        self.samples.sort_unstable();

        let n = self.samples.len();
        let total: Duration = self.samples.iter().sum();
        let mean = total / n as u32;
        let min = self.samples[0];
        let max = self.samples[n - 1];
        let p50 = self.samples[n * 50 / 100];
        let p95 = self.samples[n * 95 / 100];
        let p99 = self.samples[(n * 99 / 100).min(n - 1)];

        let mean_us = mean.as_secs_f64() * 1_000_000.0;
        let variance = self
            .samples
            .iter()
            .map(|d| {
                let diff = d.as_secs_f64() * 1_000_000.0 - mean_us;
                diff * diff
            })
            .sum::<f64>()
            / n as f64;
        let stddev_us = variance.sqrt();

        let total_ops = n * self.ops_per_sample;
        let ops_per_sec = total_ops as f64 / total.as_secs_f64();

        println!("  ┌─ {}", self.label);
        println!("  │  samples    : {}  ({} total ops)", n, total_ops);
        println!("  │  ops/sec    : {:.0}", ops_per_sec);
        println!("  │  mean/op    : {:>10.3}ms", mean.as_secs_f64() * 1000.0);
        println!("  │  stddev     : {:>10.3}µs", stddev_us);
        println!("  │  min        : {:>10.3}ms", min.as_secs_f64() * 1000.0);
        println!("  │  p50        : {:>10.3}ms", p50.as_secs_f64() * 1000.0);
        println!("  │  p95        : {:>10.3}ms", p95.as_secs_f64() * 1000.0);
        println!("  │  p99        : {:>10.3}ms", p99.as_secs_f64() * 1000.0);
        println!("  └─ max        : {:>10.3}ms", max.as_secs_f64() * 1000.0);
        println!();
    }
}

fn separator(title: &str) {
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  {}", title);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
}

fn make_users(round: usize, count: usize) -> Vec<CreateUser> {
    (0..count)
        .map(|i| CreateUser {
            id: Key::new(format!("r{}_user_{}", round, i)).unwrap(),
            name: format!("User {}", i),
            email: format!("r{}_user{}@example.com", round, i),
            metadata: Some(format!("{{\"index\": {}}}", i)),
            permissions: vec!["read(\"any\")".to_string()],
        })
        .collect()
}

fn make_posts(round: usize, users: &[models::prod_models::UserEntity], posts_per_user: usize) -> Vec<CreatePost> {
    users.iter().flat_map(|user| {
        let uid = user.id.to_string();
        (0..posts_per_user).map(move |j| CreatePost {
            id: Key::new(format!("r{}_post_{}_{}", round, uid, j)).unwrap(),
            title: format!("Post {} by {}", j, uid),
            content: Some("production grade text search capabilities with dummy content for benchmarking purposes".to_string()),
            author: uid.clone(),
            permissions: vec!["read(\"any\")".to_string()],
        })
    }).collect()
}

// ─── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    println!("Connecting to database...");
    let pool = nx_db::db_connect!(&url).await?;

    let db = Database::builder()
        .with_adapter(nx_db::postgres::PostgresAdapter::new(&pool))
        .with_registry(registry()?)
        .with_cache(nx_db::cache::MemoryCacheBackend::default())
        .build()?;

    let ctx = db_context!(schema: "nuvix_bench", role: Role::any());
    let db_scoped = db.scope(ctx.clone());

    let user_repo = db_scoped.repo::<User>();
    let post_repo = db_scoped.repo::<Post>();

    let user_count = 100usize;
    let posts_per_user = 10usize;
    let total_posts = user_count * posts_per_user;
    let total_records = user_count + total_posts; // 1100

    // ── 1. BATCH INSERT ───────────────────────────────────────────────────────
    separator("1. Batch Insert  (users + posts)");

    let insert_rounds = 5usize;
    let mut insert_stats = Stats::with_ops("insert_many — full round", total_records);

    for round in 0..insert_rounds {
        sqlx::query("TRUNCATE TABLE nuvix_bench.users, nuvix_bench.posts, nuvix_bench.users_perms, nuvix_bench.posts_perms CASCADE")
            .execute(&pool).await?;

        let start = Instant::now();
        let users = user_repo.insert_many(make_users(round, user_count)).await?;
        post_repo
            .insert_many(make_posts(round, &users, posts_per_user))
            .await?;
        insert_stats.push(start.elapsed());

        print!("  round {}/{} complete\r", round + 1, insert_rounds);
    }
    println!();
    insert_stats.print();

    // ── Seed clean dataset for remaining benchmarks ───────────────────────────
    sqlx::query("TRUNCATE TABLE nuvix_bench.users, nuvix_bench.posts, nuvix_bench.users_perms, nuvix_bench.posts_perms CASCADE")
        .execute(&pool).await?;

    let seed_users = user_repo.insert_many(make_users(99, user_count)).await?;
    post_repo
        .insert_many(make_posts(99, &seed_users, posts_per_user))
        .await?;

    let all_posts = post_repo.find(db_query!(limit: total_posts)).await?;
    assert_eq!(all_posts.len(), total_posts, "Seed data mismatch");

    // ── 2a. POINT LOOKUPS — AFTER find() (cache state unknown) ───────────────
    separator("2a. Point Lookups — after find()");

    let mut rng = rand::thread_rng();
    let mut cold_stats = Stats::new("repo.get() — post find()");

    for _ in 0..200 {
        let idx = rng.gen_range(0..total_posts);
        let id = all_posts[idx].id.clone();
        let start = Instant::now();
        let _ = post_repo.get(&id).await?;
        cold_stats.push(start.elapsed());
    }
    cold_stats.print();

    // ── 2b. POINT LOOKUPS — WARM CACHE ───────────────────────────────────────
    separator("2b. Point Lookups — Warm Cache");

    for post in all_posts.iter() {
        let _ = post_repo.get(&post.id).await?;
    }

    let mut warm_stats = Stats::new("repo.get() — warm cache hit");
    for _ in 0..1000 {
        let idx = rng.gen_range(0..total_posts);
        let id = all_posts[idx].id.clone();
        let start = Instant::now();
        let _ = post_repo.get(&id).await?;
        warm_stats.push(start.elapsed());
    }
    warm_stats.print();

    // ── 3. FIND — PAGINATED ───────────────────────────────────────────────────
    separator("3. find() — paginated");

    for &limit in &[10usize, 50, 100] {
        let mut stats = Stats::with_ops(&format!("find(limit: {})", limit), limit);
        for _ in 0..50 {
            let start = Instant::now();
            let results = post_repo.find(db_query!(limit: limit)).await?;
            stats.push(start.elapsed());
            assert_eq!(results.len(), limit, "find() returned wrong count");
        }
        stats.print();
    }

    // ── 3.5 FIND — SELECTIVE ──────────────────────────────────────────────────
    separator("3.5 find() — selective fields");
    
    let mut select_stats = Stats::new("find(limit: 50, select: [title])");
    for _ in 0..50 {
        let start = Instant::now();
        let q = db_query!(limit: 50).select(vec!["title"]);
        let _ = post_repo.find(q).await?;
        select_stats.push(start.elapsed());
    }
    select_stats.print();

    // ── 4. FULL-TEXT SEARCH ───────────────────────────────────────────────────
    separator("4. Full-Text Search");

    let fts_cases: &[(&str, bool)] = &[
        ("production grade", true),
        ("dummy content", true),
    ];

    for &(term, _expect_results) in fts_cases {
        let mut stats = Stats::new(&format!("text_search(\"{}\")  limit:50", term));
        for _ in 0..50 {
            let q = db_query!(
                filter: Post::CONTENT.text_search(term),
                limit: 50
            );
            let start = Instant::now();
            let _ = post_repo.find(q).await?;
            stats.push(start.elapsed());
        }
        stats.print();
    }

    // ── 4.5 COMPLEX LOGIC — and/or/not ───────────────────────────────────────
    separator("4.5 Complex Logic — and!(), or!()");

    let mut logic_stats = Stats::new("Complex Query: content ~ 'production' AND (author ~ 'user_0' OR author ~ 'user_1')");
    for _ in 0..50 {
        let q = db_query!(
            filter: and!(
                Post::CONTENT.contains("production"),
                or!(
                    Post::AUTHOR.contains("user_0"),
                    Post::AUTHOR.contains("user_1")
                )
            ),
            limit: 50
        );
        let start = Instant::now();
        let _ = post_repo.find(q).await?;
        logic_stats.push(start.elapsed());
    }
    logic_stats.print();

    // ── 5. RELATIONSHIP LOADING ───────────────────────────────────────────────
    separator("5. load_many_to_one::<User>");

    for &batch in &[50usize, 100] {
        let mut stats = Stats::new(&format!("load_many_to_one — {} posts", batch));
        for _ in 0..20 {
            let posts = post_repo.find(db_query!(limit: batch)).await?;
            let start = Instant::now();
            let authors: std::collections::HashMap<String, _> = post_repo
                .load_many_to_one::<User>(&posts, |p| Some(p.author.clone()))
                .await?;
            stats.push(start.elapsed());
            assert!(authors.len() <= user_count);
        }
        stats.print();
    }

    // ── SUMMARY ───────────────────────────────────────────────────────────────
    separator("Complete");
    println!("Metadata test (Post 0): sequence={}, uid={}, created_at={}", 
        all_posts[0]._metadata.sequence, 
        all_posts[0]._metadata.uid,
        all_posts[0]._metadata.created_at
    );

    Ok(())
}
