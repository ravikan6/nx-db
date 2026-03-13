use nx_db::prelude::*;
use nx_db::{db_context, db_query, db_registry};
use rand::Rng;
use std::time::{Duration, Instant};

mod models {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../examples/codegen/production_models.rs"
    ));
}

use crate::models::prod_models::UserEntity;
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

fn make_posts(round: usize, users: &[UserEntity], posts_per_user: usize) -> Vec<CreatePost> {
    users.iter().flat_map(|user| {
        let uid = user.id.to_string();
        (0..posts_per_user).map(move |j| CreatePost {
            id: Key::new(format!("r{}_post_{}_{}", round, uid, j)).unwrap(),
            title: format!("Post {} by {}", j, uid),
            // Lowercase, consistent content so FTS works regardless of stemmer config
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
    separator("1. Batch Insert  (users + posts, 3 queries/record)");

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

    // Load all post IDs — note: this find() may or may not warm the get() cache.
    // Section 2a measures whichever state this leaves the cache in.
    let all_posts = post_repo.find(db_query!(limit: total_posts)).await?;
    assert_eq!(all_posts.len(), total_posts, "Seed data mismatch");

    // ── 2a. POINT LOOKUPS — AFTER find() (cache state unknown) ───────────────
    separator("2a. Point Lookups — after find()  (cache state depends on adapter)");
    println!("  NOTE: If p50 here ≈ section 2b, your find() pre-warms the get() cache.\n");

    let mut rng = rand::thread_rng();
    let mut cold_stats = Stats::new("repo.get() — post find(), pre explicit warming");

    for _ in 0..200 {
        let idx = rng.gen_range(0..total_posts);
        let id = all_posts[idx].id.clone();
        let start = Instant::now();
        let _ = post_repo.get(&id).await?;
        cold_stats.push(start.elapsed());
    }
    cold_stats.print();

    // ── 2b. POINT LOOKUPS — WARM CACHE ───────────────────────────────────────
    separator("2b. Point Lookups — Warm Cache  (explicit 100% warming)");

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
    separator("3. find() — paginated  (DB + perm filter, bypasses get() cache)");

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

    // ── 4. FULL-TEXT SEARCH ───────────────────────────────────────────────────
    separator("4. Full-Text Search — text_search()");

    // (term, expect_results)
    // All content is: "production grade text search capabilities with dummy content for benchmarking purposes"
    let fts_cases: &[(&str, bool)] = &[
        ("production grade", true),
        ("dummy content", true),
        ("text search", true),
        ("benchmarking", true),
        ("xqznotfound", false), // intentional miss — tests empty-result path
    ];

    for &(term, expect_results) in fts_cases {
        let mut stats = Stats::new(&format!("text_search(\"{}\")  limit:50", term));
        let mut empty_count = 0usize;

        for _ in 0..50 {
            let q = db_query!(
                filter: Post::CONTENT.text_search(term),
                limit: 50
            );
            let start = Instant::now();
            let results = post_repo.find(q).await?;
            stats.push(start.elapsed());
            if results.is_empty() {
                empty_count += 1;
            }
        }

        if expect_results && empty_count > 0 {
            println!("  ⚠  '{}' returned empty {} / 50 times", term, empty_count);
            println!("     → Check tsvector column config, GIN index, and text search language.\n");
        }

        stats.print();
    }

    // ── 4.5 COMPLEX LOGIC — and/or/not ───────────────────────────────────────
    separator("4.5 Complex Logic — and(), or(), not()");

    let mut logic_stats = Stats::new("Complex Query: content ~ 'production' AND (author ~ 'user_0' OR author ~ 'user_1')");
    for _ in 0..50 {
        let q = db_query!(
            filter: nx_db::and!(
                Post::CONTENT.contains("production"),
                nx_db::or!(
                    Post::AUTHOR.contains("user_0"),
                    Post::AUTHOR.contains("user_1")
                )
            ),
            limit: 50
        );
        let start = Instant::now();
        let results = post_repo.find(q).await?;
        logic_stats.push(start.elapsed());
        // In this benchmark setup, authors are like 'user_0', 'user_1', etc. 
        // Wait, the make_posts function uses 'r{round}_post_{uid}_{j}' but author is just uid.
        // Let's check how authors are named in make_users.
    }
    logic_stats.print();

    // ── 5. RELATIONSHIP LOADING ───────────────────────────────────────────────
    separator("5. load_many_to_one::<User>  (N+1 prevention)");

    for &batch in &[10usize, 50, 100] {
        let mut stats = Stats::new(&format!(
            "load_many_to_one — {} posts  (expect ≤{} unique authors)",
            batch, user_count
        ));
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

    // ── 6. SINGLE insert() vs insert_many() ──────────────────────────────────
    separator("6. insert() ×10 sequential  vs  insert_many() ×10 batch");

    const SINGLE_BATCH: usize = 10;

    let mut single_stats = Stats::with_ops("insert() ×10 sequential", SINGLE_BATCH);
    for round in 0..20usize {
        sqlx::query("DELETE FROM nuvix_bench.users WHERE _uid LIKE 'si_%'")
            .execute(&pool)
            .await?;
        // sqlx::query("DELETE FROM nuvix_bench.users_perms WHERE document_id LIKE 'si_%'").execute(&pool).await?;

        let start = Instant::now();
        for i in 0..SINGLE_BATCH {
            let _ = user_repo
                .insert(CreateUser {
                    id: Key::new(format!("si_r{}_u{}", round, i)).unwrap(),
                    name: format!("SI {}", i),
                    email: format!("si_r{}_u{}@ex.com", round, i),
                    metadata: None,
                    permissions: vec!["read(\"any\")".to_string()],
                })
                .await?;
        }
        single_stats.push(start.elapsed());
    }
    single_stats.print();

    let mut batch_stats = Stats::with_ops("insert_many() ×10 batch", SINGLE_BATCH);
    for round in 0..20usize {
        sqlx::query("DELETE FROM nuvix_bench.users WHERE _uid LIKE 'bi_%'")
            .execute(&pool)
            .await?;
        // sqlx::query("DELETE FROM nuvix_bench.users_perms WHERE document_id LIKE 'bi_%'").execute(&pool).await?;

        let records: Vec<CreateUser> = (0..SINGLE_BATCH)
            .map(|i| CreateUser {
                id: Key::new(format!("bi_r{}_u{}", round, i)).unwrap(),
                name: format!("BI {}", i),
                email: format!("bi_r{}_u{}@ex.com", round, i),
                metadata: None,
                permissions: vec!["read(\"any\")".to_string()],
            })
            .collect();

        let start = Instant::now();
        let _ = user_repo.insert_many(records).await?;
        batch_stats.push(start.elapsed());
    }
    batch_stats.print();

    // ── SUMMARY ───────────────────────────────────────────────────────────────
    separator("Complete");
    println!(
        "  Dataset : {} users  {} posts  ({} total records)",
        user_count, total_posts, total_records
    );
    println!();
    println!("  What to look for:");
    println!("   2a p50 ≈ 2b p50?          → find() pre-warms cache (note this behaviour)");
    println!("   insert() mean / insert_many() mean  → actual batch speedup ratio");
    println!("   find(10) vs find(100) ops/sec        → per-record overhead vs fixed cost");
    println!("   FTS ⚠ warnings             → tsvector / GIN index config issue");
    println!("   p99 >> p95 anywhere        → occasional spike worth investigating");
    println!();

    Ok(())
}
