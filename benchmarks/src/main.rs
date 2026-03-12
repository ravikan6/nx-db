use database::{Context, Database, PostgresAdapter, Key, Model};
use database::traits::storage::StorageAdapter;
use std::time::{Instant};
use rand::Rng;
use futures::future::join_all;

// Include the generated models
mod models {
    include!(concat!(env!("CARGO_MANIFEST_DIR"), "/../examples/codegen/production_models.rs"));
}

use models::prod_models::{User, CreateUser, Post, CreatePost, registry};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    
    println!("Connecting to database...");
    let pool = sqlx::PgPool::connect(&url).await?;
    
    sqlx::query("CREATE SCHEMA IF NOT EXISTS public")
        .execute(&pool)
        .await?;
    // Clear tables
    sqlx::query("TRUNCATE TABLE public.users, public.posts, public.users_perms, public.posts_perms CASCADE")
        .execute(&pool)
        .await?;
    
    let db = Database::builder()
        .with_adapter(PostgresAdapter::new(&pool))
        .with_registry(registry()?)
        .with_cache(database::cache::MemoryCacheBackend::default())
        .build()?;

    // Use the dedicated benchmark schema
    let ctx = Context::default().with_schema("public").with_role(database::Role::any());
    let db_scoped = db.scope(ctx.clone());

    println!("\n--- Benchmarking Production Scenarios ---\n");

    // 1. Batch Insertion Performance
    let user_count = 500;
    let posts_per_user = 10;
    
    println!("Inserting {} users and {} posts...", user_count, user_count * posts_per_user);
    let start = Instant::now();
    
    let user_repo = db_scoped.repo::<User>();
    let post_repo = db_scoped.repo::<Post>();
    
    let mut create_users = Vec::with_capacity(user_count);
    for i in 0..user_count {
        create_users.push(CreateUser {
            id: Key::new(format!("user_{}", i)).unwrap(),
            name: format!("User {}", i),
            email: format!("user{}@example.com", i),
            metadata: Some(format!("{{\"index\": {}, \"type\": \"benchmark\"}}", i)),
            permissions: vec!["read(\"any\")".to_string()],
        });
    }
    
    let users = user_repo.insert_many(create_users).await?;
    
    let mut posts = Vec::with_capacity(posts_per_user * user_count);
    for user in users {
        let u_id_str = user.id.to_string();
        for j in 0..posts_per_user {
            posts.push(CreatePost {
                id: Key::new(format!("post_{}_{}", u_id_str, j)).unwrap(),
                title: format!("Post {} by {}", j, u_id_str),
                content: Some("Benchmarking our production grade text search capabilities with some dummy content.".to_string()),
                author: u_id_str.clone(),
                permissions: vec!["read(\"any\")".to_string()],
            });
        }
    }
    post_repo.insert_many(posts).await?;
    
    let duration = start.elapsed();
    println!("Batch Insert: {:?} ({:.2} ops/sec)", duration, (user_count + user_count * posts_per_user) as f64 / duration.as_secs_f64());

    // Warm cache
    println!("Warming cache for all posts...");
    let all_posts = post_repo.find(database::QuerySpec::new().limit(user_count * posts_per_user)).await?;
    println!("Fetched {} posts for warming.", all_posts.len());
    
    // Explicitly re-get them to ensure they are in the individual document cache if find didn't do it
    for post in all_posts.iter().take(500) {
        let _ = post_repo.get(&post.id).await?;
    }

    // 2. Point Lookup Performance (with Cache)
    println!("\nBenchmarking Point Lookups (Random Cached)...");
    let lookups = 1000;
    let mut rng = rand::thread_rng();
    let start = Instant::now();
    
    for _ in 0..lookups {
        let p_idx = rng.gen_range(0..500);
        let id = all_posts[p_idx].id.clone();
        let _ = post_repo.get(&id).await?;
    }
    
    let duration = start.elapsed();
    println!("Point Lookups ({} ops): {:?} ({:.2} ops/sec)", lookups, duration, lookups as f64 / duration.as_secs_f64());

    // 2.5 Raw Adapter Lookup Performance (Baseline)
    println!("\nBenchmarking Raw Adapter Point Lookups (Random)...");
    let start = Instant::now();
    let adapter = PostgresAdapter::new(&pool);
    let collection = User::schema();
    
    for _ in 0..lookups {
        let p_idx = rng.gen_range(0..100);
        let id = format!("post_user_0_{}", p_idx);
        let _ = adapter.get(&ctx, collection, &id).await?;
    }
    
    let duration = start.elapsed();
    println!("Raw Adapter Lookups ({} ops): {:?} ({:.2} ops/sec)", lookups, duration, lookups as f64 / duration.as_secs_f64());

    // 3. Full-Text Search Performance
    println!("\nBenchmarking Full-Text Search...");
    let searches = 100;
    let start = Instant::now();
    
    for _ in 0..searches {
        // Search for "production grade" in content
        let query = Post::CONTENT.text_search("production grade");
        let results: Vec<_> = post_repo.find(query.into()).await?;
        assert!(!results.is_empty());
    }
    
    let duration = start.elapsed();
    println!("Full-Text Search ({} ops): {:?} ({:.2} ops/sec)", searches, duration, searches as f64 / duration.as_secs_f64());

    // 4. Optimized Relationship Loading (The N+1 Fix)
    println!("\nBenchmarking Relationship Loading (load_many_to_one)...");
    let start = Instant::now();
    
    let posts = post_repo.find(database::QuerySpec::new().limit(100)).await?;
    let authors: std::collections::HashMap<String, _> = post_repo.load_many_to_one::<User>(&posts, |p| Some(p.author.clone())).await?;
    
    let duration = start.elapsed();
    println!("Batch Load Authors for 100 posts: {:?}", duration);
    println!("Authors loaded: {}", authors.len());

    println!("\n--- Benchmarks Completed ---");

    Ok(())
}
