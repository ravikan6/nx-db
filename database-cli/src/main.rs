use clap::{Parser, Subcommand};
use database_codegen::{generate_from_json, parse_project_spec, validate_project_spec};
use std::fs;
use std::path::PathBuf;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate Rust models from a schema file
    Generate {
        /// Input schema.json file
        #[arg(short, long)]
        input: PathBuf,
        /// Output .rs file
        #[arg(short, long)]
        output: PathBuf,
    },
    /// Validate a schema file
    Check {
        /// Input schema.json file
        #[arg(short, long)]
        input: PathBuf,
    },
    /// Apply schema changes to the database
    Migrate {
        /// Input schema.json file
        #[arg(short, long)]
        input: PathBuf,
        /// Database URL (defaults to DATABASE_URL env var)
        #[arg(short, long)]
        database_url: Option<String>,
        /// Dry run: show changes without applying them
        #[arg(long)]
        dry_run: bool,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();

    match cli.command {
        Commands::Generate { input, output } => {
            let contents = fs::read_to_string(&input)?;
            let generated = generate_from_json(&contents)?;

            if let Some(parent) = output.parent() {
                if !parent.as_os_str().is_empty() {
                    fs::create_dir_all(parent)?;
                }
            }

            fs::write(&output, generated)?;
            println!("generated {}", output.display());
        }
        Commands::Check { input } => {
            let contents = fs::read_to_string(&input)?;
            let spec = parse_project_spec(&contents)?;
            validate_project_spec(&spec)?;
            println!("schema ok: {} collection(s)", spec.collections.len());
        }
        Commands::Migrate { input, database_url, dry_run } => {
            let url = database_url
                .or_else(|| std::env::var("DATABASE_URL").ok())
                .ok_or("Database URL not provided. Set DATABASE_URL env var or use --database-url")?;

            let contents = fs::read_to_string(&input)?;
            let spec = parse_project_spec(&contents)?;
            validate_project_spec(&spec)?;

            let pool = sqlx::PgPool::connect(&url).await?;
            let engine = database::migration::MigrationEngine::new(&pool);
            let context = database::Context::default();

            let collections: Vec<&dyn database::traits::migration::MigrationCollection> = spec.collections
                .iter()
                .map(|c| c as &dyn database::traits::migration::MigrationCollection)
                .collect();

            let changes: Vec<database::migration::MigrationChange> = engine.diff(&context, &collections).await?;

            if changes.is_empty() {
                println!("Database is up to date.");
                return Ok(());
            }

            println!("Pending changes:");
            for change in &changes {
                match change {
                    database::migration::MigrationChange::CreateTable(id) => {
                        println!("  - Create table {}", id);
                    }
                    database::migration::MigrationChange::AddColumn { table, column, sql_type, .. } => {
                        println!("  - Add column {}.{} ({})", table, column, sql_type);
                    }
                    database::migration::MigrationChange::CreateIndex { index_id, .. } => {
                        println!("  - Create index {}", index_id);
                    }
                    database::migration::MigrationChange::DropIndex { index_id, .. } => {
                        println!("  - Drop index {}", index_id);
                    }
                }
            }

            if dry_run {
                println!("Dry run: skipping application of changes.");
            } else {
                println!("Applying changes...");
                engine.migrate(&context, &collections).await?;
                println!("Migration successful.");
            }
        }
    }

    Ok(())
}
