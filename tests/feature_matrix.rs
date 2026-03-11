use std::path::PathBuf;
use std::process::Command;
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn target_dir(label: &str) -> PathBuf {
    workspace_root().join("target").join("feature-matrix").join(label)
}

fn temp_dir(label: &str) -> PathBuf {
    workspace_root().join("target").join("feature-matrix-tmp").join(label)
}

fn run_check(args: &[&str], label: &str) {
    let temp_dir = temp_dir(label);
    std::fs::create_dir_all(&temp_dir).expect("temp dir should be created");

    let output = Command::new("fish")
        .arg("-lc")
        .arg(format!("cargo check --quiet {}", args.join(" ")))
        .current_dir(workspace_root())
        .env("CARGO_TARGET_DIR", target_dir(label))
        .env("TMPDIR", &temp_dir)
        .output()
        .expect("cargo check should run through fish");

    assert!(
        output.status.success(),
        "cargo check {:?} failed\nstdout:\n{}\nstderr:\n{}",
        args,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn feature_matrix_compiles() {
    run_check(
        &["-p", "database", "--features", "postgres"],
        "root-postgres",
    );
    run_check(
        &["-p", "database", "--features", "cache-redis"],
        "root-cache-redis",
    );
    run_check(
        &["-p", "database-cache", "--features", "redis"],
        "cache-redis",
    );
}
