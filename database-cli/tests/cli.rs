use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("examples")
        .join("codegen")
        .join(name)
}

fn temp_output(name: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be valid")
        .as_nanos();
    std::env::temp_dir().join(format!("database-cli-{stamp}-{name}"))
}

#[test]
fn check_command_validates_schema() {
    let output = Command::new(env!("CARGO_BIN_EXE_database-cli"))
        .args(["check", "--input"])
        .arg(fixture("schema.json"))
        .output()
        .expect("cli should run");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("schema ok"));
}

#[test]
fn generate_command_writes_models_file() {
    let output_path = temp_output("models.rs");
    let output = Command::new(env!("CARGO_BIN_EXE_database-cli"))
        .args(["generate", "--input"])
        .arg(fixture("schema.json"))
        .args(["--output"])
        .arg(&output_path)
        .output()
        .expect("cli should run");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let contents = fs::read_to_string(&output_path).expect("generated file should exist");
    assert!(contents.contains("pub mod app_models"));
    assert!(contents.contains("pub struct User;"));
    assert!(contents.contains("pub fn registry() -> Result<StaticRegistry, DatabaseError>"));
    assert!(contents.contains("::serde::Serialize"));

    let _ = fs::remove_file(output_path);
}

#[test]
fn checked_in_codegen_examples_match_generator_output() {
    for (schema, generated) in [
        ("schema.json", "models.rs"),
        ("filtered_schema.json", "filtered_models.rs"),
        ("virtual_schema.json", "virtual_models.rs"),
    ] {
        let output_path = temp_output(generated);
        let output = Command::new(env!("CARGO_BIN_EXE_database-cli"))
            .args(["generate", "--input"])
            .arg(fixture(schema))
            .args(["--output"])
            .arg(&output_path)
            .output()
            .expect("cli should run");

        assert!(
            output.status.success(),
            "{} stderr: {}",
            schema,
            String::from_utf8_lossy(&output.stderr)
        );

        let expected =
            fs::read_to_string(fixture(generated)).expect("checked-in fixture should exist");
        let actual = fs::read_to_string(&output_path).expect("generated file should exist");
        assert_eq!(actual, expected, "{} drifted from {}", generated, schema);

        let _ = fs::remove_file(output_path);
    }
}
