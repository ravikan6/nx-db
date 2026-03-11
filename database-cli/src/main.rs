use database_codegen::{
    CodegenError, generate_from_json, parse_project_spec, validate_project_spec,
};
use std::env;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::PathBuf;

#[derive(Debug)]
enum CliError {
    Usage(String),
    Io(std::io::Error),
    Codegen(CodegenError),
}

impl Display for CliError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Usage(message) => f.write_str(message),
            Self::Io(error) => write!(f, "io error: {error}"),
            Self::Codegen(error) => write!(f, "{error}"),
        }
    }
}

impl Error for CliError {}

impl From<std::io::Error> for CliError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<CodegenError> for CliError {
    fn from(value: CodegenError) -> Self {
        Self::Codegen(value)
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), CliError> {
    let mut args = env::args().skip(1);
    let Some(command) = args.next() else {
        return Err(CliError::Usage(usage()));
    };

    match command.as_str() {
        "generate" => {
            let input = parse_flag_path(&mut args, "--input")?;
            let output = parse_flag_path(&mut args, "--output")?;
            let contents = fs::read_to_string(&input)?;
            let generated = generate_from_json(&contents)?;

            if let Some(parent) = output.parent() {
                if !parent.as_os_str().is_empty() {
                    fs::create_dir_all(parent)?;
                }
            }

            fs::write(&output, generated)?;
            println!("generated {}", output.display());
            Ok(())
        }
        "check" => {
            let input = parse_flag_path(&mut args, "--input")?;
            let contents = fs::read_to_string(&input)?;
            let spec = parse_project_spec(&contents)?;
            validate_project_spec(&spec)?;
            println!("schema ok: {} collection(s)", spec.collections.len());
            Ok(())
        }
        "--help" | "-h" | "help" => {
            println!("{}", usage());
            Ok(())
        }
        _ => Err(CliError::Usage(usage())),
    }
}

fn parse_flag_path(
    args: &mut impl Iterator<Item = String>,
    expected_flag: &str,
) -> Result<PathBuf, CliError> {
    let flag = args.next().ok_or_else(|| CliError::Usage(usage()))?;
    if flag != expected_flag {
        return Err(CliError::Usage(usage()));
    }

    let value = args.next().ok_or_else(|| CliError::Usage(usage()))?;
    Ok(PathBuf::from(value))
}

fn usage() -> String {
    [
        "Usage:",
        "  database-cli generate --input <schema.json> --output <generated.rs>",
        "  database-cli check --input <schema.json>",
    ]
    .join("\n")
}
