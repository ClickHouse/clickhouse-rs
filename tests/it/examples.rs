#[test]
fn test_all_examples_exit_zero() {
    let entries = std::fs::read_dir("./examples").unwrap();
    for entry in entries {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_file() && path.extension().map_or(false, |ext| ext == "rs") {
            let file_name = path.file_stem().unwrap().to_str().unwrap();
            if !file_name.ends_with("_test.rs") {
                println!("-- Running example: {}", file_name);
                let output = std::process::Command::new("cargo")
                    .args(&["run", "--example", file_name, "--all-features"])
                    .envs([
                        ("CLICKHOUSE_URL", "http://localhost:8123"),
                        ("CLICKHOUSE_USER", "default"),
                        ("CLICKHOUSE_PASSWORD", ""),
                    ])
                    .output()
                    .expect(&format!("Failed to execute example {}", file_name));
                assert!(
                    output.status.success(),
                    "Example '{}' failed with stderr: {}",
                    file_name,
                    String::from_utf8_lossy(&output.stderr)
                );
            }
        }
    }
}
