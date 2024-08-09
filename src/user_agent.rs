use std::env;

pub(crate) fn get_user_agent(app_name: Option<&str>) -> String {
    // See https://doc.rust-lang.org/cargo/reference/environment-variables.html#environment-variables-cargo-sets-for-crates
    let pkg_version = env!("CARGO_PKG_VERSION");
    let rust_version = env!("CARGO_PKG_RUST_VERSION");
    let os = env::consts::OS;
    let default_agent = format!("clickhouse-rs/{pkg_version} (lv:rust/{rust_version}, os:{os})");
    if let Some(app_name) = app_name {
        format!("{app_name} {default_agent}")
    } else {
        default_agent
    }
}
