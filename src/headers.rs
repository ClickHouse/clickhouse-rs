use hyper::header::USER_AGENT;
use hyper::http::request::Builder;
use std::collections::HashMap;

// See https://doc.rust-lang.org/cargo/reference/environment-variables.html#environment-variables-cargo-sets-for-crates
const PKG_VER: &str = env!("CARGO_PKG_VERSION", "unknown");
const RUST_VER: &str = env!("CARGO_PKG_RUST_VERSION", "unknown");
const OS: &str = std::env::consts::OS;

fn get_user_agent(app_name: Option<&str>) -> String {
    let default_agent = format!("clickhouse-rs/{PKG_VER} (lv:rust/{RUST_VER}, os:{OS})");
    if let Some(app_name) = app_name {
        format!("{app_name} {default_agent}")
    } else {
        default_agent
    }
}

pub(crate) fn with_request_headers(
    mut builder: Builder,
    headers: &HashMap<String, String>,
    app_name: Option<&str>,
) -> Builder {
    for (name, value) in headers {
        builder = builder.header(name, value);
    }
    builder = builder.header(USER_AGENT.to_string(), get_user_agent(app_name));
    builder
}
