use hyper::header::USER_AGENT;
use hyper::http::request::Builder;
use std::collections::HashMap;
use std::env;

fn get_user_agent(app_name: Option<&str>) -> String {
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
