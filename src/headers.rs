use crate::ProductInfo;
use hyper::header::USER_AGENT;
use hyper::http::request::Builder;
use std::collections::HashMap;

// See https://doc.rust-lang.org/cargo/reference/environment-variables.html#environment-variables-cargo-sets-for-crates
const PKG_VER: &str = option_env!("CARGO_PKG_VERSION").unwrap_or("unknown");
const RUST_VER: &str = option_env!("CARGO_PKG_RUST_VERSION").unwrap_or("unknown");
const OS: &str = std::env::consts::OS;

fn get_user_agent(products_info: &[ProductInfo]) -> String {
    let default_agent = format!("clickhouse-rs/{PKG_VER} (lv:rust/{RUST_VER}, os:{OS})");
    if products_info.is_empty() {
        default_agent
    } else {
        let products = products_info
            .iter()
            .rev()
            .map(|product_info| product_info.to_string())
            .collect::<Vec<String>>()
            .join(" ");
        format!("{products} {default_agent}")
    }
}

pub(crate) fn with_request_headers(
    mut builder: Builder,
    headers: &HashMap<String, String>,
    products_info: &[ProductInfo],
) -> Builder {
    for (name, value) in headers {
        builder = builder.header(name, value);
    }
    builder = builder.header(USER_AGENT.to_string(), get_user_agent(products_info));
    builder
}
