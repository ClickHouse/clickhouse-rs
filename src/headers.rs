use crate::{Authentication, ProductInfo};
use hyper::header::{AUTHORIZATION, USER_AGENT};
use hyper::http::request::Builder;
use std::collections::HashMap;
use std::env::consts::OS;

fn get_user_agent(app_product_info: &[ProductInfo], stack_product_info: &[ProductInfo]) -> String {
    use std::fmt::Write;

    // See https://doc.rust-lang.org/cargo/reference/environment-variables.html#environment-variables-cargo-sets-for-crates
    let pkg_ver = option_env!("CARGO_PKG_VERSION").unwrap_or("unknown");
    let rust_ver = option_env!("CARGO_PKG_RUST_VERSION").unwrap_or("unknown");

    let mut infix = "";

    let mut user_agent = String::new();

    for product_info in stack_product_info.iter().chain(app_product_info).rev() {
        write!(user_agent, "{infix}{product_info}")
            .expect("BUG: formatting to a string should be infallible");

        infix = " ";
    }

    write!(
        user_agent,
        "{infix}clickhouse-rs/{pkg_ver} (lv:rust/{rust_ver}; os:{OS})"
    )
    .expect("BUG: formatting to a string should be infallible");

    user_agent
}

#[inline]
pub(crate) fn with_request_headers(
    mut builder: Builder,
    headers: &HashMap<String, String>,
    app_product_info: &[ProductInfo],
    stack_product_info: &[ProductInfo],
) -> Builder {
    // Inject the OpenTelemetry trace context if the feature is enabled
    #[cfg(feature = "opentelemetry")]
    opentelemetry::global::get_text_map_propagator(|propagator| {
        use opentelemetry_http::HeaderInjector;

        // Will only be `None` if there's already an error in building the request,
        // in which case injecting the headers would be redundant anyway.
        let Some(headers) = builder.headers_mut() else {
            return;
        };

        // Note that `HeaderInjector` skips headers with invalid names, as of writing.
        propagator.inject(&mut HeaderInjector(headers));
    });

    for (name, value) in headers {
        builder = builder.header(name, value);
    }
    builder = builder.header(
        USER_AGENT.to_string(),
        get_user_agent(app_product_info, stack_product_info),
    );
    builder
}

#[inline]
pub(crate) fn with_authentication(mut builder: Builder, auth: &Authentication) -> Builder {
    match auth {
        Authentication::Jwt { access_token } => {
            let bearer = format!("Bearer {access_token}");
            builder = builder.header(AUTHORIZATION, bearer);
        }
        Authentication::Credentials { user, password } => {
            if let Some(user) = &user {
                builder = builder.header("X-ClickHouse-User", user);
            }
            if let Some(password) = &password {
                builder = builder.header("X-ClickHouse-Key", password);
            }
        }
    }
    builder
}
