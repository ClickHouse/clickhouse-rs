//! TLS for `tcps://` URLs, with the TLS implementation selected by the crate features.

use crate::error::{Error, Result};
use crate::tcp::Address;
use crate::tcp::connection::Stream;
use tokio::net::TcpStream;

pub(crate) async fn wrap(stream: TcpStream, address: &Address) -> Result<Box<dyn Stream>> {
    if !address.secure {
        return Ok(Box::new(stream));
    }

    connect(stream, &address.host).await
}

#[cfg(feature = "native-tls")]
async fn connect(stream: TcpStream, host: &str) -> Result<Box<dyn Stream>> {
    use tokio_native_tls::native_tls;

    let connector = native_tls::TlsConnector::new().map_err(|e| Error::Network(Box::new(e)))?;
    let stream = tokio_native_tls::TlsConnector::from(connector)
        .connect(host, stream)
        .await
        .map_err(|e| Error::Network(Box::new(e)))?;

    Ok(Box::new(stream))
}

#[cfg(all(
    not(feature = "native-tls"),
    any(feature = "rustls-tls-aws-lc", feature = "rustls-tls-ring")
))]
async fn connect(stream: TcpStream, host: &str) -> Result<Box<dyn Stream>> {
    use rustls::pki_types::ServerName;
    use std::sync::{Arc, OnceLock};

    static CONFIG: OnceLock<std::result::Result<Arc<rustls::ClientConfig>, String>> =
        OnceLock::new();

    let config = CONFIG
        .get_or_init(|| rustls_config().map(Arc::new).map_err(|e| e.to_string()))
        .clone()
        .map_err(|e| Error::Network(e.into()))?;

    let server_name =
        ServerName::try_from(host.to_owned()).map_err(|e| Error::InvalidParams(Box::new(e)))?;

    let stream = tokio_rustls::TlsConnector::from(config)
        .connect(server_name, stream)
        .await
        .map_err(|e| Error::Network(Box::new(e)))?;

    Ok(Box::new(stream))
}

#[cfg(all(
    not(feature = "native-tls"),
    any(feature = "rustls-tls-aws-lc", feature = "rustls-tls-ring")
))]
fn rustls_config()
-> std::result::Result<rustls::ClientConfig, Box<dyn std::error::Error + Send + Sync>> {
    use hyper_rustls::ConfigBuilderExt;

    #[cfg(feature = "rustls-tls-aws-lc")]
    let provider = rustls::crypto::aws_lc_rs::default_provider();
    #[cfg(not(feature = "rustls-tls-aws-lc"))]
    let provider = rustls::crypto::ring::default_provider();

    let builder = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(provider))
        .with_safe_default_protocol_versions()?;

    #[cfg(feature = "rustls-tls-native-roots")]
    let builder = builder.with_native_roots()?;

    #[cfg(all(
        feature = "rustls-tls-webpki-roots",
        not(feature = "rustls-tls-native-roots")
    ))]
    let builder = builder.with_webpki_roots();

    Ok(builder.with_no_client_auth())
}

#[cfg(not(any(
    feature = "native-tls",
    feature = "rustls-tls-aws-lc",
    feature = "rustls-tls-ring"
)))]
async fn connect(_stream: TcpStream, _host: &str) -> Result<Box<dyn Stream>> {
    Err(Error::Unsupported(
        "`tcps://` URLs require the `native-tls` or `rustls-tls` feature".into(),
    ))
}
