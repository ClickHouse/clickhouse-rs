use std::time::Duration;

use hyper::Request;
use hyper_util::{
    client::legacy::{
        Client, Client as HyperClient, ResponseFuture,
        connect::{Connect, HttpConnector},
    },
    rt::TokioExecutor,
};

use crate::request_body::RequestBody;

/// A trait for underlying HTTP client.
///
/// Firstly, now it is implemented only for
/// `hyper_util::client::legacy::Client`, it's impossible to use another HTTP
/// client.
///
/// Secondly, although it's stable in terms of semver, it will be changed in the
/// future (e.g. to support more runtimes, not only tokio). Thus, prefer to open
/// a feature request instead of implementing this trait manually.
pub trait HttpClient: sealed::Sealed + Send + Sync + 'static {
    fn request(&self, req: Request<RequestBody>) -> ResponseFuture;
}

impl<C> HttpClient for Client<C, RequestBody>
where
    C: Connect + Clone + Send + Sync + 'static,
{
    fn request(&self, req: Request<RequestBody>) -> ResponseFuture {
        self.request(req)
    }
}

impl<C> sealed::Sealed for Client<C, RequestBody> {}

// === Default ===

const TCP_KEEPALIVE: Duration = Duration::from_secs(60);

// ClickHouse uses 3s by default.
// See https://github.com/ClickHouse/ClickHouse/blob/368cb74b4d222dc5472a7f2177f6bb154ebae07a/programs/server/config.xml#L201
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(2);

pub(crate) fn default() -> impl HttpClient {
    let connector = http_connector();

    #[cfg(feature = "native-tls")]
    let connector = hyper_tls::HttpsConnector::new_with_connector(connector);

    #[cfg(all(feature = "rustls-tls-aws-lc", not(feature = "native-tls")))]
    let connector =
        prepare_hyper_rustls_connector(connector, rustls::crypto::aws_lc_rs::default_provider());

    #[cfg(all(
        feature = "rustls-tls-ring",
        not(feature = "rustls-tls-aws-lc"),
        not(feature = "native-tls"),
    ))]
    let connector =
        prepare_hyper_rustls_connector(connector, rustls::crypto::ring::default_provider());

    build(connector)
}

/// The default client, but with a user-provided rustls config.
#[cfg(any(feature = "rustls-tls-aws-lc", feature = "rustls-tls-ring"))]
pub(crate) fn with_rustls_config(config: rustls::ClientConfig) -> impl HttpClient {
    let builder = hyper_rustls::HttpsConnectorBuilder::new().with_tls_config(config);
    build(wrap_hyper_rustls_connector(builder, http_connector()))
}

fn http_connector() -> HttpConnector {
    let mut connector = HttpConnector::new();

    // TODO: make configurable in `Client::builder()`.
    connector.set_keepalive(Some(TCP_KEEPALIVE));

    connector.enforce_http(!cfg!(any(
        feature = "native-tls",
        feature = "rustls-tls-aws-lc",
        feature = "rustls-tls-ring",
    )));

    connector
}

fn build<C>(connector: C) -> Client<C, RequestBody>
where
    C: Connect + Clone + Send + Sync + 'static,
{
    HyperClient::builder(TokioExecutor::new())
        .pool_idle_timeout(POOL_IDLE_TIMEOUT)
        .build(connector)
}

#[cfg(not(feature = "native-tls"))]
#[cfg(any(feature = "rustls-tls-aws-lc", feature = "rustls-tls-ring"))]
fn prepare_hyper_rustls_connector(
    connector: HttpConnector,
    provider: rustls::crypto::CryptoProvider,
) -> hyper_rustls::HttpsConnector<HttpConnector> {
    #[cfg(feature = "rustls-tls-native-roots")]
    let builder = hyper_rustls::HttpsConnectorBuilder::new()
        .with_provider_and_native_roots(provider)
        .unwrap();

    #[cfg(all(
        feature = "rustls-tls-webpki-roots",
        not(feature = "rustls-tls-native-roots")
    ))]
    let builder = hyper_rustls::HttpsConnectorBuilder::new()
        .with_provider_and_webpki_roots(provider)
        .unwrap();

    // Without a roots feature, nothing is trusted until `Client::with_rustls_config()` is used.
    #[cfg(not(any(
        feature = "rustls-tls-webpki-roots",
        feature = "rustls-tls-native-roots"
    )))]
    let builder = hyper_rustls::HttpsConnectorBuilder::new().with_tls_config(
        rustls::ClientConfig::builder_with_provider(provider.into())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth(),
    );

    wrap_hyper_rustls_connector(builder, connector)
}

#[cfg(any(feature = "rustls-tls-aws-lc", feature = "rustls-tls-ring"))]
fn wrap_hyper_rustls_connector(
    builder: hyper_rustls::HttpsConnectorBuilder<hyper_rustls::builderstates::WantsSchemes>,
    connector: HttpConnector,
) -> hyper_rustls::HttpsConnector<HttpConnector> {
    builder
        .https_or_http()
        .enable_http1()
        .wrap_connector(connector)
}

mod sealed {
    pub trait Sealed {}
}

#[cfg(test)]
#[cfg(any(feature = "rustls-tls-aws-lc", feature = "rustls-tls-ring"))]
mod tests {
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::Arc,
    };

    use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};

    use super::*;

    const CA: &[u8] = include_bytes!("../tests/it/fixtures/tls/ca.pem");
    const SERVER_CERT: &[u8] = include_bytes!("../tests/it/fixtures/tls/server.pem");
    const SERVER_KEY: &[u8] = include_bytes!("../tests/it/fixtures/tls/server.key");

    fn provider() -> Arc<rustls::crypto::CryptoProvider> {
        #[cfg(feature = "rustls-tls-aws-lc")]
        return Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        #[cfg(not(feature = "rustls-tls-aws-lc"))]
        return Arc::new(rustls::crypto::ring::default_provider());
    }

    /// Serves one HTTPS connection with the certificate signed by the test CA.
    fn spawn_server() -> u16 {
        let config = rustls::ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![CertificateDer::from_pem_slice(SERVER_CERT).unwrap()],
                PrivateKeyDer::from_pem_slice(SERVER_KEY).unwrap(),
            )
            .unwrap();
        let config = Arc::new(config);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let conn = rustls::ServerConnection::new(config).unwrap();
            let mut tls = rustls::StreamOwned::new(conn, stream);
            let mut buf = [0; 4096];
            // Fails if the client rejects the certificate, which is fine.
            if tls.read(&mut buf).is_ok() {
                let _ = tls.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n");
                let _ = tls.flush();
            }
        });

        port
    }

    fn get(port: u16) -> Request<RequestBody> {
        Request::get(format!("https://127.0.0.1:{port}/"))
            .body(RequestBody::full(""))
            .unwrap()
    }

    #[tokio::test]
    async fn rustls_config_with_custom_ca() {
        let port = spawn_server();

        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(CertificateDer::from_pem_slice(CA).unwrap())
            .unwrap();
        let config = rustls::ClientConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();

        let response = with_rustls_config(config).request(get(port)).await.unwrap();
        assert_eq!(response.status(), 200);
    }

    #[tokio::test]
    async fn default_client_rejects_unknown_ca() {
        let port = spawn_server();

        let err = default().request(get(port)).await.unwrap_err();
        let err = format!("{:?}", std::error::Error::source(&err)).to_lowercase();
        assert!(err.contains("certificate"), "unexpected error: {err}");
    }
}
