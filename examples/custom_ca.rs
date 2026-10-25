use clickhouse::Client;
use rustls::pki_types::{CertificateDer, pem::PemObject};
use std::env;

// Connects over HTTPS to a ClickHouse server whose certificate is signed by a private CA.
// Only that CA is trusted, so no public roots feature is needed:
//
//   cargo run --example custom_ca --no-default-features --features rustls-tls-aws-lc
//
// Required environment variables:
//
// - CLICKHOUSE_URL (e.g., https://clickhouse.internal:8443)
// - CLICKHOUSE_CA_CERT (path to the CA certificate in PEM format)

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(read_env_var("CLICKHOUSE_CA_CERT"))? {
        roots.add(cert?)?;
    }
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    let client = Client::with_rustls_config(config).with_url(read_env_var("CLICKHOUSE_URL"));

    let version = client
        .query("SELECT version()")
        .fetch_one::<String>()
        .await?;
    println!("Connected to ClickHouse {version}");

    Ok(())
}

fn read_env_var(key: &str) -> String {
    env::var(key).unwrap_or_else(|_| panic!("{key} env variable should be set"))
}
