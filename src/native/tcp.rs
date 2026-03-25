//! TCP connection setup for ClickHouse native protocol.
//!
//! Configures socket options (keepalive, buffer sizes, nodelay) via `socket2`
//! for high-throughput data transfer.
//!
//! # TLS support
//!
//! When the `native-tls-rustls` feature is enabled, [`connect_tls`] wraps the
//! plain TCP socket in a `tokio_rustls::client::TlsStream`.  Both plain and
//! TLS paths return a [`MaybeTlsStream`] -- an enum over the two stream types
//! that implements `AsyncRead + AsyncWrite + Unpin`.
//!
//! This follows the same `MaybeTlsStream` pattern used by `hyper-rustls`,
//! `tungstenite`, and other Rust networking crates.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

use crate::error::{Error, Result};

// Socket configuration constants
const TCP_READ_BUFFER_SIZE: usize = 128 * 1024;
const TCP_WRITE_BUFFER_SIZE: usize = 8 * 1024 * 1024;
const TCP_CONNECT_TIMEOUT_SECS: u64 = 30;
const TCP_KEEP_ALIVE_SECS: u64 = 60;
const TCP_KEEP_ALIVE_INTERVAL: u64 = 10;
const TCP_KEEP_ALIVE_RETRIES: u32 = 6;

// Buffered I/O sizes for the connection
pub(crate) const CONN_READ_BUFFER: usize = 1024 * 1024;
pub(crate) const CONN_WRITE_BUFFER: usize = 10 * 1024 * 1024;

// -----------------------------------------------------------------------
// MaybeTlsStream -- plain TCP or TLS, both AsyncRead + AsyncWrite + Unpin.
//
// Same pattern as hyper-rustls `MaybeHttpsStream` and tungstenite
// `MaybeTlsStream`.  We need this because NativeConnection splits the
// stream into read/write halves via tokio::io::split(), which requires
// a single concrete type implementing AsyncRead + AsyncWrite.
//
// When `native-tls-rustls` is not enabled, this is a plain wrapper
// around TcpStream -- the Tls variant doesn't exist at compile time.
// -----------------------------------------------------------------------

/// A TCP stream that may or may not be wrapped in TLS.
pub(crate) enum MaybeTlsStream {
    /// Plain TCP (port 9000 default).
    Plain(TcpStream),
    /// TLS-wrapped TCP (port 9440 default).
    #[cfg(feature = "native-tls-rustls")]
    Tls(tokio_rustls::client::TlsStream<TcpStream>),
}

impl AsyncRead for MaybeTlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "native-tls-rustls")]
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "native-tls-rustls")]
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "native-tls-rustls")]
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "native-tls-rustls")]
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// Connect to ClickHouse via plain TCP with configured socket options.
pub(crate) async fn connect(addr: &SocketAddr) -> Result<MaybeTlsStream> {
    let stream = connect_tcp(addr).await?;
    Ok(MaybeTlsStream::Plain(stream))
}

/// Connect to ClickHouse via TLS-wrapped TCP.
///
/// The `server_name` is used for SNI and certificate verification.
#[cfg(feature = "native-tls-rustls")]
pub(crate) async fn connect_tls(
    addr: &SocketAddr,
    tls_config: std::sync::Arc<rustls::ClientConfig>,
    server_name: rustls::pki_types::ServerName<'static>,
) -> Result<MaybeTlsStream> {
    let stream = connect_tcp(addr).await?;
    let connector = tokio_rustls::TlsConnector::from(tls_config);
    let tls_stream = connector
        .connect(server_name, stream)
        .await
        .map_err(|e| Error::Network(Box::new(e)))?;
    Ok(MaybeTlsStream::Tls(tls_stream))
}

/// Raw TCP connect with socket2 configuration (shared by plain and TLS paths).
async fn connect_tcp(addr: &SocketAddr) -> Result<TcpStream> {
    let domain = if addr.is_ipv4() {
        socket2::Domain::IPV4
    } else {
        socket2::Domain::IPV6
    };

    let socket =
        socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))
            .map_err(|e| Error::Network(Box::new(e)))?;

    socket
        .set_nonblocking(true)
        .map_err(|e| Error::Network(Box::new(e)))?;
    socket
        .set_recv_buffer_size(TCP_READ_BUFFER_SIZE)
        .map_err(|e| Error::Network(Box::new(e)))?;
    socket
        .set_send_buffer_size(TCP_WRITE_BUFFER_SIZE)
        .map_err(|e| Error::Network(Box::new(e)))?;

    let keepalive = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(TCP_KEEP_ALIVE_SECS))
        .with_interval(Duration::from_secs(TCP_KEEP_ALIVE_INTERVAL))
        .with_retries(TCP_KEEP_ALIVE_RETRIES);
    socket
        .set_tcp_keepalive(&keepalive)
        .map_err(|e| Error::Network(Box::new(e)))?;

    let sock_addr = socket2::SockAddr::from(*addr);
    socket
        .connect_timeout(&sock_addr, Duration::from_secs(TCP_CONNECT_TIMEOUT_SECS))
        .map_err(|e| Error::Network(Box::new(e)))?;

    let stream = std::net::TcpStream::from(socket);
    stream
        .set_nodelay(true)
        .map_err(|e| Error::Network(Box::new(e)))?;
    stream
        .set_nonblocking(true)
        .map_err(|e| Error::Network(Box::new(e)))?;

    TcpStream::from_std(stream).map_err(|e| Error::Network(Box::new(e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_connect_refused() {
        // Connecting to a port with nothing listening should fail
        let addr: SocketAddr = "127.0.0.1:19999".parse().unwrap();
        let result = connect(&addr).await;
        assert!(result.is_err());
    }
}
