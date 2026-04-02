//! TCP connection setup for ClickHouse native protocol.
//!
//! Configures socket options (keepalive, buffer sizes, nodelay) via `socket2`
//! for high-throughput data transfer on port 9000.

use std::net::SocketAddr;
use std::time::Duration;

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

/// Connect to ClickHouse via TCP with configured socket options.
pub(crate) async fn connect(addr: &SocketAddr) -> Result<TcpStream> {
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
