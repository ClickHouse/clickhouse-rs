//! Public server version information from the native protocol handshake.

/// ClickHouse server version information received during the native TCP handshake.
///
/// Available after the first connection is established.  Obtain via
/// [`crate::native::NativeClient::server_version`] or
/// [`crate::unified::UnifiedClient::server_version`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerVersion {
    /// Human-readable server name (e.g. `"ClickHouse"`).
    pub name: String,
    /// Major version component.
    pub major: u64,
    /// Minor version component.
    pub minor: u64,
    /// Patch version component.
    pub patch: u64,
    /// Internal revision number (monotonically increasing, used for protocol
    /// feature negotiation).
    pub revision: u64,
    /// Server timezone (e.g. `"UTC"`), if reported.
    pub timezone: Option<String>,
    /// Server display name, if reported.
    pub display_name: Option<String>,
}
