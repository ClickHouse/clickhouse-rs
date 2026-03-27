//! ClickHouse native TCP protocol definitions.
//!
//! Packet IDs, handshake structures, version constants, and compression methods
//! for the native binary protocol (port 9000).

use std::str::FromStr;

use crate::error::{Error, Result};

// === Protocol version constants ===

pub(crate) const DBMS_MIN_REVISION_WITH_CLIENT_INFO: u64 = 54032;
pub(crate) const DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE: u64 = 54058;
pub(crate) const DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO: u64 = 54060;
pub(crate) const DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME: u64 = 54372;
pub(crate) const DBMS_MIN_REVISION_WITH_VERSION_PATCH: u64 = 54401;
pub(crate) const DBMS_MIN_REVISION_WITH_SERVER_LOGS: u64 = 54406;
pub(crate) const DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO: u64 = 54420;
#[allow(dead_code)] // Protocol constant -- used when settings serialisation as strings is wired
pub(crate) const DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS: u64 = 54429;
pub(crate) const DBMS_MIN_REVISION_WITH_OPENTELEMETRY: u64 = 54442;
pub(crate) const DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET: u64 = 54441;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH: u64 = 54448;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_QUERY_START_TIME: u64 = 54449;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_PARALLEL_REPLICAS: u64 = 54453;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION: u64 = 54454;
#[allow(dead_code)] // Protocol constant -- used when profile events during INSERT are surfaced
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_PROFILE_EVENTS_IN_INSERT: u64 = 54456;
#[allow(dead_code)] // Protocol constant -- used when addendum packet handling is wired
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM: u64 = 54458;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_QUOTA_KEY: u64 = 54458;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS: u64 = 54459;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS: u64 = 54460;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_PASSWORD_COMPLEXITY_RULES: u64 = 54461;
pub(crate) const DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2: u64 = 54462;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS: u64 = 54463;
pub(crate) const DBMS_MIN_REVISION_WITH_ROWS_BEFORE_AGGREGATION: u64 = 54469;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS: u64 = 54470;
pub(crate) const DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL: u64 = 54471;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_INTERSERVER_EXTERNALLY_GRANTED_ROLES: u64 = 54472;
pub(crate) const DBMS_MIN_REVISION_WITH_SERVER_SETTINGS: u64 = 54474;
pub(crate) const DBMS_MIN_REVISION_WITH_QUERY_AND_LINE_NUMBERS: u64 = 54475;
pub(crate) const DBMS_MIN_REVISION_WITH_JWT_IN_INTERSERVER: u64 = 54476;
pub(crate) const DBMS_MIN_REVISION_WITH_QUERY_PLAN_SERIALIZATION: u64 = 54477;
pub(crate) const DBMS_MIN_REVISION_WITH_VERSIONED_CLUSTER_FUNCTION_PROTOCOL: u64 = 54479;

/// Active protocol version this client advertises.
pub(crate) const DBMS_TCP_PROTOCOL_VERSION: u64 =
    DBMS_MIN_REVISION_WITH_VERSIONED_CLUSTER_FUNCTION_PROTOCOL;

pub(crate) const DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION: u64 = 4;

/// Maximum string size over the native protocol (1 GiB).
pub(crate) const MAX_STRING_SIZE: usize = 1 << 30;

// === Query processing stage ===

#[repr(u64)]
#[derive(Clone, Copy, Debug)]
#[allow(unused)]
pub(crate) enum QueryProcessingStage {
    FetchColumns,
    WithMergeableState,
    Complete,
    WithMergableStateAfterAggregation,
}

// === Client packets ===

#[allow(unused)]
#[repr(u64)]
#[derive(Clone, Copy, Debug)]
pub(crate) enum ClientPacketId {
    Hello = 0,
    Query = 1,
    Data = 2,
    Cancel = 3,
    Ping = 4,
    TablesStatusRequest = 5,
    KeepAlive = 6,
    Scalar = 7,
    IgnoredPartUUIDs = 8,
    ReadTaskResponse = 9,
    MergeTreeReadTaskResponse = 10,
    SSHChallengeRequest = 11,
    SSHChallengeResponse = 12,
    QueryPlan = 13,
}

// === Server packets ===

#[repr(u64)]
#[derive(Clone, Copy, Debug)]
pub(crate) enum ServerPacketId {
    Hello = 0,
    Data = 1,
    Exception = 2,
    Progress = 3,
    Pong = 4,
    EndOfStream = 5,
    ProfileInfo = 6,
    Totals = 7,
    Extremes = 8,
    TablesStatusResponse = 9,
    Log = 10,
    TableColumns = 11,
    PartUUIDs = 12,
    ReadTaskRequest = 13,
    ProfileEvents = 14,
    MergeTreeAllRangesAnnouncement = 15,
    MergeTreeReadTaskRequest = 16,
    TimezoneUpdate = 17,
    SSHChallenge = 18,
}

impl ServerPacketId {
    pub(crate) fn from_u64(i: u64) -> Result<Self> {
        Ok(match i {
            0 => ServerPacketId::Hello,
            1 => ServerPacketId::Data,
            2 => ServerPacketId::Exception,
            3 => ServerPacketId::Progress,
            4 => ServerPacketId::Pong,
            5 => ServerPacketId::EndOfStream,
            6 => ServerPacketId::ProfileInfo,
            7 => ServerPacketId::Totals,
            8 => ServerPacketId::Extremes,
            9 => ServerPacketId::TablesStatusResponse,
            10 => ServerPacketId::Log,
            11 => ServerPacketId::TableColumns,
            12 => ServerPacketId::PartUUIDs,
            13 => ServerPacketId::ReadTaskRequest,
            14 => ServerPacketId::ProfileEvents,
            15 => ServerPacketId::MergeTreeAllRangesAnnouncement,
            16 => ServerPacketId::MergeTreeReadTaskRequest,
            17 => ServerPacketId::TimezoneUpdate,
            18 => ServerPacketId::SSHChallenge,
            x => {
                return Err(Error::BadResponse(format!(
                    "native protocol: unknown server packet id {x}"
                )));
            }
        })
    }
}

// === Server response structures ===

#[derive(Debug, Clone, Default)]
pub(crate) struct ServerHello {
    pub server_name: String,
    pub version: (u64, u64, u64),
    pub revision_version: u64,
    pub timezone: Option<String>,
    pub display_name: Option<String>,
    pub(crate) chunked_send: ChunkedProtocolMode,
    pub(crate) chunked_recv: ChunkedProtocolMode,
}

impl ServerHello {
    #[allow(unused)]
    pub(crate) fn supports_chunked_send(&self) -> bool {
        matches!(
            self.chunked_send,
            ChunkedProtocolMode::Chunked | ChunkedProtocolMode::ChunkedOptional
        )
    }

    #[allow(unused)]
    pub(crate) fn supports_chunked_recv(&self) -> bool {
        matches!(
            self.chunked_recv,
            ChunkedProtocolMode::Chunked | ChunkedProtocolMode::ChunkedOptional
        )
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ServerException {
    pub(crate) code: i32,
    pub(crate) name: String,
    pub(crate) message: String,
    pub(crate) stack_trace: String,
    pub(crate) _has_nested: bool, // read from wire; nested exceptions not yet surfaced
}

#[allow(unused)]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ProfileInfo {
    pub rows: u64,
    pub blocks: u64,
    pub bytes: u64,
    pub applied_limit: bool,
    pub rows_before_limit: u64,
    pub calculated_rows_before_limit: bool,
    pub applied_aggregation: bool,
    pub rows_before_aggregation: u64,
}

#[allow(unused)]
#[derive(Debug, Clone)]
pub(crate) struct TableColumns {
    pub(crate) name: String,
    pub(crate) description: String,
}

// === Progress ===

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct Progress {
    pub read_rows: u64,
    pub read_bytes: u64,
    pub total_rows_to_read: u64,
    pub total_bytes_to_read: Option<u64>,
    pub written_rows: Option<u64>,
    pub written_bytes: Option<u64>,
    pub elapsed_ns: Option<u64>,
}

impl std::ops::Add for Progress {
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        Self {
            read_rows: self.read_rows + rhs.read_rows,
            read_bytes: self.read_bytes + rhs.read_bytes,
            total_rows_to_read: self.total_rows_to_read + rhs.total_rows_to_read,
            total_bytes_to_read: match (self.total_bytes_to_read, rhs.total_bytes_to_read) {
                (Some(a), Some(b)) => Some(a + b),
                (a, b) => a.or(b),
            },
            written_rows: match (self.written_rows, rhs.written_rows) {
                (Some(a), Some(b)) => Some(a + b),
                (a, b) => a.or(b),
            },
            written_bytes: match (self.written_bytes, rhs.written_bytes) {
                (Some(a), Some(b)) => Some(a + b),
                (a, b) => a.or(b),
            },
            elapsed_ns: match (self.elapsed_ns, rhs.elapsed_ns) {
                (Some(a), Some(b)) => Some(a + b),
                (a, b) => a.or(b),
            },
        }
    }
}

impl std::ops::AddAssign for Progress {
    fn add_assign(&mut self, rhs: Self) {
        *self = std::mem::take(self) + rhs;
    }
}

// === Chunked protocol negotiation ===

#[derive(Clone, Default, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ChunkedProtocolMode {
    #[default]
    ChunkedOptional,
    Chunked,
    NotChunkedOptional,
    NotChunked,
}

impl ChunkedProtocolMode {
    /// Negotiates chunked protocol between client and server (based on C++ `is_chunked` function).
    pub(crate) fn negotiate(
        server_mode: ChunkedProtocolMode,
        client_mode: ChunkedProtocolMode,
        direction: &str,
    ) -> Result<ChunkedProtocolMode> {
        let server_chunked = matches!(
            server_mode,
            ChunkedProtocolMode::Chunked | ChunkedProtocolMode::ChunkedOptional
        );
        let server_optional = matches!(
            server_mode,
            ChunkedProtocolMode::ChunkedOptional | ChunkedProtocolMode::NotChunkedOptional
        );
        let client_chunked = matches!(
            client_mode,
            ChunkedProtocolMode::Chunked | ChunkedProtocolMode::ChunkedOptional
        );
        let client_optional = matches!(
            client_mode,
            ChunkedProtocolMode::ChunkedOptional | ChunkedProtocolMode::NotChunkedOptional
        );
        let result_chunked = if server_optional {
            client_chunked
        } else if client_optional {
            server_chunked
        } else if client_chunked != server_chunked {
            return Err(Error::BadResponse(format!(
                "native protocol: incompatible chunked mode for {direction}: \
                 client={}, server={}",
                if client_chunked {
                    "chunked"
                } else {
                    "notchunked"
                },
                if server_chunked {
                    "chunked"
                } else {
                    "notchunked"
                },
            )));
        } else {
            server_chunked
        };

        Ok(if result_chunked {
            ChunkedProtocolMode::Chunked
        } else {
            ChunkedProtocolMode::NotChunked
        })
    }
}

impl FromStr for ChunkedProtocolMode {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "chunked" => Self::Chunked,
            "chunked_optional" => Self::ChunkedOptional,
            "notchunked" => Self::NotChunked,
            "notchunked_optional" => Self::NotChunkedOptional,
            _ => {
                return Err(Error::BadResponse(format!(
                    "native protocol: unexpected chunked mode: {s}"
                )));
            }
        })
    }
}

impl std::fmt::Display for ChunkedProtocolMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ChunkedOptional => write!(f, "chunked_optional"),
            Self::Chunked => write!(f, "chunked"),
            Self::NotChunkedOptional => write!(f, "notchunked_optional"),
            Self::NotChunked => write!(f, "notchunked"),
        }
    }
}

// === Compression method for native protocol ===

#[derive(Clone, Default, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum NativeCompressionMethod {
    None,
    #[default]
    Lz4,
    Zstd,
}

impl NativeCompressionMethod {
    pub(crate) fn byte(self) -> u8 {
        match self {
            NativeCompressionMethod::None => 0x02,
            NativeCompressionMethod::Lz4 => 0x82,
            NativeCompressionMethod::Zstd => 0x90,
        }
    }
}

impl std::fmt::Display for NativeCompressionMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NativeCompressionMethod::None => write!(f, "None"),
            NativeCompressionMethod::Lz4 => write!(f, "LZ4"),
            NativeCompressionMethod::Zstd => write!(f, "ZSTD"),
        }
    }
}

impl FromStr for NativeCompressionMethod {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "lz4" | "LZ4" => Ok(NativeCompressionMethod::Lz4),
            "zstd" | "ZSTD" => Ok(NativeCompressionMethod::Zstd),
            "none" | "None" => Ok(NativeCompressionMethod::None),
            _ => Err(Error::BadResponse(format!(
                "native protocol: unknown compression method: {s}"
            ))),
        }
    }
}
