//! Packet reader for ClickHouse native protocol.
//!
//! Reads and dispatches server packets: hello, exception, progress,
//! profile info, data blocks, and end-of-stream markers.

use std::str::FromStr;

use tokio::io::AsyncReadExt;

use crate::error::{Error, Result};
use crate::native::block_info::BlockInfo;
use crate::native::columns::{
    self, ColumnData, ColumnType, transpose_to_rowbinary, write_var_uint,
};
use crate::native::compression::decompress_data;
use crate::native::error_codes::{self, ServerError};
use crate::native::io::ClickHouseRead;
use crate::native::protocol::DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION;
use crate::native::protocol::{
    ChunkedProtocolMode, DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS,
    DBMS_MIN_PROTOCOL_VERSION_WITH_PASSWORD_COMPLEXITY_RULES,
    DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS,
    DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS,
    DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO, DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2,
    DBMS_MIN_REVISION_WITH_QUERY_PLAN_SERIALIZATION,
    DBMS_MIN_REVISION_WITH_ROWS_BEFORE_AGGREGATION, DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME,
    DBMS_MIN_REVISION_WITH_SERVER_LOGS, DBMS_MIN_REVISION_WITH_SERVER_SETTINGS,
    DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE, DBMS_MIN_REVISION_WITH_VERSION_PATCH,
    DBMS_MIN_REVISION_WITH_VERSIONED_CLUSTER_FUNCTION_PROTOCOL,
    DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL, NativeCompressionMethod,
    ProfileInfo, Progress, ServerException, ServerHello, ServerPacketId, TableColumns,
};
use crate::native::sparse::{SparseDeserializeState, read_sparse_offsets};

/// Server packet after dispatch.
#[derive(Debug)]
#[allow(unused)]
pub(crate) enum ServerPacket {
    Hello(ServerHello),
    Data(DataBlock),
    Exception(ServerError),
    Progress(Progress),
    Pong,
    EndOfStream,
    ProfileInfo(ProfileInfo),
    TableColumns(TableColumns),
}

/// A fully-read data block from the server.
#[derive(Debug)]
pub(crate) struct DataBlock {
    pub(crate) _info: BlockInfo, // read from wire; not yet exposed to callers
    pub(crate) _num_columns: u64, // derived from column_headers.len(); wire value kept for parity
    pub(crate) num_rows: u64,
    /// Column name + type.
    pub(crate) column_headers: Vec<ColumnHeader>,
    /// Row-oriented RowBinary bytes, one `Vec<u8>` per row.
    ///
    /// Each element is the complete RowBinary bytes for one row, ready for
    /// `rowbinary::deserialize_row()`.  Empty if `num_rows == 0`.
    pub(crate) row_data: Vec<Vec<u8>>,
}

/// Column name + type string from a data block header.
#[derive(Debug, Clone)]
pub(crate) struct ColumnHeader {
    pub(crate) name: String,
    pub(crate) type_name: String,
}

/// Read server hello response.
pub(crate) async fn read_hello<R: ClickHouseRead>(
    reader: &mut R,
    client_revision: u64,
    chunked_modes: (ChunkedProtocolMode, ChunkedProtocolMode),
) -> Result<ServerHello> {
    let packet_id = ServerPacketId::from_u64(reader.read_var_uint().await?)?;
    match packet_id {
        ServerPacketId::Hello => read_hello_body(reader, client_revision, chunked_modes).await,
        ServerPacketId::Exception => {
            let exc = read_exception(reader).await?;
            Err(Error::BadResponse(format!(
                "server exception during hello: {}: {}",
                exc.name, exc.message
            )))
        }
        other => Err(Error::BadResponse(format!(
            "native protocol: expected hello, got {other:?}"
        ))),
    }
}

/// Read the body of a server hello packet (after packet ID).
async fn read_hello_body<R: ClickHouseRead>(
    reader: &mut R,
    client_revision: u64,
    chunked_modes: (ChunkedProtocolMode, ChunkedProtocolMode),
) -> Result<ServerHello> {
    let server_name = reader.read_utf8_string().await?;
    let major = reader.read_var_uint().await?;
    let minor = reader.read_var_uint().await?;
    let server_revision = reader.read_var_uint().await?;
    let revision = std::cmp::min(server_revision, client_revision);

    if revision >= DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL {
        let _ = reader.read_var_uint().await?;
    }

    let timezone = if revision >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
        Some(reader.read_utf8_string().await?)
    } else {
        None
    };

    let display_name = if revision >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME {
        Some(reader.read_utf8_string().await?)
    } else {
        None
    };

    let patch = if revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
        reader.read_var_uint().await?
    } else {
        revision
    };

    let (chunked_send, chunked_recv) = if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS
    {
        let srv_send =
            ChunkedProtocolMode::from_str(&String::from_utf8_lossy(&reader.read_string().await?))
                .unwrap_or_default();
        let srv_recv =
            ChunkedProtocolMode::from_str(&String::from_utf8_lossy(&reader.read_string().await?))
                .unwrap_or_default();

        (
            ChunkedProtocolMode::negotiate(srv_send, chunked_modes.0, "send")?,
            ChunkedProtocolMode::negotiate(srv_recv, chunked_modes.1, "recv")?,
        )
    } else {
        (
            ChunkedProtocolMode::default(),
            ChunkedProtocolMode::default(),
        )
    };

    if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_PASSWORD_COMPLEXITY_RULES {
        let rules_size = reader.read_var_uint().await?;
        for _ in 0..rules_size {
            drop(reader.read_utf8_string().await?);
            drop(reader.read_utf8_string().await?);
        }
    }

    if revision >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2 {
        let _ = reader.read_u64_le().await?;
    }

    // Skip server settings
    if revision >= DBMS_MIN_REVISION_WITH_SERVER_SETTINGS {
        skip_settings(reader).await?;
    }

    if revision >= DBMS_MIN_REVISION_WITH_QUERY_PLAN_SERIALIZATION {
        let _ = reader.read_var_uint().await?;
    }

    if revision >= DBMS_MIN_REVISION_WITH_VERSIONED_CLUSTER_FUNCTION_PROTOCOL {
        let _ = reader.read_var_uint().await?;
    }

    Ok(ServerHello {
        server_name,
        version: (major, minor, patch),
        revision_version: revision,
        timezone,
        display_name,
        chunked_send,
        chunked_recv,
    })
}

/// Skip settings key/value pairs from the wire.
async fn skip_settings<R: ClickHouseRead>(reader: &mut R) -> Result<()> {
    loop {
        let name = reader.read_utf8_string().await?;
        if name.is_empty() {
            break;
        }
        // Each setting: flag (varuint) + value_string
        let _is_important = reader.read_var_uint().await?;
        let _value = reader.read_string().await?;
    }
    Ok(())
}

/// Read a server exception chain from the wire.
///
/// ClickHouse sends exceptions as a linked list: each exception has a
/// `has_nested` flag, and if true the next exception follows immediately.
/// We MUST consume the entire chain or the stream goes out of sync.
///
/// Returns the outermost exception. Nested exceptions are appended to
/// the message -- they're usually the root cause.
pub(crate) async fn read_exception<R: ClickHouseRead>(reader: &mut R) -> Result<ServerException> {
    let mut first: Option<ServerException> = None;
    let mut nested_messages = Vec::new();

    loop {
        let code = reader.read_i32_le().await?;
        let name = reader.read_utf8_string().await?;
        let message = String::from_utf8_lossy(&reader.read_string().await?).to_string();
        let stack_trace = reader.read_utf8_string().await?;
        let has_nested = reader.read_u8().await? != 0;

        if first.is_none() {
            first = Some(ServerException {
                code,
                name,
                message,
                stack_trace,
                _has_nested: has_nested,
            });
        } else {
            // Append nested exception info to help with debugging.
            nested_messages.push(format!("{name}: {message}"));
        }

        if !has_nested {
            break;
        }
    }

    let mut exc = first.expect("at least one exception");
    if !nested_messages.is_empty() {
        exc.message = format!(
            "{}\nCaused by: {}",
            exc.message,
            nested_messages.join("\nCaused by: ")
        );
    }
    Ok(exc)
}

/// Read progress from the wire.
pub(crate) async fn read_progress<R: ClickHouseRead>(
    reader: &mut R,
    revision: u64,
) -> Result<Progress> {
    let read_rows = reader.read_var_uint().await?;
    let read_bytes = reader.read_var_uint().await?;

    let total_rows_to_read = if revision >= DBMS_MIN_REVISION_WITH_SERVER_LOGS {
        reader.read_var_uint().await?
    } else {
        0
    };

    let total_bytes_to_read = if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS
    {
        Some(reader.read_var_uint().await?)
    } else {
        None
    };

    let written = if revision >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO {
        Some((reader.read_var_uint().await?, reader.read_var_uint().await?))
    } else {
        None
    };

    let elapsed_ns = if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS {
        Some(reader.read_var_uint().await?)
    } else {
        None
    };

    Ok(Progress {
        read_rows,
        read_bytes,
        total_rows_to_read,
        total_bytes_to_read,
        written_rows: written.map(|w| w.0),
        written_bytes: written.map(|w| w.1),
        elapsed_ns,
    })
}

/// Read profile info from the wire.
pub(crate) async fn read_profile_info<R: ClickHouseRead>(
    reader: &mut R,
    revision: u64,
) -> Result<ProfileInfo> {
    let rows = reader.read_var_uint().await?;
    let blocks = reader.read_var_uint().await?;
    let bytes = reader.read_var_uint().await?;
    let applied_limit = reader.read_u8().await? != 0;
    let rows_before_limit = reader.read_var_uint().await?;
    let calculated_rows_before_limit = reader.read_u8().await? != 0;

    let (applied_aggregation, rows_before_aggregation) =
        if revision >= DBMS_MIN_REVISION_WITH_ROWS_BEFORE_AGGREGATION {
            (reader.read_u8().await? != 0, reader.read_var_uint().await?)
        } else {
            (false, 0)
        };

    Ok(ProfileInfo {
        rows,
        blocks,
        bytes,
        applied_limit,
        rows_before_limit,
        calculated_rows_before_limit,
        applied_aggregation,
        rows_before_aggregation,
    })
}

/// Read table columns packet from the wire.
pub(crate) async fn read_table_columns<R: ClickHouseRead>(reader: &mut R) -> Result<TableColumns> {
    Ok(TableColumns {
        name: reader.read_utf8_string().await?,
        description: reader.read_utf8_string().await?,
    })
}

/// Read and dispatch a single server packet.
///
/// Log and ProfileEvents blocks are consumed and skipped; the next
/// packet is returned instead (loop, not recursion).
pub(crate) async fn read_packet<R: ClickHouseRead>(
    reader: &mut R,
    revision: u64,
    compression: NativeCompressionMethod,
) -> Result<ServerPacket> {
    loop {
        let packet_id = ServerPacketId::from_u64(reader.read_var_uint().await?)?;

        match packet_id {
            ServerPacketId::Data | ServerPacketId::Totals | ServerPacketId::Extremes => {
                return read_data_packet(reader, revision, compression).await;
            }
            ServerPacketId::Exception => {
                let exc = read_exception(reader).await?;
                return Ok(ServerPacket::Exception(
                    error_codes::map_exception_to_error(exc),
                ));
            }
            ServerPacketId::Progress => {
                return read_progress(reader, revision)
                    .await
                    .map(ServerPacket::Progress);
            }
            ServerPacketId::Pong => return Ok(ServerPacket::Pong),
            ServerPacketId::EndOfStream => return Ok(ServerPacket::EndOfStream),
            ServerPacketId::ProfileInfo => {
                return read_profile_info(reader, revision)
                    .await
                    .map(ServerPacket::ProfileInfo);
            }
            ServerPacketId::TableColumns => {
                return read_table_columns(reader)
                    .await
                    .map(ServerPacket::TableColumns);
            }
            ServerPacketId::Log | ServerPacketId::ProfileEvents => {
                // ClickHouse sends Log and ProfileEvents blocks through the
                // UNCOMPRESSED stream even when write_compression=1, so we
                // always read them raw (None), never try to decompress.
                read_data_packet(reader, revision, NativeCompressionMethod::None).await?;
            }
            other => {
                return Err(Error::BadResponse(format!(
                    "native protocol: unhandled server packet: {other:?}"
                )));
            }
        }
    }
}

/// Read a full data block from the stream.
async fn read_data_packet<R: ClickHouseRead>(
    reader: &mut R,
    revision: u64,
    compression: NativeCompressionMethod,
) -> Result<ServerPacket> {
    // Temp table name (empty for normal queries)
    let _table_name = reader.read_string().await?;

    match compression {
        NativeCompressionMethod::None => read_data_block(reader, revision).await,
        _ => {
            let decompressed = decompress_data(reader, compression).await?;
            let mut cursor = std::io::Cursor::new(decompressed);
            read_data_block(&mut cursor, revision).await
        }
    }
}

/// Read a data block from already-decompressed bytes.
async fn read_data_block<R: ClickHouseRead>(reader: &mut R, revision: u64) -> Result<ServerPacket> {
    // Sanity caps to prevent OOM from a malicious server. ClickHouse's
    // default max_block_size is 65536 rows, and tables rarely exceed a
    // few thousand columns. These limits are deliberately generous.
    const MAX_BLOCK_COLUMNS: u64 = 100_000;
    const MAX_BLOCK_ROWS: u64 = 100_000_000;

    let info = BlockInfo::read_async(reader).await?;
    let num_columns = reader.read_var_uint().await?;
    let num_rows = reader.read_var_uint().await?;

    if num_columns > MAX_BLOCK_COLUMNS {
        return Err(Error::BadResponse(format!(
            "block claims {num_columns} columns, limit is {MAX_BLOCK_COLUMNS}"
        )));
    }
    if num_rows > MAX_BLOCK_ROWS {
        return Err(Error::BadResponse(format!(
            "block claims {num_rows} rows, limit is {MAX_BLOCK_ROWS}"
        )));
    }

    let has_custom_serialization = revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION;

    let mut column_headers = Vec::with_capacity(num_columns as usize);
    let mut column_data: Vec<columns::ColumnData> = Vec::with_capacity(num_columns as usize);

    for _ in 0..num_columns {
        let name = reader.read_utf8_string().await?;
        let type_name = reader.read_utf8_string().await?;

        // Newer servers send a custom serialization flag per column.
        // 0 = normal, 1 = sparse (only non-default values are stored with offset groups).
        let is_sparse = if has_custom_serialization {
            reader.read_u8().await? != 0
        } else {
            false
        };

        let col_type = ColumnType::parse(&type_name).ok_or_else(|| {
            Error::BadResponse(format!(
                "native protocol: unsupported column type '{type_name}' for column '{name}'"
            ))
        })?;

        let data = if num_rows == 0 {
            Vec::new()
        } else if is_sparse {
            read_sparse_column(reader, &col_type, num_rows as usize).await?
        } else {
            columns::read_column(reader, &col_type, num_rows).await?
        };

        column_headers.push(ColumnHeader { name, type_name });
        column_data.push(data);
    }

    let row_data = if num_rows == 0 {
        Vec::new()
    } else {
        transpose_to_rowbinary(column_data, num_rows)
    };

    Ok(ServerPacket::Data(DataBlock {
        _info: info,
        _num_columns: num_columns,
        num_rows,
        column_headers,
        row_data,
    }))
}

/// Read a sparsely-serialized column.
///
/// Sparse format: offset groups (varuint, final has END_OF_GRANULE_FLAG) identify
/// positions of non-default values.  Only those values follow in the stream.
/// All other row positions get the type's default (zero/empty/null).
async fn read_sparse_column<R: ClickHouseRead>(
    reader: &mut R,
    col_type: &ColumnType,
    num_rows: usize,
) -> Result<ColumnData> {
    let mut state = SparseDeserializeState::default();
    let non_default_positions = read_sparse_offsets(reader, num_rows, &mut state).await?;

    let non_default_count = non_default_positions.len();
    let non_default_data = if non_default_count > 0 {
        columns::read_column(reader, col_type, non_default_count as u64).await?
    } else {
        Vec::new()
    };

    let default = sparse_default_bytes(col_type);
    let mut result = vec![default; num_rows];
    for (i, pos) in non_default_positions.into_iter().enumerate() {
        if pos < num_rows {
            result[pos] = non_default_data[i].clone();
        }
    }
    Ok(result)
}

/// Return the RowBinary-encoded default value for a type in sparse context.
///
/// The sparse default is the column's "zero" value: 0 for numerics, empty for
/// strings, NULL for Nullable.
fn sparse_default_bytes(col_type: &ColumnType) -> Vec<u8> {
    if let Some(size) = col_type.fixed_size() {
        return vec![0u8; size];
    }
    match col_type {
        ColumnType::String | ColumnType::Json => vec![0u8], // varuint(0) = empty string
        ColumnType::FixedString(n) => {
            let mut v = Vec::with_capacity(*n + 9);
            write_var_uint(*n as u64, &mut v);
            v.extend(std::iter::repeat(0u8).take(*n));
            v
        }
        ColumnType::Nullable(_) => vec![0x01], // NULL
        // Complex types (Array, Tuple, Map, etc.) are unlikely to be sparse, but
        // return an empty vec as a safe fallback.
        _ => vec![],
    }
}
