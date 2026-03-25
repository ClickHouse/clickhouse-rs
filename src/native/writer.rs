//! Packet writer for ClickHouse native protocol.
//!
//! Sends client packets: hello, query, data, addendum, ping.

use tokio::io::AsyncWriteExt;

use crate::error::Result;
use crate::native::block_info::BlockInfo;
use crate::native::client_info::ClientInfo;
use crate::native::compression::compress_data;
use crate::native::io::ClickHouseWrite;
use crate::native::protocol::{
    ClientPacketId, NativeCompressionMethod, QueryProcessingStage, ServerHello,
    DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS,
    DBMS_MIN_PROTOCOL_VERSION_WITH_INTERSERVER_EXTERNALLY_GRANTED_ROLES,
    DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS, DBMS_MIN_PROTOCOL_VERSION_WITH_QUOTA_KEY,
    DBMS_MIN_REVISION_WITH_CLIENT_INFO, DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET,
    DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL,
    DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION, DBMS_TCP_PROTOCOL_VERSION,
};

/// Send client hello packet.
pub(crate) async fn send_hello<W: ClickHouseWrite>(
    writer: &mut W,
    database: &str,
    username: &str,
    password: &str,
) -> Result<()> {
    writer
        .write_var_uint(ClientPacketId::Hello as u64)
        .await?;
    writer
        .write_string(format!(
            "clickhouse-rs native {}",
            env!("CARGO_PKG_VERSION")
        ))
        .await?;
    // Client version (major, minor, revision)
    writer.write_var_uint(0).await?; // major
    writer.write_var_uint(14).await?; // minor
    writer
        .write_var_uint(DBMS_TCP_PROTOCOL_VERSION)
        .await?;
    writer.write_string(database).await?;
    writer.write_string(username).await?;
    writer.write_string(password).await?;
    writer.flush().await?;
    Ok(())
}

/// Send a query for execution.
pub(crate) async fn send_query<W: ClickHouseWrite>(
    writer: &mut W,
    query_id: &str,
    query: &str,
    settings: &[(String, String)],
    revision: u64,
    compression: NativeCompressionMethod,
) -> Result<()> {
    writer
        .write_var_uint(ClientPacketId::Query as u64)
        .await?;
    writer.write_string(query_id).await?;

    if revision >= DBMS_MIN_REVISION_WITH_CLIENT_INFO {
        let info = ClientInfo::default();
        info.write(writer, revision).await?;
    }

    // Settings: (name, flags_varuint, value) per entry, terminated by empty name.
    // Only non-param_ settings go here. param_ entries are sent as Parameters
    // after the query body (revision >= 54459).
    const FLAG_IMPORTANT: u8 = 0x01;

    for (name, value) in settings {
        if name.starts_with("param_") {
            continue; // sent later as Parameters
        }
        writer.write_string(name).await?;
        writer.write_u8(FLAG_IMPORTANT).await?;
        writer.write_string(value).await?;
    }
    writer.write_string("").await?; // end of settings

    if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_INTERSERVER_EXTERNALLY_GRANTED_ROLES {
        writer.write_string("").await?;
    }

    if revision >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET {
        writer.write_string("").await?;
    }

    writer
        .write_var_uint(QueryProcessingStage::Complete as u64)
        .await?;

    // Compression flag
    let use_compression = !matches!(compression, NativeCompressionMethod::None);
    writer.write_u8(u8::from(use_compression)).await?;

    writer.write_string(query).await?;

    // Parameters (revision >= 54459): sent AFTER the query body as a separate
    // block. Each parameter is (key, FLAG_CUSTOM, field_dump_value). The key
    // includes the "param_" prefix. Field dump wraps string values in single
    // quotes with escaping.
    if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS {
        const FLAG_CUSTOM: u8 = 0x02;
        for (name, value) in settings {
            if !name.starts_with("param_") {
                continue; // already sent as settings above
            }
            writer.write_string(name).await?;
            writer.write_u8(FLAG_CUSTOM).await?;
            let escaped = value.replace('\'', "\\'");
            writer.write_string(&format!("'{escaped}'")).await?;
        }
        writer.write_string("").await?; // end of parameters
    }

    writer.flush().await?;
    Ok(())
}

/// Send an empty data block (signals end of client data).
///
/// When `compression` is not `None`, the block body (info + counts) is
/// wrapped in a single ClickHouse compressed chunk.
pub(crate) async fn send_empty_block<W: ClickHouseWrite>(
    writer: &mut W,
    compression: NativeCompressionMethod,
) -> Result<()> {
    writer
        .write_var_uint(ClientPacketId::Data as u64)
        .await?;
    writer.write_string("").await?; // table name (always uncompressed)

    if matches!(compression, NativeCompressionMethod::None) {
        let info = BlockInfo::default();
        info.write_async(writer).await?;
        writer.write_var_uint(0).await?; // 0 columns
        writer.write_var_uint(0).await?; // 0 rows
    } else {
        let mut body: Vec<u8> = Vec::new();
        BlockInfo::default().write_async(&mut body).await?;
        body.write_var_uint(0).await?; // 0 columns
        body.write_var_uint(0).await?; // 0 rows
        compress_data(writer, &body, compression).await?;
    }

    writer.flush().await?;
    Ok(())
}

/// Send addendum after hello exchange.
pub(crate) async fn send_addendum<W: ClickHouseWrite>(
    writer: &mut W,
    server_hello: &ServerHello,
) -> Result<()> {
    let revision = server_hello.revision_version;

    if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_QUOTA_KEY {
        writer.write_string("").await?;
    }

    if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS {
        writer
            .write_string(server_hello.chunked_send.to_string())
            .await?;
        writer
            .write_string(server_hello.chunked_recv.to_string())
            .await?;
    }

    if revision >= DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL {
        writer
            .write_var_uint(DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION)
            .await?;
    }

    writer.flush().await?;
    Ok(())
}

/// Send a data block containing encoded column bytes.
///
/// `column_bytes` must be pre-encoded by [`crate::native::encode::encode_columns`]:
/// for each column in order, it contains `string(name) + string(type) + column_data`.
///
/// When `compression` is not `None`, the block body (info + counts + column_bytes)
/// is wrapped in a single ClickHouse compressed chunk.
pub(crate) async fn send_data_block<W: ClickHouseWrite>(
    writer: &mut W,
    num_columns: usize,
    num_rows: usize,
    column_bytes: &[u8],
    compression: NativeCompressionMethod,
) -> Result<()> {
    writer
        .write_var_uint(ClientPacketId::Data as u64)
        .await?;
    writer.write_string("").await?; // temp table name (always uncompressed)

    if matches!(compression, NativeCompressionMethod::None) {
        let info = BlockInfo::default();
        info.write_async(writer).await?;
        writer.write_var_uint(num_columns as u64).await?;
        writer.write_var_uint(num_rows as u64).await?;
        writer.write_all(column_bytes).await?;
    } else {
        let mut body: Vec<u8> = Vec::with_capacity(32 + column_bytes.len());
        BlockInfo::default().write_async(&mut body).await?;
        body.write_var_uint(num_columns as u64).await?;
        body.write_var_uint(num_rows as u64).await?;
        body.extend_from_slice(column_bytes);
        compress_data(writer, &body, compression).await?;
    }

    writer.flush().await?;
    Ok(())
}

/// Send ping.
pub(crate) async fn send_ping<W: ClickHouseWrite>(writer: &mut W) -> Result<()> {
    writer
        .write_var_uint(ClientPacketId::Ping as u64)
        .await?;
    writer.flush().await?;
    Ok(())
}
