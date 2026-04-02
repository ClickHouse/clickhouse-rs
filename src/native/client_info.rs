//! Client information sent during query execution.
//!
//! Version-gated fields are written conditionally based on the negotiated
//! protocol revision with the server.

use tokio::io::AsyncWriteExt;

use crate::error::Result;
use crate::native::io::ClickHouseWrite;
use crate::native::protocol::{
    DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH,
    DBMS_MIN_PROTOCOL_VERSION_WITH_PARALLEL_REPLICAS,
    DBMS_MIN_PROTOCOL_VERSION_WITH_QUERY_START_TIME, DBMS_MIN_REVISION_WITH_JWT_IN_INTERSERVER,
    DBMS_MIN_REVISION_WITH_OPENTELEMETRY, DBMS_MIN_REVISION_WITH_QUERY_AND_LINE_NUMBERS,
    DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO, DBMS_MIN_REVISION_WITH_VERSION_PATCH,
    DBMS_TCP_PROTOCOL_VERSION,
};

// Client version derived from this crate's Cargo.toml
const CLIENT_VERSION_MAJOR: u64 = 0;
const CLIENT_VERSION_MINOR: u64 = 14;
const CLIENT_VERSION_PATCH: u64 = 2;

#[repr(u8)]
#[derive(PartialEq, Clone, Copy, Debug)]
#[allow(unused)]
pub(crate) enum QueryKind {
    NoQuery,
    InitialQuery,
    SecondaryQuery,
}

#[derive(Debug)]
pub(crate) struct ClientInfo<'a> {
    pub(crate) kind: QueryKind,
    pub(crate) initial_user: &'a str,
    pub(crate) initial_query_id: &'a str,
    pub(crate) initial_address: &'a str,
    pub(crate) os_user: &'a str,
    pub(crate) client_hostname: &'a str,
    pub(crate) client_name: &'a str,
    pub(crate) client_version_major: u64,
    pub(crate) client_version_minor: u64,
    pub(crate) client_version_patch: u64,
    pub(crate) client_tcp_protocol_version: u64,
    pub(crate) query_start_time: u64,
    pub(crate) quota_key: &'a str,
    pub(crate) distributed_depth: u64,
}

impl Default for ClientInfo<'_> {
    fn default() -> Self {
        #[allow(clippy::cast_possible_truncation)]
        let query_start_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or(std::time::Duration::from_secs(0))
            .as_micros() as u64;

        ClientInfo {
            kind: QueryKind::InitialQuery,
            initial_user: "",
            initial_query_id: "",
            initial_address: "0.0.0.0:0",
            os_user: "",
            client_hostname: "localhost",
            client_name: "clickhouse-rs",
            client_version_major: CLIENT_VERSION_MAJOR,
            client_version_minor: CLIENT_VERSION_MINOR,
            client_version_patch: CLIENT_VERSION_PATCH,
            client_tcp_protocol_version: DBMS_TCP_PROTOCOL_VERSION,
            query_start_time,
            quota_key: "",
            distributed_depth: 1,
        }
    }
}

impl ClientInfo<'_> {
    pub(crate) async fn write<W: ClickHouseWrite>(
        &self,
        writer: &mut W,
        revision: u64,
    ) -> Result<()> {
        writer.write_u8(self.kind as u8).await?;
        if self.kind == QueryKind::NoQuery {
            return Ok(());
        }

        writer.write_string(self.initial_user).await?;
        writer.write_string(self.initial_query_id).await?;
        writer.write_string(self.initial_address).await?;

        if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_QUERY_START_TIME {
            writer.write_u64_le(self.query_start_time).await?;
        }

        // interface = TCP = 1
        writer.write_u8(1).await?;

        writer.write_string(self.os_user).await?;
        writer.write_string(self.client_hostname).await?;
        writer.write_string(self.client_name).await?;

        writer
            .write_var_uint(self.client_version_major)
            .await?;
        writer
            .write_var_uint(self.client_version_minor)
            .await?;
        writer
            .write_var_uint(self.client_tcp_protocol_version)
            .await?;

        if revision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
            writer.write_string(self.quota_key).await?;
        }
        if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH {
            writer.write_var_uint(self.distributed_depth).await?;
        }
        if revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
            writer.write_var_uint(self.client_version_patch).await?;
        }
        if revision >= DBMS_MIN_REVISION_WITH_OPENTELEMETRY {
            // No OpenTelemetry support in MVP
            writer.write_u8(0).await?;
        }
        if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_PARALLEL_REPLICAS {
            writer.write_var_uint(0).await?; // collaborate_with_initiator
            writer.write_var_uint(0).await?; // count_participating_replicas
            writer.write_var_uint(0).await?; // number_of_current_replica
        }
        if revision >= DBMS_MIN_REVISION_WITH_QUERY_AND_LINE_NUMBERS {
            writer.write_var_uint(0).await?; // script_query_number
            writer.write_var_uint(0).await?; // script_line_number
        }
        if revision >= DBMS_MIN_REVISION_WITH_JWT_IN_INTERSERVER {
            writer.write_u8(0).await?;
        }

        Ok(())
    }
}
