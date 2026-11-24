//! Constants and encoding helpers of the native TCP protocol.
//!
//! Reference: `src/Core/Protocol.h` and `src/Core/ProtocolDefines.h` in the ClickHouse repository.

use crate::native::varuint;
use bytes::{BufMut, BytesMut};

/// The protocol revision of this client.
///
/// The connection uses the smaller of this revision and the revision of the server.
/// This revision is below `DBMS_MIN_REVISION_WITH_SPARSE_SERIALIZATION`,
/// so new servers send all columns with the default serialization.
pub(crate) const CLIENT_REVISION: u64 = 54460;

pub(crate) const REVISION_WITH_CLIENT_INFO: u64 = 54032;
pub(crate) const REVISION_WITH_SERVER_TIMEZONE: u64 = 54058;
pub(crate) const REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO: u64 = 54060;
pub(crate) const REVISION_WITH_SERVER_DISPLAY_NAME: u64 = 54372;
pub(crate) const REVISION_WITH_VERSION_PATCH: u64 = 54401;
pub(crate) const REVISION_WITH_CLIENT_WRITE_INFO: u64 = 54420;
pub(crate) const REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS: u64 = 54429;
pub(crate) const REVISION_WITH_INTERSERVER_SECRET: u64 = 54441;
pub(crate) const REVISION_WITH_OPENTELEMETRY: u64 = 54442;
pub(crate) const REVISION_WITH_DISTRIBUTED_DEPTH: u64 = 54448;
pub(crate) const REVISION_WITH_INITIAL_QUERY_START_TIME: u64 = 54449;
pub(crate) const REVISION_WITH_PARALLEL_REPLICAS: u64 = 54453;
pub(crate) const REVISION_WITH_ADDENDUM: u64 = 54458;
pub(crate) const REVISION_WITH_QUOTA_KEY: u64 = 54458;
pub(crate) const REVISION_WITH_PARAMETERS: u64 = 54459;
pub(crate) const REVISION_WITH_SERVER_QUERY_TIME_IN_PROGRESS: u64 = 54460;

/// Packets that the client sends.
pub(crate) mod client {
    pub(crate) const HELLO: u64 = 0;
    pub(crate) const QUERY: u64 = 1;
    pub(crate) const DATA: u64 = 2;
    pub(crate) const CANCEL: u64 = 3;
}

/// Packets that the server sends.
pub(crate) mod server {
    pub(crate) const HELLO: u64 = 0;
    pub(crate) const DATA: u64 = 1;
    pub(crate) const EXCEPTION: u64 = 2;
    pub(crate) const PROGRESS: u64 = 3;
    pub(crate) const PONG: u64 = 4;
    pub(crate) const END_OF_STREAM: u64 = 5;
    pub(crate) const PROFILE_INFO: u64 = 6;
    pub(crate) const TOTALS: u64 = 7;
    pub(crate) const EXTREMES: u64 = 8;
    pub(crate) const LOG: u64 = 10;
    pub(crate) const TABLE_COLUMNS: u64 = 11;
    pub(crate) const PART_UUIDS: u64 = 12;
    pub(crate) const PROFILE_EVENTS: u64 = 14;
    pub(crate) const TIMEZONE_UPDATE: u64 = 17;
}

/// `QueryProcessingStage::Complete`
pub(crate) const STAGE_COMPLETE: u64 = 2;

/// `ClientInfo::QueryKind::INITIAL_QUERY`
const QUERY_KIND_INITIAL: u8 = 1;
/// `ClientInfo::Interface::TCP`
const INTERFACE_TCP: u8 = 1;

/// `BaseSettingsHelpers::Flags::CUSTOM`
///
/// Settings and parameters are sent as custom settings with quoted string values.
/// The server converts the value to the type of a built-in setting, stores the value of
/// a user-defined setting (`custom_*`), and rejects unknown settings, like the HTTP interface.
const FLAG_CUSTOM: usize = 0x02;

pub(crate) fn write_varuint(out: &mut BytesMut, value: usize) {
    varuint::write(out, value);
}

pub(crate) fn write_str(out: &mut BytesMut, value: impl AsRef<[u8]>) {
    let value = value.as_ref();
    varuint::write(&mut *out, value.len());
    out.put_slice(value);
}

pub(crate) fn revision_varuint(revision: u64) -> usize {
    usize::try_from(revision).unwrap_or(usize::MAX)
}

/// Fields of `ClientInfo` that this client sends.
pub(crate) struct ClientInfo<'a> {
    pub(crate) client_name: &'a str,
    pub(crate) quota_key: &'a str,
    #[cfg(feature = "opentelemetry")]
    pub(crate) trace_context: Option<TraceContext>,
}

/// The OpenTelemetry trace context of a query.
#[cfg(feature = "opentelemetry")]
#[derive(Clone)]
pub(crate) struct TraceContext {
    pub(crate) trace_id: u128,
    pub(crate) span_id: u64,
    pub(crate) trace_state: String,
    pub(crate) trace_flags: u8,
}

impl ClientInfo<'_> {
    /// `ClientInfo::write()`
    pub(crate) fn write(&self, revision: u64, out: &mut BytesMut) {
        out.put_u8(QUERY_KIND_INITIAL);
        // initial_user, initial_query_id, initial_address
        write_str(out, "");
        write_str(out, "");
        write_str(out, "0.0.0.0:0");

        if revision >= REVISION_WITH_INITIAL_QUERY_START_TIME {
            out.put_i64_le(0);
        }

        out.put_u8(INTERFACE_TCP);
        // os_user, client_hostname
        write_str(out, "");
        write_str(out, "");
        write_str(out, self.client_name);
        write_varuint(out, crate_version().0);
        write_varuint(out, crate_version().1);
        write_varuint(out, revision_varuint(CLIENT_REVISION));

        if revision >= REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
            write_str(out, self.quota_key);
        }

        if revision >= REVISION_WITH_DISTRIBUTED_DEPTH {
            write_varuint(out, 0);
        }

        if revision >= REVISION_WITH_VERSION_PATCH {
            write_varuint(out, crate_version().2);
        }

        if revision >= REVISION_WITH_OPENTELEMETRY {
            #[cfg(feature = "opentelemetry")]
            if let Some(trace) = &self.trace_context {
                out.put_u8(1);
                // `UUID` is written as two little-endian halves, high half first.
                // The casts take the halves.
                out.put_u64_le((trace.trace_id >> 64) as u64);
                out.put_u64_le(trace.trace_id as u64);
                out.put_u64_le(trace.span_id);
                write_str(out, &trace.trace_state);
                out.put_u8(trace.trace_flags);
            } else {
                out.put_u8(0);
            }

            #[cfg(not(feature = "opentelemetry"))]
            out.put_u8(0);
        }

        if revision >= REVISION_WITH_PARALLEL_REPLICAS {
            // collaborate_with_initiator, count_participating_replicas, number_of_current_replica
            write_varuint(out, 0);
            write_varuint(out, 0);
            write_varuint(out, 0);
        }
    }
}

/// The version of this crate: `(major, minor, patch)`.
pub(crate) fn crate_version() -> (usize, usize, usize) {
    let parse = |part: &str| part.parse().unwrap_or(0);
    let mut parts = env!("CARGO_PKG_VERSION").split(['.', '-']);
    (
        parts.next().map_or(0, parse),
        parts.next().map_or(0, parse),
        parts.next().map_or(0, parse),
    )
}

/// Write `(name, value)` pairs as custom settings, followed by the end marker.
pub(crate) fn write_settings<'a>(
    out: &mut BytesMut,
    settings: impl IntoIterator<Item = (&'a str, &'a str)>,
) {
    for (name, value) in settings {
        write_str(out, name);
        write_varuint(out, FLAG_CUSTOM);
        write_str(out, quote(value));
    }

    // An empty name is the end of the list
    write_str(out, "");
}

/// Quote `value` as a ClickHouse string literal.
pub(crate) fn quote(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    // Writing to a `String` cannot fail.
    let _ = crate::sql::escape::string(value, &mut quoted);
    quoted
}

/// The trace context of the current OpenTelemetry context, as the HTTP transport injects it
/// into the `traceparent` and `tracestate` headers.
#[cfg(feature = "opentelemetry")]
pub(crate) fn current_trace_context() -> Option<TraceContext> {
    let mut carrier = std::collections::HashMap::<String, String>::new();
    opentelemetry::global::get_text_map_propagator(|propagator| propagator.inject(&mut carrier));

    // `version-trace_id-parent_id-trace_flags`
    let traceparent = carrier.get("traceparent")?;
    let mut parts = traceparent.split('-');
    let _version = parts.next()?;
    let trace_id = u128::from_str_radix(parts.next()?, 16).ok()?;
    let span_id = u64::from_str_radix(parts.next()?, 16).ok()?;
    let trace_flags = u8::from_str_radix(parts.next()?, 16).ok()?;

    Some(TraceContext {
        trace_id,
        span_id,
        trace_state: carrier.get("tracestate").cloned().unwrap_or_default(),
        trace_flags,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_are_custom_and_quoted() {
        let mut out = BytesMut::new();
        write_settings(&mut out, [("max_threads", "1"), ("custom_x", "it's")]);

        let mut expected = Vec::new();
        expected.extend_from_slice(b"\x0bmax_threads\x02\x03'1'");
        expected.extend_from_slice(b"\x08custom_x\x02\x07'it\\'s'");
        expected.push(0);
        assert_eq!(&out[..], &expected[..]);
    }

    #[test]
    fn client_info_depends_on_revision() {
        let info = ClientInfo {
            client_name: "c",
            quota_key: "q",
            #[cfg(feature = "opentelemetry")]
            trace_context: None,
        };

        let mut old = BytesMut::new();
        info.write(REVISION_WITH_CLIENT_INFO, &mut old);
        let mut new = BytesMut::new();
        info.write(CLIENT_REVISION, &mut new);

        // The old revision has no quota key, distributed depth, patch version,
        // OpenTelemetry flag, parallel replicas fields and query start time.
        assert!(old.len() < new.len());
        assert_eq!(old[0], QUERY_KIND_INITIAL);
        assert!(!old.windows(2).any(|w| w == b"\x01q"));
        assert!(new.windows(2).any(|w| w == b"\x01q"));
        // query start time (8 bytes) + quota key (2) + depth (1) + patch (1+)
        // + OpenTelemetry flag (1) + parallel replicas (3)
        assert!(new.len() - old.len() >= 16);
    }
}
