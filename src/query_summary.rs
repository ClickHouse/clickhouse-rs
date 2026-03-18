use serde::{Deserialize, Deserializer};

/// Typed representation of the `X-ClickHouse-Summary` HTTP response header.
///
/// ClickHouse sends all values as JSON strings (e.g. `"1000"` instead of
/// `1000`), so custom deserialization is used to parse them into `u64`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct QuerySummary {
    #[serde(deserialize_with = "deserialize_string_to_u64")]
    pub read_rows: u64,
    #[serde(deserialize_with = "deserialize_string_to_u64")]
    pub read_bytes: u64,
    #[serde(deserialize_with = "deserialize_string_to_u64")]
    pub written_rows: u64,
    #[serde(deserialize_with = "deserialize_string_to_u64")]
    pub written_bytes: u64,
    #[serde(deserialize_with = "deserialize_string_to_u64")]
    pub total_rows_to_read: u64,
    #[serde(deserialize_with = "deserialize_string_to_u64")]
    pub result_rows: u64,
    #[serde(deserialize_with = "deserialize_string_to_u64")]
    pub result_bytes: u64,
    #[serde(deserialize_with = "deserialize_string_to_u64")]
    pub elapsed_ns: u64,
    #[serde(deserialize_with = "deserialize_string_to_u64")]
    pub memory_usage: u64,
    /// Only present when the query uses a `LIMIT` clause.
    #[serde(default, deserialize_with = "deserialize_optional_string_to_u64")]
    pub rows_before_limit_at_least: Option<u64>,
}

fn deserialize_string_to_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    s.parse().map_err(serde::de::Error::custom)
}

fn deserialize_optional_string_to_u64<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    s.parse().map(Some).map_err(serde::de::Error::custom)
}
