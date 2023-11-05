use hyper::client::ResponseFuture;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;

use crate::error::{Error, Result};

#[derive(Debug, Clone, Serialize)]
pub struct Summary {
    pub read_rows: u64,
    pub read_bytes: u64,
    pub written_rows: u64,
    pub written_bytes: u64,
    pub total_rows_to_read: u64,
    pub result_rows: u64,
    pub result_bytes: u64,
    pub elapsed_ns: u64,
}

impl Summary {
    pub(crate) async fn new(response: ResponseFuture) -> Result<Self> {
        let response = response.await?;
        let headers = response.headers();
        println!("status: {:?}", headers);

        let summary = headers
            .get("x-clickhouse-summary")
            .ok_or(Error::SummaryHeaderNotFound)?
            .to_str()
            .map_err(|_| {
                Error::Custom("could not parse x-clickhouse-summary header".to_string())
            })?;

        serde_json::from_str(summary).map_err(|e| Error::Custom(e.to_string()))
    }
}

impl<'de> Deserialize<'de> for Summary {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize, Debug)]
        #[serde(untagged)]
        enum StrOrNum<'a> {
            Str(&'a str),
            Num(u64),
        }

        impl StrOrNum<'_> {
            pub(crate) fn get_num(&self) -> Option<u64> {
                match self {
                    StrOrNum::Str(s) => s.parse::<u64>().ok(),
                    StrOrNum::Num(n) => Some(*n),
                }
            }
        }

        let raw = HashMap::<String, StrOrNum<'de>>::deserialize(deserializer)?;

        Ok(Self {
            read_rows: raw.get("read_rows").and_then(|v| v.get_num()).unwrap_or(0),
            read_bytes: raw.get("read_bytes").and_then(|v| v.get_num()).unwrap_or(0),
            written_rows: raw
                .get("written_rows")
                .and_then(|v| v.get_num())
                .unwrap_or(0),
            written_bytes: raw
                .get("written_bytes")
                .and_then(|v| v.get_num())
                .unwrap_or(0),
            total_rows_to_read: raw
                .get("total_rows_to_read")
                .and_then(|v| v.get_num())
                .unwrap_or(0),
            result_rows: raw
                .get("result_rows")
                .and_then(|v| v.get_num())
                .unwrap_or(0),
            result_bytes: raw
                .get("result_bytes")
                .and_then(|v| v.get_num())
                .unwrap_or(0),
            elapsed_ns: raw.get("elapsed_ns").and_then(|v| v.get_num()).unwrap_or(0),
        })
    }
}
