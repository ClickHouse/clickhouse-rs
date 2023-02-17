use std::time::UNIX_EPOCH;

use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use clickhouse::{error::Result, Client, Row};

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::default().with_url("http://localhost:8123");

    client
        .query("DROP TABLE IF EXISTS event_log")
        .execute()
        .await?;

    client
        .query(
            "
            CREATE TABLE event_log (
                timestamp       DateTime64(9),
                message         String,
                level           Enum8(
                                    'Debug' = 1,
                                    'Info' = 2,
                                    'Warn' = 3,
                                    'Error' = 4
                                )
            )
            ENGINE = MergeTree
            ORDER BY timestamp",
        )
        .execute()
        .await?;

    #[derive(Debug, Serialize, Deserialize, Row)]
    struct Event {
        timestamp: u64,
        message: String,
        level: Level,
    }

    // How to define enums that map to `Enum8`/`Enum16`.
    #[derive(Debug, Serialize_repr, Deserialize_repr)]
    #[repr(u8)]
    enum Level {
        Debug = 1,
        Info = 2,
        Warn = 3,
        Error = 4,
    }

    let mut insert = client.insert("event_log")?;
    insert
        .write(&Event {
            timestamp: now(),
            message: "one".into(),
            level: Level::Info,
        })
        .await?;
    insert.end().await?;

    let events = client
        .query("SELECT ?fields FROM event_log")
        .fetch_all::<Event>()
        .await?;
    println!("{events:?}");

    Ok(())
}

fn now() -> u64 {
    UNIX_EPOCH
        .elapsed()
        .expect("invalid system time")
        .as_nanos() as u64
}
