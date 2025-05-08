use clickhouse_rowbinary::header::parse_names_and_types_header;
use clickhouse_rowbinary::types::{Column, DataType};

#[tokio::test]
async fn test_header_parsing() {
    let client = prepare_database!();
    client
        .query(
            "
                CREATE OR REPLACE TABLE visits
                (
                    CounterID UInt32,
                    StartDate Date,
                    Sign Int8,
                    IsNew UInt8,
                    VisitID UInt64,
                    UserID UInt64,
                    Goals Nested
                    (
                        ID UInt32,
                        Serial UInt32,
                        EventTime DateTime,
                        Price Int64,
                        OrderID String,
                        CurrencyID UInt32
                    )
                ) ENGINE = MergeTree ORDER BY ()
            ",
        )
        .execute()
        .await
        .unwrap();

    let mut cursor = client
        .query("SELECT * FROM visits LIMIT 0")
        .fetch_bytes("RowBinaryWithNamesAndTypes")
        .unwrap();

    let mut data = cursor.collect().await.unwrap();
    let result = parse_names_and_types_header(&mut data).unwrap();
    assert_eq!(
        result,
        vec![
            Column {
                name: "CounterID".to_string(),
                data_type: DataType::UInt32
            },
            Column {
                name: "StartDate".to_string(),
                data_type: DataType::Date
            },
            Column {
                name: "Sign".to_string(),
                data_type: DataType::Int8
            },
            Column {
                name: "IsNew".to_string(),
                data_type: DataType::UInt8
            },
            Column {
                name: "VisitID".to_string(),
                data_type: DataType::UInt64
            },
            Column {
                name: "UserID".to_string(),
                data_type: DataType::UInt64
            },
            Column {
                name: "Goals.ID".to_string(),
                data_type: DataType::Array(Box::new(DataType::UInt32))
            },
            Column {
                name: "Goals.Serial".to_string(),
                data_type: DataType::Array(Box::new(DataType::UInt32))
            },
            Column {
                name: "Goals.EventTime".to_string(),
                data_type: DataType::Array(Box::new(DataType::DateTime(None)))
            },
            Column {
                name: "Goals.Price".to_string(),
                data_type: DataType::Array(Box::new(DataType::Int64))
            },
            Column {
                name: "Goals.OrderID".to_string(),
                data_type: DataType::Array(Box::new(DataType::String))
            },
            Column {
                name: "Goals.CurrencyID".to_string(),
                data_type: DataType::Array(Box::new(DataType::UInt32))
            }
        ]
    );
}
