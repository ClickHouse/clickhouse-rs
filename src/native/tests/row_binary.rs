//! Round trips between `RowBinary`, columns and the native wire format.

use crate::native::from_row_binary::RowBinaryColumns;
use crate::native::reader::{BlockReader, ChunkSource, VARIANT_NULL_DISCRIMINATOR, WireFormat};
use crate::native::string::MaybeUtf8;
use crate::native::{Block, Column, Layout, LayoutKind, block_encoder, to_row_binary, varuint};
use bytes::{Bytes, BytesMut};
use clickhouse_types::DataTypeNode;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::collections::VecDeque;

/// Types to test, with names as the server writes them.
const TYPES: &[&str] = &[
    "UInt8",
    "Int64",
    "UInt256",
    "Float64",
    "Bool",
    "Decimal(18, 4)",
    "String",
    "FixedString(3)",
    "UUID",
    "Date32",
    "DateTime('UTC')",
    "DateTime64(6, 'Asia/Tokyo')",
    "IPv6",
    "Enum8('a' = 1, 'b' = 2)",
    "Nullable(String)",
    "Nullable(Nothing)",
    "Array(Nullable(UInt32))",
    "Array(Array(String))",
    "Map(String, Array(UInt8))",
    "Tuple(UInt8, Nullable(String), Array(Int16))",
    "Tuple(a UInt8, `b c` String)",
    "LowCardinality(String)",
    "LowCardinality(Nullable(String))",
    "Array(LowCardinality(String))",
    "Map(LowCardinality(String), UInt64)",
    "Variant(Array(UInt64), String, UInt64)",
    "Array(Variant(String, UInt8))",
    "Point",
    "Polygon",
    "SimpleAggregateFunction(sum, UInt64)",
];

fn header(types: &[&str]) -> Block {
    let names: Vec<String> = (0..types.len()).map(|index| format!("c{index}")).collect();
    let columns: Vec<(&str, &str)> = names
        .iter()
        .map(String::as_str)
        .zip(types.iter().copied())
        .collect();
    Block::header(&columns)
}

/// Write a random value of `data_type` in the `RowBinary` format.
fn random_value(data_type: &DataTypeNode, rng: &mut SmallRng, out: &mut Vec<u8>) {
    let wire = crate::native::reader::wire_type(data_type);

    match &*wire {
        DataTypeNode::Bool => out.push(rng.random_range(0..2)),
        DataTypeNode::String => {
            let len = rng.random_range(0..20);
            varuint::write(&mut *out, len);
            out.extend((0..len).map(|_| rng.random::<u8>()));
        }
        DataTypeNode::Nullable(inner) => {
            // `Nothing` has no values.
            if **inner == DataTypeNode::Nothing || rng.random_bool(0.3) {
                out.push(1);
            } else {
                out.push(0);
                random_value(inner, rng, out);
            }
        }
        DataTypeNode::LowCardinality(inner) => {
            // Repeat values to test the dictionary.
            let mut pool = SmallRng::seed_from_u64(rng.random_range(0..4));
            random_value(inner, &mut pool, out);
        }
        DataTypeNode::Array(inner) => {
            let len = rng.random_range(0..4);
            varuint::write(&mut *out, len);
            for _ in 0..len {
                random_value(inner, rng, out);
            }
        }
        DataTypeNode::Map([key, value]) => {
            let len = rng.random_range(0..3);
            varuint::write(&mut *out, len);
            for _ in 0..len {
                random_value(key, rng, out);
                random_value(value, rng, out);
            }
        }
        DataTypeNode::Tuple(types) => {
            for ty in types {
                random_value(ty, rng, out);
            }
        }
        DataTypeNode::Variant(types) => {
            if rng.random_bool(0.2) {
                out.push(VARIANT_NULL_DISCRIMINATOR);
            } else {
                let discriminator = rng.random_range(0..types.len());
                out.push(u8::try_from(discriminator).unwrap());
                random_value(&types[discriminator], rng, out);
            }
        }
        other => {
            let width = crate::native::utils::type_fixed_width(other)
                .unwrap_or_else(|| panic!("no generator for {other}"));
            out.extend((0..width).map(|_| rng.random::<u8>()));
        }
    }
}

struct Chunks(VecDeque<Bytes>);

impl ChunkSource for Chunks {
    async fn next_chunk(&mut self) -> Option<Result<Bytes, crate::error::Error>> {
        self.0.pop_front().map(Ok)
    }
}

/// Split `data` into chunks of random sizes, including empty chunks.
fn split(data: &[u8], rng: &mut SmallRng) -> VecDeque<Bytes> {
    let mut chunks = VecDeque::new();
    let mut rest = data;

    while !rest.is_empty() {
        let len = rng.random_range(0..=rest.len().min(64));
        let (chunk, tail) = rest.split_at(len);
        chunks.push_back(Bytes::copy_from_slice(chunk));
        rest = tail;
    }

    chunks
}

/// Convert `rows` to blocks, reading them in chunks of random sizes.
fn to_blocks(header: &Block, rows: &[u8], rng: &mut SmallRng) -> Vec<Block> {
    let mut columns = RowBinaryColumns::new(header).unwrap();
    let mut pending = Vec::new();
    let mut blocks = Vec::new();

    for chunk in split(rows, rng) {
        pending.extend_from_slice(&chunk);
        let mut input = &pending[..];

        while columns.read_row(&mut input).unwrap() {
            if rng.random_bool(0.05) {
                blocks.push(columns.take_block().unwrap());
            }
        }

        pending.drain(..pending.len() - input.len());
    }

    assert!(pending.is_empty(), "{} bytes left", pending.len());
    blocks.push(columns.take_block().unwrap());
    blocks
}

async fn decode(data: &[u8], format: WireFormat, rng: &mut SmallRng) -> Vec<Block> {
    let mut reader = BlockReader::with_format(Chunks(split(data, rng)), format);
    let mut blocks = Vec::new();

    if format.revision == 0 {
        while let Some(block) = reader.read_block().await.unwrap() {
            blocks.push(block);
        }
    } else {
        loop {
            let (Chunks(chunks), leftover) = reader.into_parts();
            if chunks.is_empty() && leftover.is_empty() {
                break;
            }

            let mut all = VecDeque::from([leftover]);
            all.extend(chunks);
            reader = BlockReader::with_format(Chunks(all), format);
            blocks.push(reader.read_protocol_block().await.unwrap());
        }
    }

    blocks
}

fn to_row_binary(blocks: &[Block]) -> Vec<u8> {
    let mut out = BytesMut::new();
    for block in blocks {
        to_row_binary::write_rows(block, &mut out).unwrap();
    }
    out.to_vec()
}

#[tokio::test]
async fn row_binary_round_trip() {
    let formats = [
        WireFormat::NATIVE,
        WireFormat {
            revision: 54460,
            variant_mode_prefix: true,
        },
        WireFormat {
            revision: 54405,
            variant_mode_prefix: false,
        },
    ];

    for seed in 0..20 {
        let rng = &mut SmallRng::seed_from_u64(seed);
        let header = header(TYPES);

        let mut rows = Vec::new();
        let num_rows = rng.random_range(0..300);
        for _ in 0..num_rows {
            for column in header.columns() {
                random_value(column.data_type(), rng, &mut rows);
            }
        }

        let blocks = to_blocks(&header, &rows, rng);
        assert_eq!(blocks.iter().map(Block::num_rows).sum::<usize>(), num_rows);
        assert_eq!(to_row_binary(&blocks), rows, "seed {seed}: columns");

        for format in formats {
            let mut encoded = BytesMut::new();
            for block in blocks
                .iter()
                .filter(|block| format.revision > 0 || block.num_rows() > 0)
            {
                block_encoder::write_block(block, format, &mut encoded).unwrap();
            }

            let decoded = decode(&encoded, format, rng).await;
            assert_eq!(to_row_binary(&decoded), rows, "seed {seed}: {format:?}");

            for block in &decoded {
                let names: Vec<_> = block.columns().iter().map(Column::name).collect();
                let expected: Vec<_> = header.columns().iter().map(Column::name).collect();
                assert_eq!(names, expected);
            }
        }
    }
}

/// A row that is split at any position is read only when it is complete.
#[test]
fn incomplete_rows_are_not_read() {
    let rng = &mut SmallRng::seed_from_u64(1);
    let header = header(TYPES);

    let mut row = Vec::new();
    for column in header.columns() {
        random_value(column.data_type(), rng, &mut row);
    }

    for split in 0..row.len() {
        let mut columns = RowBinaryColumns::new(&header).unwrap();
        let mut input = &row[..split];
        assert!(!columns.read_row(&mut input).unwrap(), "split at {split}");
        assert_eq!(input.len(), split, "the input must not be consumed");
        assert_eq!(columns.num_rows(), 0);

        // The columns are unchanged: the complete row is read afterwards.
        let mut input = &row[..];
        assert!(columns.read_row(&mut input).unwrap());
        assert!(input.is_empty());
        assert_eq!(to_row_binary(&[columns.take_block().unwrap()]), row);
    }
}

#[test]
fn invalid_row_binary_is_an_error() {
    let header = header(&["Nullable(UInt8)", "Variant(String, UInt64)"]);

    let mut columns = RowBinaryColumns::new(&header).unwrap();
    // Invalid `Nullable` flag
    assert!(columns.read_row(&mut &[2u8, 0][..]).is_err());

    let mut columns = RowBinaryColumns::new(&header).unwrap();
    // Variant discriminator out of range
    assert!(columns.read_row(&mut &[1u8, 7][..]).is_err());
}

#[test]
fn unsupported_insert_types() {
    for name in ["Dynamic", "Array(Dynamic)", "Map(String, Dynamic)"] {
        let error = RowBinaryColumns::new(&header(&[name])).err().unwrap();
        assert!(
            error.to_string().contains("not supported"),
            "{name}: {error}"
        );
    }
}

/// Corrupted offsets cause errors instead of panics.
#[test]
fn corrupted_layouts_are_errors() {
    let column = Column {
        name: MaybeUtf8::from_string("s".to_owned()),
        data_type: DataTypeNode::String,
        type_name: MaybeUtf8::from_string("String".to_owned()),
        layout: Layout {
            kind: LayoutKind::Variable {
                end_offsets: vec![5, 3].into(),
                data: Bytes::from_static(b"abcde"),
            },
            nulls: None,
            num_values: 2,
        },
    };
    let block = Block::from_columns(vec![column], 2);

    assert!(to_row_binary::write_rows(&block, &mut BytesMut::new()).is_err());
    assert!(block_encoder::write_block(&block, WireFormat::NATIVE, &mut BytesMut::new()).is_err());
}
