use crate::error::BoxedError;
use crate::native::builder::BlockBuilder;
use crate::native::encode::{Encode, ValueWriter};
use clickhouse_types::DataTypeNode;

use std::collections::HashMap;

#[test]
fn array_writer_rolls_back() {
    struct BadArray<'a>(&'a [u32]);

    impl Encode for BadArray<'_> {
        fn produces() -> DataTypeNode {
            DataTypeNode::Array(Box::new(DataTypeNode::UInt32))
        }

        fn encode(&self, writer: &mut ValueWriter<'_>) -> Result<(), BoxedError> {
            let mut writer = writer.write_array()?;

            for val in self.0 {
                writer.write(val)?;
            }

            // Deliberately don't call `writer.finish()`
            drop(writer);

            Ok(())
        }
    }

    let mut builder = BlockBuilder::new();

    builder
        .upsert_column("foo")
        .unwrap()
        .add(&[0u32, 1, 2, 3, 4, 5][..])
        .unwrap();

    builder
        .upsert_column("foo")
        .unwrap()
        .add(BadArray(&[6, 7, 8, 9, 10]))
        .unwrap();

    let block = builder.build().unwrap();

    assert_eq!(block.num_rows(), 1);
}

#[test]
fn tuple_writer_rolls_back() {
    struct BadTuple(i32, String, #[expect(dead_code)] Vec<i64>);

    impl Encode for BadTuple {
        fn produces() -> DataTypeNode {
            DataTypeNode::Tuple(vec![
                DataTypeNode::Int32,
                DataTypeNode::String,
                DataTypeNode::Array(Box::new(DataTypeNode::Int64)),
            ])
        }

        fn encode(&self, writer: &mut ValueWriter<'_>) -> Result<(), BoxedError> {
            let mut writer = writer.write_tuple()?;

            writer.write(self.0)?;
            writer.write(&self.1)?;

            // Deliberately don't finish, this would put the block out of sync
            drop(writer);

            Ok(())
        }
    }

    let mut builder = BlockBuilder::new();

    builder
        .upsert_column("foo")
        .unwrap()
        .add((0i32, "0".to_string(), vec![0i64; 16]))
        .unwrap();

    builder
        .upsert_column("foo")
        .unwrap()
        .add(BadTuple(1, "1".to_string(), vec![1i64; 16]))
        .unwrap();

    let block = builder.build().unwrap();

    assert_eq!(block.num_rows(), 1);
}

#[test]
fn map_writer_rolls_back() {
    struct BadMap(Vec<(u32, String)>);

    impl Encode for BadMap {
        fn produces() -> DataTypeNode {
            DataTypeNode::Map([
                Box::new(DataTypeNode::UInt32),
                Box::new(DataTypeNode::String),
            ])
        }

        fn encode(&self, writer: &mut ValueWriter<'_>) -> Result<(), BoxedError> {
            let mut writer = writer.write_map()?;

            for (key, val) in &self.0 {
                writer.write(key, val)?;
            }

            drop(writer);

            Ok(())
        }
    }

    let mut builder = BlockBuilder::new();

    builder
        .upsert_column("foo")
        .unwrap()
        .add(
            (0u32..5)
                .map(|i| (i, i.to_string()))
                .collect::<HashMap<_, _>>(),
        )
        .unwrap();

    builder
        .upsert_column("foo")
        .unwrap()
        .add(BadMap((5..10).map(|i| (i, i.to_string())).collect()))
        .unwrap();

    let block = builder.build().unwrap();

    assert_eq!(block.num_rows(), 1);
}
