pub use crate::data_types::{Column, DataTypeNode};
use crate::decoders::decode_string;
use crate::error::TypesError;
pub use crate::leb128::put_leb128;
pub use crate::leb128::read_leb128;
use bytes::BufMut;

pub mod data_types;
pub mod decoders;
pub mod error;
pub mod leb128;

pub fn parse_rbwnat_columns_header(bytes: &mut &[u8]) -> Result<Vec<Column>, TypesError> {
    if bytes.len() < 1 {
        return Err(TypesError::NotEnoughData(
            "decoding columns header, expected at least one byte to start".to_string(),
        ));
    }
    let num_columns = read_leb128(bytes)?;
    if num_columns == 0 {
        return Err(TypesError::HeaderParsingError(
            "Expected at least one column in the header".to_string(),
        ));
    }
    let mut columns_names: Vec<String> = Vec::with_capacity(num_columns as usize);
    for _ in 0..num_columns {
        let column_name = decode_string(bytes)?;
        columns_names.push(column_name);
    }
    let mut column_data_types: Vec<DataTypeNode> = Vec::with_capacity(num_columns as usize);
    for _ in 0..num_columns {
        let column_type = decode_string(bytes)?;
        let data_type = DataTypeNode::new(&column_type)?;
        column_data_types.push(data_type);
    }
    let columns = columns_names
        .into_iter()
        .zip(column_data_types)
        .map(|(name, data_type)| Column::new(name, data_type))
        .collect();
    Ok(columns)
}

pub fn put_rbwnat_columns_header(
    columns: &[Column],
    mut buffer: impl BufMut,
) -> Result<(), TypesError> {
    if columns.is_empty() {
        return Err(TypesError::EmptyColumns);
    }
    put_leb128(&mut buffer, columns.len() as u64);
    for column in columns {
        put_leb128(&mut buffer, column.name.len() as u64);
        buffer.put_slice(column.name.as_bytes());
    }
    for column in columns.into_iter() {
        put_leb128(&mut buffer, column.data_type.to_string().len() as u64);
        buffer.put_slice(column.data_type.to_string().as_bytes());
    }
    Ok(())
}
