pub use crate::data_types::{Column, DataTypeNode};
use crate::decoders::{ensure_size, read_string};
use crate::error::TypesError;
pub use crate::leb128::put_leb128;
pub use crate::leb128::read_leb128;
use bytes::BufMut;

pub mod data_types;
pub mod decoders;
pub mod error;
pub mod leb128;

pub fn parse_rbwnat_columns_header(buffer: &mut &[u8]) -> Result<Vec<Column>, TypesError> {
    ensure_size(buffer, 1)?;
    let num_columns = read_leb128(buffer)?;
    if num_columns == 0 {
        return Err(TypesError::HeaderParsingError(
            "Expected at least one column in the header".to_string(),
        ));
    }
    let mut columns_names: Vec<String> = Vec::with_capacity(num_columns as usize);
    for _ in 0..num_columns {
        let column_name = read_string(buffer)?;
        columns_names.push(column_name);
    }
    let mut column_data_types: Vec<DataTypeNode> = Vec::with_capacity(num_columns as usize);
    for _ in 0..num_columns {
        let column_type = read_string(buffer)?;
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
    for column in columns.iter() {
        put_leb128(&mut buffer, column.data_type.to_string().len() as u64);
        buffer.put_slice(column.data_type.to_string().as_bytes());
    }
    Ok(())
}
