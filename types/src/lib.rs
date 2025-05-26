use crate::data_types::{Column, DataTypeNode};
use crate::decoders::decode_string;
use crate::error::ParserError;
use crate::leb128::decode_leb128;

pub mod data_types;
pub mod decoders;
pub mod error;
pub mod leb128;

pub fn parse_rbwnat_columns_header(bytes: &mut &[u8]) -> Result<Vec<Column>, ParserError> {
    let num_columns = decode_leb128(bytes)?;
    if num_columns == 0 {
        return Err(ParserError::HeaderParsingError(
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
