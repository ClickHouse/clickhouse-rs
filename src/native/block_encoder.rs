//! Synchronous encoding of [`Block`]s for the native TCP protocol and the `Native` format.

use crate::error::Error;
use crate::native::reader::{
    REVISION_WITH_CUSTOM_SERIALIZATION, WireFormat, lc_dict_type, variant_counts, wire_type,
};
use crate::native::varuint;
use crate::native::{Block, Layout, LayoutKind, LayoutLowCardinality};
use bytes::{BufMut, BytesMut};
use clickhouse_types::DataTypeNode;

/// Write `block` in `format`.
pub(crate) fn write_block(
    block: &Block,
    format: WireFormat,
    out: &mut BytesMut,
) -> Result<(), Error> {
    let revision = format.revision;
    write_block_info(revision, out);

    varuint::write(&mut *out, block.columns.len());
    varuint::write(&mut *out, block.num_rows);

    for column in &block.columns {
        varuint::write_bytes(&mut *out, column.name.as_bytes());
        varuint::write_bytes(&mut *out, type_name(&column.type_name(), revision));

        if revision >= REVISION_WITH_CUSTOM_SERIALIZATION {
            // Default serialization
            out.put_u8(0);
        }

        if block.num_rows == 0 {
            continue;
        }

        let result = write_prefix(&column.data_type, format, out)
            .and_then(|()| write_data(&column.data_type, &column.layout, block.num_rows, out));

        result.map_err(|e| {
            Error::DataFormat(
                format!(
                    "cannot encode column `{}` of type {}: {e}",
                    column.name, column.data_type
                )
                .into(),
            )
        })?;
    }

    Ok(())
}

/// `DBMS_MIN_REVISION_WITH_TIME_ZONE_PARAMETER_IN_DATETIME_DATA_TYPE`
const REVISION_WITH_TIME_ZONE_PARAMETER_IN_DATETIME: u64 = 54337;

/// The name of the type as the server writes it for `revision`.
///
/// Like the server, the time zone of a `DateTime` column is not written for older revisions,
/// including the `Native` format (revision `0`).
fn type_name(name: &[u8], revision: u64) -> &[u8] {
    if revision < REVISION_WITH_TIME_ZONE_PARAMETER_IN_DATETIME && name.starts_with(b"DateTime(") {
        b"DateTime"
    } else {
        name
    }
}

/// Write a block without columns and rows: the end of data for the native TCP protocol.
#[cfg(feature = "tcp")]
pub(crate) fn write_empty_block(revision: u64, out: &mut BytesMut) {
    write_block_info(revision, out);
    // No columns, no rows
    out.put_slice(&[0, 0]);
}

fn write_block_info(revision: u64, out: &mut BytesMut) {
    if revision == 0 {
        return;
    }

    // `is_overflows = false`
    varuint::write(&mut *out, 1);
    out.put_u8(0);
    // `bucket_num = -1`
    varuint::write(&mut *out, 2);
    out.put_i32_le(-1);
    // End of fields
    varuint::write(&mut *out, 0);
}

fn invalid(message: impl Into<String>) -> Error {
    Error::DataFormat(message.into().into())
}

/// Write the serialization state prefixes of the type, which come before the data.
fn write_prefix(
    data_type: &DataTypeNode,
    format: WireFormat,
    out: &mut BytesMut,
) -> Result<(), Error> {
    match &*wire_type(data_type) {
        DataTypeNode::Nullable(inner) | DataTypeNode::Array(inner) => {
            write_prefix(inner, format, out)
        }
        DataTypeNode::Tuple(types) => types
            .iter()
            .try_for_each(|ty| write_prefix(ty, format, out)),
        DataTypeNode::Map([key, value]) => {
            write_prefix(key, format, out)?;
            write_prefix(value, format, out)
        }
        DataTypeNode::LowCardinality(inner) => {
            // `sharedDictionariesWithAdditionalKeys`
            out.put_u64_le(1);
            write_prefix(inner, format, out)
        }
        DataTypeNode::Variant(types) => {
            if format.variant_mode_prefix {
                // `DiscriminatorsSerializationMode::BASIC`
                out.put_u64_le(0);
            }

            types
                .iter()
                .try_for_each(|ty| write_prefix(ty, format, out))
        }
        DataTypeNode::JSON | DataTypeNode::JsonWithHint(_) => {
            // `SerializationObject::SerializationVersion::STRING`
            out.put_u64_le(1);
            Ok(())
        }
        DataTypeNode::Dynamic | DataTypeNode::AggregateFunction(..) => Err(invalid(format!(
            "encoding type {data_type} is not supported"
        ))),
        _ => Ok(()),
    }
}

fn write_data(
    data_type: &DataTypeNode,
    layout: &Layout,
    num_values: usize,
    out: &mut BytesMut,
) -> Result<(), Error> {
    if layout.num_values != num_values {
        return Err(invalid(format!(
            "column has {} values, expected {num_values}",
            layout.num_values
        )));
    }

    let wire = wire_type(data_type);

    let data_type = match (&*wire, &layout.nulls) {
        (DataTypeNode::Nullable(inner), Some(nulls)) if nulls.len() == num_values => {
            out.put_slice(nulls);
            &**inner
        }
        (DataTypeNode::Nullable(_), _) => {
            return Err(invalid("null map is missing or has a wrong size"));
        }
        (_, Some(_)) => return Err(invalid("unexpected null map")),
        (other, None) => other,
    };

    write_kind(data_type, &layout.kind, num_values, out)
}

fn write_kind(
    data_type: &DataTypeNode,
    kind: &LayoutKind,
    num_values: usize,
    out: &mut BytesMut,
) -> Result<(), Error> {
    let wire = wire_type(data_type);

    match (&*wire, kind) {
        (_, LayoutKind::Fixed { type_width, data }) => {
            if Some(data.len()) != type_width.checked_mul(num_values) {
                return Err(invalid("fixed-width data has a wrong size"));
            }

            out.put_slice(data);
        }
        (_, LayoutKind::Variable { end_offsets, data }) => {
            if end_offsets.len() != num_values {
                return Err(invalid("wrong number of strings"));
            }

            let mut start = 0;

            for &end in end_offsets.iter() {
                let value = data
                    .get(start..end)
                    .ok_or_else(|| invalid("string offsets are out of range"))?;
                varuint::write_bytes(&mut *out, value);
                start = end;
            }
        }
        (
            DataTypeNode::Array(elem_type),
            LayoutKind::Array {
                end_indices,
                elem_layout,
            },
        ) => {
            let total = write_offsets(end_indices, num_values, out)?;
            write_data(elem_type, elem_layout, total, out)?;
        }
        (DataTypeNode::Tuple(types), LayoutKind::Tuple { layouts })
            if types.len() == layouts.len() =>
        {
            for (ty, layout) in types.iter().zip(layouts.iter()) {
                write_data(ty, layout, num_values, out)?;
            }
        }
        (
            DataTypeNode::Map([key_type, value_type]),
            LayoutKind::Map {
                key_val_layouts,
                end_indices,
            },
        ) => {
            let total = write_offsets(end_indices, num_values, out)?;
            let [key_layout, value_layout] = &**key_val_layouts;
            write_data(key_type, key_layout, total, out)?;
            write_data(value_type, value_layout, total, out)?;
        }
        (DataTypeNode::LowCardinality(inner), LayoutKind::LowCardinality(lc)) => {
            write_low_cardinality(inner, lc, num_values, out)?;
        }
        (
            DataTypeNode::Variant(types),
            LayoutKind::Variant {
                discriminators,
                variants,
            },
        ) if types.len() == variants.len() && discriminators.len() == num_values => {
            out.put_slice(discriminators);

            let counts = variant_counts(discriminators, types.len())?;

            for ((ty, layout), count) in types.iter().zip(variants.iter()).zip(counts) {
                write_data(ty, layout, count, out)?;
            }
        }
        _ => {
            return Err(invalid(format!(
                "data layout does not match type {data_type}"
            )));
        }
    }

    Ok(())
}

/// Write the ending offsets of arrays and return the total number of elements.
fn write_offsets(
    end_indices: &[usize],
    num_values: usize,
    out: &mut BytesMut,
) -> Result<usize, Error> {
    if end_indices.len() != num_values {
        return Err(invalid("wrong number of array offsets"));
    }

    let mut prev = 0;

    for &end in end_indices {
        if end < prev {
            return Err(invalid("array offsets are not monotonic"));
        }

        out.put_u64_le(end as u64);
        prev = end;
    }

    Ok(prev)
}

fn write_low_cardinality(
    inner: &DataTypeNode,
    lc: &LayoutLowCardinality,
    num_values: usize,
    out: &mut BytesMut,
) -> Result<(), Error> {
    const HAS_ADDITIONAL_KEYS_BIT: u64 = 0x200;
    const NEEDS_UPDATE_DICTIONARY_BIT: u64 = 0x400;

    if num_values == 0 {
        return Ok(());
    }

    if lc.keys.len() != num_values {
        return Err(invalid("wrong number of LowCardinality keys"));
    }

    let (dict_type, _) = lc_dict_type(inner);

    let dict_len = lc.dict.num_values;

    if let Some(&key) = lc.keys.iter().find(|&&key| key >= dict_len) {
        return Err(invalid(format!(
            "LowCardinality key {key} is out of range for {dict_len} dictionary values"
        )));
    }

    let (key_type, key_width) = match dict_len {
        0..=0xFF => (0u64, 1usize),
        0x100..=0xFFFF => (1, 2),
        0x1_0000..=0xFFFF_FFFF => (2, 4),
        _ => (3, 8),
    };

    out.put_u64_le(key_type | HAS_ADDITIONAL_KEYS_BIT | NEEDS_UPDATE_DICTIONARY_BIT);
    out.put_u64_le(dict_len as u64);
    write_data(dict_type, &lc.dict, dict_len, out)?;
    out.put_u64_le(num_values as u64);

    for &key in lc.keys.iter() {
        // Checked above: the key fits into `key_width` bytes.
        out.put_slice(&(key as u64).to_le_bytes()[..key_width]);
    }

    Ok(())
}
