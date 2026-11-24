//! Conversion of [`Block`]s to the `RowBinary` format.
//!
//! The native TCP protocol always returns data as columnar blocks.
//! The row cursors of this crate read `RowBinary`, so the blocks are converted row by row.

use crate::error::Error;
use crate::native::reader::{VARIANT_NULL_DISCRIMINATOR, wire_type};
use crate::native::varuint;
use crate::native::{Block, Layout, LayoutKind};
use bytes::{BufMut, BytesMut};
use clickhouse_types::DataTypeNode;

/// Write the `RowBinaryWithNamesAndTypes` header for the columns of `block`.
pub(crate) fn write_names_and_types(
    block: &Block,
    with_types: bool,
    out: &mut impl BufMut,
) -> Result<(), Error> {
    varuint::write(&mut *out, block.columns.len());

    for column in &block.columns {
        write_bytes(column.name.as_bytes(), out);
    }

    if with_types {
        for column in &block.columns {
            write_bytes(column.type_name.as_bytes(), out);
        }
    }

    Ok(())
}

/// Append all rows of `block` to `out` in the `RowBinary` format.
pub(crate) fn write_rows(block: &Block, out: &mut BytesMut) -> Result<(), Error> {
    let columns = block
        .columns
        .iter()
        .map(|column| {
            Node::new(&column.data_type, &column.layout).map_err(|e| {
                Error::DataFormat(
                    format!(
                        "cannot convert column `{}` of type {} to RowBinary: {e}",
                        column.name, column.data_type
                    )
                    .into(),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    // A single fixed-width column has the same layout in both formats.
    if let [Node::Fixed { width, data }] = &columns[..]
        && Some(data.len()) == width.checked_mul(block.num_rows)
    {
        out.put_slice(data);
        return Ok(());
    }

    for row in 0..block.num_rows {
        for column in &columns {
            column.write(row, out)?;
        }
    }

    Ok(())
}

fn write_bytes(bytes: &[u8], out: &mut impl BufMut) {
    varuint::write(&mut *out, bytes.len());
    out.put_slice(bytes);
}

/// A view of a column prepared for row-wise access.
enum Node<'a> {
    /// `Nothing` has no values: `RowBinary` has no data for it.
    Nothing,
    Fixed {
        width: usize,
        data: &'a [u8],
    },
    Strings {
        end_offsets: &'a [usize],
        data: &'a [u8],
    },
    Nullable {
        nulls: &'a [u8],
        inner: Box<Node<'a>>,
    },
    LowCardinality {
        keys: &'a [usize],
        dict: Box<Node<'a>>,
        /// Key `0` is `NULL`.
        nullable: bool,
    },
    Array {
        end_indices: &'a [usize],
        elem: Box<Node<'a>>,
    },
    Tuple(Vec<Node<'a>>),
    Map {
        end_indices: &'a [usize],
        key: Box<Node<'a>>,
        value: Box<Node<'a>>,
    },
    Variant {
        discriminators: &'a [u8],
        /// The index of each row in the values of its variant.
        local_indices: Vec<usize>,
        variants: Vec<Node<'a>>,
    },
}

fn mismatch(data_type: &DataTypeNode) -> Error {
    Error::DataFormat(format!("unexpected data layout for type {data_type}").into())
}

fn corrupted(what: &str) -> Error {
    Error::DataFormat(format!("corrupted column data: {what}").into())
}

impl<'a> Node<'a> {
    fn new(data_type: &DataTypeNode, layout: &'a Layout) -> Result<Self, Error> {
        let wire = wire_type(data_type);

        let inner_type = match (&*wire, &layout.nulls) {
            (DataTypeNode::Nullable(inner), Some(nulls)) => {
                return Ok(Node::Nullable {
                    nulls,
                    inner: Box::new(Self::new_kind(inner, layout)?),
                });
            }
            (DataTypeNode::Nullable(_), None) | (_, Some(_)) => return Err(mismatch(data_type)),
            (inner, None) => inner,
        };

        Self::new_kind(inner_type, layout)
    }

    /// Prepare the data of `layout`, ignoring its null map.
    fn new_kind(data_type: &DataTypeNode, layout: &'a Layout) -> Result<Self, Error> {
        let wire = wire_type(data_type);

        Ok(match (&*wire, &layout.kind) {
            (DataTypeNode::Nothing, LayoutKind::Fixed { .. }) => Node::Nothing,
            (_, LayoutKind::Fixed { type_width, data }) => Node::Fixed {
                width: *type_width,
                data,
            },
            (
                DataTypeNode::String | DataTypeNode::JSON | DataTypeNode::JsonWithHint(_),
                LayoutKind::Variable { end_offsets, data },
            ) => Node::Strings { end_offsets, data },
            (DataTypeNode::LowCardinality(inner), LayoutKind::LowCardinality(lc)) => {
                let (dict_type, nullable) = match &**inner {
                    DataTypeNode::Nullable(inner) => (&**inner, true),
                    inner => (inner, false),
                };

                Node::LowCardinality {
                    keys: &lc.keys,
                    dict: Box::new(Self::new(dict_type, &lc.dict)?),
                    nullable,
                }
            }
            (
                DataTypeNode::Array(elem_type),
                LayoutKind::Array {
                    end_indices,
                    elem_layout,
                },
            ) => Node::Array {
                end_indices,
                elem: Box::new(Self::new(elem_type, elem_layout)?),
            },
            (DataTypeNode::Tuple(types), LayoutKind::Tuple { layouts })
                if types.len() == layouts.len() =>
            {
                Node::Tuple(
                    types
                        .iter()
                        .zip(layouts.iter())
                        .map(|(ty, layout)| Self::new(ty, layout))
                        .collect::<Result<_, _>>()?,
                )
            }
            (
                DataTypeNode::Map([key_type, value_type]),
                LayoutKind::Map {
                    key_val_layouts,
                    end_indices,
                },
            ) => {
                let [key_layout, value_layout] = &**key_val_layouts;

                Node::Map {
                    end_indices,
                    key: Box::new(Self::new(key_type, key_layout)?),
                    value: Box::new(Self::new(value_type, value_layout)?),
                }
            }
            (
                DataTypeNode::Variant(types),
                LayoutKind::Variant {
                    discriminators,
                    variants,
                },
            ) if types.len() == variants.len() => {
                let mut counts = vec![0usize; variants.len()];

                let local_indices = discriminators
                    .iter()
                    .map(|&discriminator| {
                        if discriminator == VARIANT_NULL_DISCRIMINATOR {
                            return Ok(0);
                        }

                        let count = counts
                            .get_mut(usize::from(discriminator))
                            .ok_or_else(|| corrupted("Variant discriminator is out of range"))?;
                        let index = *count;
                        *count += 1;
                        Ok(index)
                    })
                    .collect::<Result<Vec<_>, Error>>()?;

                Node::Variant {
                    discriminators,
                    local_indices,
                    variants: types
                        .iter()
                        .zip(variants.iter())
                        .map(|(ty, layout)| Self::new(ty, layout))
                        .collect::<Result<_, _>>()?,
                }
            }
            _ => return Err(mismatch(data_type)),
        })
    }

    fn write(&self, index: usize, out: &mut impl BufMut) -> Result<(), Error> {
        match self {
            Node::Nothing => {}
            Node::Fixed { width, data } => {
                let start = index
                    .checked_mul(*width)
                    .ok_or_else(|| corrupted("index is out of range"))?;
                let value = data
                    .get(start..start.saturating_add(*width))
                    .ok_or_else(|| corrupted("fixed-width value is out of range"))?;
                out.put_slice(value);
            }
            Node::Strings { end_offsets, data } => {
                let (start, end) = range(end_offsets, index)?;
                let value = data
                    .get(start..end)
                    .ok_or_else(|| corrupted("string is out of range"))?;
                write_bytes(value, out);
            }
            Node::Nullable { nulls, inner } => {
                let is_null = *nulls
                    .get(index)
                    .ok_or_else(|| corrupted("null map is too short"))?
                    != 0;

                out.put_u8(u8::from(is_null));

                if !is_null {
                    inner.write(index, out)?;
                }
            }
            Node::LowCardinality {
                keys,
                dict,
                nullable,
            } => {
                let key = *keys
                    .get(index)
                    .ok_or_else(|| corrupted("LowCardinality keys are too short"))?;

                if *nullable {
                    out.put_u8(u8::from(key == 0));

                    if key == 0 {
                        return Ok(());
                    }
                }

                dict.write(key, out)?;
            }
            Node::Array { end_indices, elem } => {
                let (start, end) = range(end_indices, index)?;
                varuint::write(&mut *out, end - start);

                for elem_index in start..end {
                    elem.write(elem_index, out)?;
                }
            }
            Node::Tuple(elements) => {
                for element in elements {
                    element.write(index, out)?;
                }
            }
            Node::Map {
                end_indices,
                key,
                value,
            } => {
                let (start, end) = range(end_indices, index)?;
                varuint::write(&mut *out, end - start);

                for entry in start..end {
                    key.write(entry, out)?;
                    value.write(entry, out)?;
                }
            }
            Node::Variant {
                discriminators,
                local_indices,
                variants,
            } => {
                let discriminator = *discriminators
                    .get(index)
                    .ok_or_else(|| corrupted("Variant discriminators are too short"))?;

                out.put_u8(discriminator);

                if discriminator != VARIANT_NULL_DISCRIMINATOR {
                    let variant = variants
                        .get(usize::from(discriminator))
                        .ok_or_else(|| corrupted("Variant discriminator is out of range"))?;
                    let local_index = *local_indices
                        .get(index)
                        .ok_or_else(|| corrupted("Variant discriminators are too short"))?;
                    variant.write(local_index, out)?;
                }
            }
        }

        Ok(())
    }
}

/// The range of the values of row `index` given the ending offset of each row.
fn range(end_offsets: &[usize], index: usize) -> Result<(usize, usize), Error> {
    let start = match index.checked_sub(1) {
        Some(prev) => *end_offsets
            .get(prev)
            .ok_or_else(|| corrupted("offsets are too short"))?,
        None => 0,
    };
    let end = *end_offsets
        .get(index)
        .ok_or_else(|| corrupted("offsets are too short"))?;

    if start > end {
        return Err(corrupted("offsets are not monotonic"));
    }

    Ok((start, end))
}
