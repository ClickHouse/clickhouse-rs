use crate::error::Error;
use crate::native::{Layout, LayoutKind};
use clickhouse_types::DataTypeNode;
use std::marker::PhantomData;
use std::ops::Range;
use std::slice;

use crate::native::Decode;
use crate::native::decode::ValueReader;

pub struct ArrayReader<'a, T> {
    elem_type: &'a DataTypeNode,
    kind: IterKind<'a>,
    nulls: Option<slice::Iter<'a, u8>>,
    // covariant over `'a` and `T`
    _marker: PhantomData<&'a T>,
}

pub struct ArrayData<'a> {
    pub(super) elem_type: &'a DataTypeNode,
    pub(super) layout: &'a Layout,
    pub(super) indices: Range<usize>,
}

impl<'a> ArrayData<'a> {
    pub fn into_reader<T>(self) -> Result<ArrayReader<'a, T>, Error>
    where
        T: Decode<'a>,
    {
        if !T::compatible(self.elem_type) {
            return Err(Error::Custom(format!(
                "incompatible data type {}",
                self.elem_type
            )));
        }

        Ok(self.into_reader_unchecked())
    }

    pub(super) fn into_reader_unchecked<T>(self) -> ArrayReader<'a, T>
    where
        T: Decode<'a>,
    {
        ArrayReader {
            elem_type: self.elem_type,
            kind: match self.layout.kind {
                LayoutKind::Fixed {
                    type_width,
                    ref data,
                } => IterKind::Fixed(
                    data[self.indices.start * type_width..self.indices.end * type_width]
                        .chunks_exact(type_width),
                ),
                LayoutKind::Variable {
                    end_offsets: ref end_indices,
                    ref data,
                } => {
                    let next_start = end_indices[..self.indices.start]
                        .last()
                        .copied()
                        .unwrap_or(0);

                    IterKind::Variable {
                        next_start,
                        end_indices: end_indices[self.indices.clone()].iter(),
                        data,
                    }
                }
                LayoutKind::Array {
                    ref elem_layout,
                    ref end_indices,
                } => IterKind::Array {
                    next_start: 0,
                    end_indices: end_indices.iter(),
                    elem_layout,
                },
                LayoutKind::LowCardinality(_) => todo!("implement LowCardinality"),
            },
            nulls: self
                .layout
                .nulls
                .as_ref()
                .map(|nulls| nulls[self.indices].iter()),
            _marker: PhantomData,
        }
    }
}

impl<'a, T> Iterator for ArrayReader<'a, T>
where
    T: Decode<'a>,
{
    type Item = Result<T, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        macro_rules! check_null {
            () => {
                match self.nulls.as_mut().map(Iterator::next) {
                    // not-null = 0x0, or no null-map
                    Some(Some(&0)) | None => (),
                    Some(Some(_)) => {
                        return Some(T::decode_null(self.elem_type).map_err(Error::Other))
                    }
                    Some(None) => return None,
                }
            };
        }

        match &mut self.kind {
            IterKind::Fixed(iter) => {
                let native_bytes = iter.next()?;

                // Need to make sure we advance the main iterator first
                check_null!();

                Some(
                    T::decode(&mut ValueReader {
                        data_type: self.elem_type,
                        native_bytes,
                    })
                    .map_err(Error::Other),
                )
            }
            IterKind::Variable {
                next_start,
                end_indices,
                data,
            } => {
                let end = *end_indices.next()?;

                let start = *next_start;
                *next_start = end;

                // Need to make sure we advance the main iterator first
                check_null!();

                Some(
                    T::decode(&mut ValueReader {
                        data_type: self.elem_type,
                        native_bytes: &data[start..end],
                    })
                    .map_err(Error::Other),
                )
            }
            IterKind::Array {
                next_start,
                end_indices,
                elem_layout,
            } => {
                let DataTypeNode::Array(elem_type) = self.elem_type else {
                    unreachable!(
                        "BUG: expected subarray element type, got {:?}",
                        self.elem_type
                    )
                };

                let array_end = *end_indices.next()?;

                let indices = *next_start..array_end;

                *next_start = array_end;

                Some(
                    T::decode_array(ArrayData {
                        elem_type,
                        layout: elem_layout,
                        indices,
                    })
                    .map_err(Error::Other),
                )
            }
        }
    }
}

enum IterKind<'a> {
    Fixed(slice::ChunksExact<'a, u8>),
    Variable {
        next_start: usize,
        end_indices: slice::Iter<'a, usize>,
        data: &'a [u8],
    },
    Array {
        next_start: usize,
        end_indices: slice::Iter<'a, usize>,
        elem_layout: &'a Layout,
    },
}
