use crate::error::Error;
use crate::native::Layout;
use clickhouse_types::DataTypeNode;
use std::marker::PhantomData;
use std::slice;

use crate::native::Decode;
use crate::native::decode::ValueReader;

pub struct ArrayReader<'a, T> {
    elem_type: &'a DataTypeNode,
    data: &'a [u8],
    kind: IterKind<'a>,
    nulls: Option<slice::Iter<'a, u8>>,
    // covariant over `'a` and `T`
    _marker: PhantomData<&'a T>,
}

pub struct ArrayData<'a> {
    pub(super) elem_type: &'a DataTypeNode,
    pub(super) layout: &'a Layout,
    pub(super) data: &'a [u8],
    pub(super) nulls: Option<&'a [u8]>,
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
            data: self.data,
            kind: match *self.layout {
                Layout::Fixed { type_width } => IterKind::Fixed(self.data.chunks_exact(type_width)),
                Layout::Offsets(ref offsets) => IterKind::Offsets(offsets.iter()),
                Layout::Array {
                    ref elem_layout,
                    cumulative_lengths: ref lengths,
                } => IterKind::Array {
                    next_start: 0,
                    cumulative_lengths: lengths.iter(),
                    elem_layout,
                },
                Layout::LowCardinality(_) => todo!("implement LowCardinality"),
            },
            nulls: self.nulls.map(|nulls| nulls.iter()),
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
            IterKind::Offsets(offsets) => {
                let start = *offsets.next()?;

                // Need to make sure we advance the main iterator first
                check_null!();

                let end = offsets
                    .as_slice()
                    .first()
                    .copied()
                    .unwrap_or(self.data.len());

                Some(
                    T::decode(&mut ValueReader {
                        data_type: self.elem_type,
                        native_bytes: &self.data[start..end],
                    })
                    .map_err(Error::Other),
                )
            }
            IterKind::Array {
                next_start,
                cumulative_lengths,
                elem_layout,
            } => {
                let DataTypeNode::Array(elem_type) = self.elem_type else {
                    unreachable!(
                        "BUG: expected subarray element type, got {:?}",
                        self.elem_type
                    )
                };

                let array_end = *cumulative_lengths.next()?;

                let nulls = if let Some(nulls) = &mut self.nulls {
                    let (array_nulls, rem) = nulls
                        .as_slice()
                        // FIXME: checked math?
                        .split_at(array_end - *next_start);

                    *nulls = rem.iter();

                    Some(array_nulls)
                } else {
                    None
                };

                *next_start = array_end;

                Some(
                    T::decode_array(ArrayData {
                        elem_type,
                        layout: elem_layout,
                        data: self.data,
                        nulls,
                    })
                    .map_err(Error::Other),
                )
            }
        }
    }
}

enum IterKind<'a> {
    Fixed(slice::ChunksExact<'a, u8>),
    Offsets(slice::Iter<'a, usize>),
    Array {
        next_start: usize,
        cumulative_lengths: slice::Iter<'a, usize>,
        elem_layout: &'a Layout,
    },
}
