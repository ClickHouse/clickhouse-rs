use crate::error::Error;
use crate::native::{Layout, LayoutKind, LayoutLowCardinality};
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

pub struct TupleIter<'a> {
    pub(super) types: slice::Iter<'a, DataTypeNode>,
    pub(super) layouts: slice::Iter<'a, Layout>,
    pub(super) index: usize,
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
    Tuple {
        layouts: &'a [Layout],
        elem_indices: Range<usize>,
    },
    Map {
        next_start: usize,
        end_indices: slice::Iter<'a, usize>,
        key_val_layouts: &'a [Layout; 2],
    },
    LowCardinality {
        keys: slice::Iter<'a, usize>,
        lc: &'a LayoutLowCardinality,
    },
}

impl<'a> ArrayData<'a> {
    pub fn into_reader<T>(self) -> Result<ArrayReader<'a, T>, Error>
    where
        T: Decode<'a>,
    {
        if !T::compatible(self.elem_type.remove_low_cardinality()) {
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
                LayoutKind::Tuple { ref layouts } => IterKind::Tuple {
                    layouts,
                    elem_indices: self.indices.clone(),
                },
                LayoutKind::Map {
                    ref key_val_layouts,
                    ref end_indices,
                } => IterKind::Map {
                    next_start: 0,
                    key_val_layouts,
                    end_indices: end_indices.iter(),
                },
                LayoutKind::LowCardinality(ref lc) => IterKind::LowCardinality {
                    keys: lc.keys.iter(),
                    lc,
                },
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
            IterKind::Tuple {
                layouts,
                elem_indices,
            } => {
                let DataTypeNode::Tuple(types) = self.elem_type else {
                    unreachable!("BUG: expected tuple type, got {:?}", self.elem_type)
                };

                let index = elem_indices.next()?;

                // Need to make sure we advance the main iterator first
                check_null!();

                Some(
                    T::decode_tuple(TupleIter {
                        layouts: layouts.iter(),
                        types: types.iter(),
                        index,
                    })
                    .map_err(Error::Other),
                )
            }
            IterKind::Map {
                next_start,
                end_indices,
                key_val_layouts: [key_layout, val_layout],
            } => {
                let DataTypeNode::Map([key_ty, val_ty]) = self.elem_type else {
                    unreachable!("BUG: expected map type, got {:?}", self.elem_type)
                };

                let array_end = *end_indices.next()?;

                let indices = *next_start..array_end;

                *next_start = array_end;

                Some(
                    T::decode_map(
                        ArrayData {
                            elem_type: key_ty,
                            layout: key_layout,
                            indices: indices.clone(),
                        },
                        ArrayData {
                            elem_type: val_ty,
                            layout: val_layout,
                            indices,
                        },
                    )
                    .map_err(Error::Other),
                )
            }
            IterKind::LowCardinality { keys, lc } => {
                let DataTypeNode::LowCardinality(inner_type) = self.elem_type else {
                    unreachable!("BUG: expected map type, got {:?}", self.elem_type)
                };

                let key = *keys.next()?;

                // Check the null map in case of `Nullable(LowCardinality(...))`
                check_null!();

                // `key = 0` is the NULL placeholder
                if lc.is_nullable && key == 0 {
                    return Some(T::decode_null(self.elem_type).map_err(Error::Other));
                }

                Some(find_lc_data(lc, inner_type, key).and_then(|data| {
                    data.into_reader()?.next().ok_or_else(|| {
                        Error::Custom(format!(
                            "data for LowCardinality({inner_type}) key not found: {key}"
                        ))
                    })?
                }))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.kind {
            IterKind::Fixed(chunks) => chunks.size_hint(),
            IterKind::Variable { end_indices, .. } => end_indices.size_hint(),
            IterKind::Array { end_indices, .. } => end_indices.size_hint(),
            IterKind::Tuple { elem_indices, .. } => elem_indices.size_hint(),
            IterKind::Map { end_indices, .. } => end_indices.size_hint(),
            IterKind::LowCardinality { keys, .. } => keys.size_hint(),
        }
    }
}

impl<'a> TupleIter<'a> {
    pub fn decode_next<T: Decode<'a>>(&mut self) -> Result<T, Error> {
        let (elem_type, layout) = self.types.next().zip(self.layouts.next()).ok_or_else(|| {
            Error::Custom("attempting to decode tuple with more types than received".into())
        })?;

        ArrayData {
            elem_type,
            layout,
            // If `self.index + 1` overflows this range will be empty
            indices: self.index..self.index + 1,
        }
        .into_reader::<T>()?
        .next()
        .ok_or_else(|| Error::Custom("attempting to decode from an empty array".into()))?
    }
}

fn find_lc_data<'a>(
    lc: &'a LayoutLowCardinality,
    inner_type: &'a DataTypeNode,
    mut key: usize,
) -> Result<ArrayData<'a>, Error> {
    if let Some(global_dict) = &lc.global_dict {
        if key < global_dict.num_values {
            return Ok(ArrayData {
                elem_type: inner_type,
                layout: global_dict,
                indices: key..key + 1,
            });
        }

        key -= global_dict.num_values;
    }

    if let Some(local_dict) = &lc.local_dict
        && key < local_dict.num_values
    {
        return Ok(ArrayData {
            elem_type: inner_type,
            layout: local_dict,
            indices: key..key + 1,
        });
    }

    Err(Error::Custom(format!(
        "key for LowCardinality({inner_type}) not in range: {key}"
    )))
}
