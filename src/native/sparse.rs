//! Sparse serialization for ClickHouse native protocol.
//!
//! Optimization for columns with many default values -- only non-default values
//! are stored along with their positions. Wire format:
//!
//! 1. Offsets: VarUInt group sizes (count of defaults before each non-default)
//!    - Final group has `END_OF_GRANULE_FLAG` (2^62) ORed in
//! 2. Values: Only the non-default values
//!
//! Example: `[0, 0, 5, 0, 3, 0, 0, 0]` -> offsets [2, 1, 3|END], values [5, 3]

use crate::error::Result;
use crate::native::io::{ClickHouseBytesRead, ClickHouseRead};

/// End-of-granule marker (bit 62). When set, this is the final VarUInt in the offsets stream.
pub(crate) const END_OF_GRANULE_FLAG: u64 = 1 << 62;

/// State for sparse deserialization across multiple reads.
#[derive(Debug, Default, Clone)]
pub(crate) struct SparseDeserializeState {
    /// Trailing defaults from previous read that haven't been consumed yet.
    pub(crate) num_trailing_defaults: u64,
    /// Non-default value pending after the trailing defaults.
    pub(crate) has_value_after_defaults: bool,
}

/// Read sparse offsets from an async stream. Returns positions of non-default values.
///
/// Must loop until `END_OF_GRANULE_FLAG` -- can't stop early even if we have enough
/// rows, or the stream will be misaligned for the next column.
#[allow(clippy::cast_possible_truncation)]
pub(crate) async fn read_sparse_offsets<R: ClickHouseRead>(
    reader: &mut R,
    num_rows: usize,
    state: &mut SparseDeserializeState,
) -> Result<Vec<usize>> {
    let mut offsets = Vec::new();
    let mut current_position: u64 = 0;

    // Handle state carried over from previous read
    if state.num_trailing_defaults > 0 {
        current_position += state.num_trailing_defaults;
        state.num_trailing_defaults = 0;
    }
    if state.has_value_after_defaults {
        if (current_position as usize) < num_rows {
            offsets.push(current_position as usize);
        }
        current_position += 1;
        state.has_value_after_defaults = false;
    }

    loop {
        let group_size = reader.read_var_uint().await?;

        let is_end_of_granule = (group_size & END_OF_GRANULE_FLAG) != 0;
        let actual_group_size = group_size & !END_OF_GRANULE_FLAG;

        current_position += actual_group_size;

        if is_end_of_granule {
            if current_position > num_rows as u64 {
                state.num_trailing_defaults = current_position - num_rows as u64;
            }
            break;
        }

        if (current_position as usize) < num_rows {
            offsets.push(current_position as usize);
            current_position += 1;
        } else {
            state.has_value_after_defaults = true;
        }
    }

    Ok(offsets)
}

/// Sync version of `read_sparse_offsets` for `bytes::Buf` readers.
#[allow(dead_code)] // Only called from tests; kept as a sync alternative for future non-async read paths
#[allow(clippy::cast_possible_truncation)]
pub(crate) fn read_sparse_offsets_sync<R: ClickHouseBytesRead>(
    reader: &mut R,
    num_rows: usize,
    state: &mut SparseDeserializeState,
) -> Result<Vec<usize>> {
    let mut offsets = Vec::new();
    let mut current_position: u64 = 0;

    if state.num_trailing_defaults > 0 {
        current_position += state.num_trailing_defaults;
        state.num_trailing_defaults = 0;
    }
    if state.has_value_after_defaults {
        if (current_position as usize) < num_rows {
            offsets.push(current_position as usize);
        }
        current_position += 1;
        state.has_value_after_defaults = false;
    }

    loop {
        let group_size = reader.try_get_var_uint()?;

        let is_end_of_granule = (group_size & END_OF_GRANULE_FLAG) != 0;
        let actual_group_size = group_size & !END_OF_GRANULE_FLAG;

        current_position += actual_group_size;

        if is_end_of_granule {
            if current_position > num_rows as u64 {
                state.num_trailing_defaults = current_position - num_rows as u64;
            }
            break;
        }

        if (current_position as usize) < num_rows {
            offsets.push(current_position as usize);
            current_position += 1;
        } else {
            state.has_value_after_defaults = true;
        }
    }

    Ok(offsets)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn encode_var_uint(value: u64) -> Vec<u8> {
        let mut result = Vec::new();
        let mut v = value;
        loop {
            let byte = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                result.push(byte);
                break;
            }
            result.push(byte | 0x80);
        }
        result
    }

    #[test]
    fn test_read_sparse_offsets_simple() {
        // Column: [default, default, value, default, value, default, default, default]
        // Positions of non-defaults: [2, 4]
        let mut data = Vec::new();
        data.extend(encode_var_uint(2)); // 2 defaults before first value
        data.extend(encode_var_uint(1)); // 1 default before second value
        data.extend(encode_var_uint(3 | END_OF_GRANULE_FLAG)); // 3 trailing defaults

        let mut bytes = Bytes::from(data);
        let mut state = SparseDeserializeState::default();
        let offsets = read_sparse_offsets_sync(&mut bytes, 8, &mut state).unwrap();

        assert_eq!(offsets, vec![2, 4]);
    }

    #[test]
    fn test_read_sparse_offsets_all_defaults() {
        let mut data = Vec::new();
        data.extend(encode_var_uint(4 | END_OF_GRANULE_FLAG));

        let mut bytes = Bytes::from(data);
        let mut state = SparseDeserializeState::default();
        let offsets = read_sparse_offsets_sync(&mut bytes, 4, &mut state).unwrap();

        assert!(offsets.is_empty());
    }

    #[test]
    fn test_read_sparse_offsets_no_defaults() {
        // All non-default values
        let mut data = Vec::new();
        data.extend(encode_var_uint(0)); // value at 0
        data.extend(encode_var_uint(0)); // value at 1
        data.extend(encode_var_uint(0)); // value at 2
        data.extend(encode_var_uint(END_OF_GRANULE_FLAG)); // 0 trailing defaults

        let mut bytes = Bytes::from(data);
        let mut state = SparseDeserializeState::default();
        let offsets = read_sparse_offsets_sync(&mut bytes, 3, &mut state).unwrap();

        assert_eq!(offsets, vec![0, 1, 2]);
    }

    #[test]
    fn test_read_sparse_offsets_first_is_value() {
        // [value, default, default, value]
        let mut data = Vec::new();
        data.extend(encode_var_uint(0)); // 0 defaults before first value
        data.extend(encode_var_uint(2)); // 2 defaults before second value
        data.extend(encode_var_uint(END_OF_GRANULE_FLAG)); // 0 trailing defaults

        let mut bytes = Bytes::from(data);
        let mut state = SparseDeserializeState::default();
        let offsets = read_sparse_offsets_sync(&mut bytes, 4, &mut state).unwrap();

        assert_eq!(offsets, vec![0, 3]);
    }

    #[tokio::test]
    async fn test_read_sparse_offsets_async() {
        let mut data = Vec::new();
        data.extend(encode_var_uint(2));
        data.extend(encode_var_uint(1));
        data.extend(encode_var_uint(3 | END_OF_GRANULE_FLAG));

        let mut reader = std::io::Cursor::new(data);
        let mut state = SparseDeserializeState::default();
        let offsets = read_sparse_offsets(&mut reader, 8, &mut state).await.unwrap();

        assert_eq!(offsets, vec![2, 4]);
    }

    #[test]
    fn test_single_row_default() {
        // One row, it's the default value.
        let mut data = Vec::new();
        data.extend(encode_var_uint(1 | END_OF_GRANULE_FLAG));

        let mut bytes = Bytes::from(data);
        let mut state = SparseDeserializeState::default();
        let offsets = read_sparse_offsets_sync(&mut bytes, 1, &mut state).unwrap();

        assert!(offsets.is_empty());
    }

    #[test]
    fn test_single_row_non_default() {
        // One row, it's a non-default value.
        let mut data = Vec::new();
        data.extend(encode_var_uint(0)); // 0 defaults before the value
        data.extend(encode_var_uint(END_OF_GRANULE_FLAG)); // 0 trailing

        let mut bytes = Bytes::from(data);
        let mut state = SparseDeserializeState::default();
        let offsets = read_sparse_offsets_sync(&mut bytes, 1, &mut state).unwrap();

        assert_eq!(offsets, vec![0]);
    }

    #[test]
    fn test_large_gap_value_at_end() {
        // 1000 rows, only the very last is non-default.
        // Sparse stream: offset group = 999 defaults, then the value, then END.
        let mut data = Vec::new();
        data.extend(encode_var_uint(999)); // 999 defaults before position 999
        data.extend(encode_var_uint(END_OF_GRANULE_FLAG)); // 0 trailing defaults

        let mut bytes = Bytes::from(data);
        let mut state = SparseDeserializeState::default();
        let offsets = read_sparse_offsets_sync(&mut bytes, 1000, &mut state).unwrap();

        assert_eq!(offsets, vec![999]);
    }

    #[test]
    fn test_value_at_position_zero_only() {
        // 100 rows, only position 0 is non-default.
        let mut data = Vec::new();
        data.extend(encode_var_uint(0)); // 0 defaults before position 0
        data.extend(encode_var_uint(99 | END_OF_GRANULE_FLAG)); // 99 trailing defaults

        let mut bytes = Bytes::from(data);
        let mut state = SparseDeserializeState::default();
        let offsets = read_sparse_offsets_sync(&mut bytes, 100, &mut state).unwrap();

        assert_eq!(offsets, vec![0]);
    }

    #[test]
    fn test_consecutive_non_defaults() {
        // [T, T, T, F, F, T] -- positions 0, 1, 2, 5 are non-default.
        let mut data = Vec::new();
        data.extend(encode_var_uint(0)); // position 0
        data.extend(encode_var_uint(0)); // position 1
        data.extend(encode_var_uint(0)); // position 2
        data.extend(encode_var_uint(2)); // 2 defaults -> position 5
        data.extend(encode_var_uint(END_OF_GRANULE_FLAG)); // 0 trailing

        let mut bytes = Bytes::from(data);
        let mut state = SparseDeserializeState::default();
        let offsets = read_sparse_offsets_sync(&mut bytes, 6, &mut state).unwrap();

        assert_eq!(offsets, vec![0, 1, 2, 5]);
    }

    #[test]
    fn test_state_carry_trailing_defaults() {
        // Simulate state from a previous partial read that left trailing defaults.
        // State: 2 unconsumed defaults from the previous read.
        //
        // New read covers only 3 rows of a block that the sparse stream
        // encodes as covering 4 positions (2 carried + 1 value + 1 trailing).
        // The trailing default beyond num_rows must be saved in state for the
        // next caller.
        //
        // In practice our code always creates fresh state per column per block,
        // but the carry-over paths must still be correct.
        let mut state = SparseDeserializeState {
            num_trailing_defaults: 2, // 2 unconsumed defaults carried in
            has_value_after_defaults: false,
        };
        // Stream: 0 more defaults before next value (-> pos 2), then END with 1 trailing.
        let mut data = Vec::new();
        data.extend(encode_var_uint(0)); // 0 more defaults -> value at position 2
        data.extend(encode_var_uint(1 | END_OF_GRANULE_FLAG)); // 1 trailing default

        let mut bytes = Bytes::from(data);
        // Only 3 rows in this block -- the trailing default goes past the end.
        let offsets = read_sparse_offsets_sync(&mut bytes, 3, &mut state).unwrap();

        // Carried 2 defaults -> position 2 is the value.
        // But num_rows=3, so position 2 is inside the block.
        assert_eq!(offsets, vec![2]);
        // The 1 trailing default puts current_position at 4, which is > num_rows(3).
        // state.num_trailing_defaults = 4 - 3 = 1.
        assert_eq!(state.num_trailing_defaults, 1);
        assert!(!state.has_value_after_defaults);
    }
}
