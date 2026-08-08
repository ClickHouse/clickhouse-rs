use std::fmt::{Debug, Formatter, Write};

pub(super) struct DebugNullMap<'a>(pub &'a [u8]);

impl Debug for DebugNullMap<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for b in self.0 {
            match b {
                0 => f.write_char('0')?,
                1 => f.write_char('1')?,
                // Flag invalid bytes with brackets
                _ => write!(f, "{{{b:x}}}")?,
            }
        }

        Ok(())
    }
}

pub(super) struct DebugFixedData<'a> {
    pub(super) type_width: usize,
    pub(super) data: &'a [u8],
}

pub(super) struct DebugHex<'a>(&'a [u8]);

impl Debug for DebugFixedData<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.data.chunks(self.type_width).map(DebugHex))
            .finish()
    }
}

impl Debug for DebugHex<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("0x")?;

        // Write the data as it appears so there's no confusion;
        // for little-endian values this may be in reverse
        for b in self.0.iter() {
            write!(f, "{b:02x}")?;
        }

        Ok(())
    }
}

pub(super) struct DebugVariableData<'a> {
    pub end_offsets: &'a [usize],
    pub data: &'a [u8],
}

impl Debug for DebugVariableData<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut list = f.debug_list();

        let mut start_offset = 0;

        for &end_offset in self.end_offsets {
            // `impl Debug for fmt::Arguments` forwards to `Display`
            list.entry(&format_args!(
                "\"{}\"",
                &self.data[start_offset..end_offset].escape_ascii()
            ));
            start_offset = end_offset;
        }

        list.finish()
    }
}
