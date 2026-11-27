//! Execute RowBinary user-defined functions over synchronous input/output streams.
//!
//! # ClickHouse configuration
//!
//! **Requires `format=RowBinary` and `send_chunk_header=true`.** Each input chunk
//! starts with an ASCII decimal row count and a newline, followed by that many
//! RowBinary rows. The executor writes one result per input row, in the same order,
//! without an output chunk header, and flushes after every chunk. It processes
//! consecutive chunks until the input closes, including for `executable_pool` UDFs.
//! It relies on ClickHouse sending the next chunk after receiving the current
//! chunk's results.
//!
//! **Input `Row` fields must match the ClickHouse arguments in both type and
//! order.** Fields are decoded in Serde order, normally Rust declaration order.
//! Field names and `#[serde(rename)]` do not reorder input. There is no schema
//! header or validation. Output fields must likewise match the declared return
//! type, including the element order of tuple results. `Row` is required on the
//! input and output types; their nested fields only need the appropriate Serde traits.
//!
//! Use the existing [`crate::serde`] adapters and [`crate::types`] wrappers for
//! ClickHouse types. For arbitrary-byte `String` values, use `serde_bytes` rather
//! than UTF-8 Rust strings. `Option<T>` uses `Nullable(T)` encoding; Variant's
//! implicit NULL encoding requires schema information and is not inferred here.
//! Unsupported Serde representations retain the existing codec's panics.
//!
//! # Example
//!
//! ```no_run
//! use clickhouse::{Row, udf};
//! use serde::Deserialize;
//!
//! #[derive(Row, Deserialize)]
//! struct Input<'a> {
//!     text: &'a str,
//! }
//!
//! fn main() -> udf::Result<()> {
//!     let stdin = std::io::stdin();
//!     let stdout = std::io::stdout();
//!     udf::Executor::new(stdin.lock(), stdout.lock())
//!         .run::<Input<'_>, u64, _>(|row| Ok(row.text.len() as u64))
//! }
//! ```
//!
//! The corresponding executable UDF configuration includes:
//!
//! ```xml
//! <format>RowBinary</format>
//! <send_chunk_header>true</send_chunk_header>
//! <argument><type>String</type></argument>
//! <return_type>UInt64</return_type>
//! ```
//!
//! Handlers may retain state across chunks. Both input and output rows may borrow:
//! each result is serialized before its input row and backing buffer are reused.
//! Reserve stdout for results and send diagnostics to stderr. On any returned
//! error, terminate the worker; some results may already have been written and
//! the input stream may no longer be aligned. ClickHouse controls pipe timeouts.

use std::io::{self, BufRead, BufWriter, Write};

use crate::{RowRead, RowWrite, rowbinary};

/// A result from an executable UDF or its handler.
pub type Result<T> = std::result::Result<T, Error>;

/// A fatal executable-UDF error. The worker must terminate after an error.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Reading, writing, flushing, or an unexpected EOF failed.
    #[error("UDF I/O error: {0}")]
    Io(#[from] io::Error),
    /// A row could not be encoded or decoded.
    #[error("UDF codec error: {0}")]
    Codec(#[from] crate::error::Error),
    /// The decimal chunk header was malformed or overflowed.
    #[error("invalid UDF chunk header: {0}")]
    InvalidHeader(&'static str),
    /// Input bytes remained buffered after the chunk's declared rows were decoded.
    #[error(
        "UDF chunk has {0} unconsumed bytes; input row fields may not match the configured arguments"
    )]
    TrailingBytes(usize),
    /// Reserving memory for the input buffer failed.
    #[error("UDF input allocation failed: {0}")]
    Allocation(#[from] std::collections::TryReserveError),
    /// The user-supplied handler failed.
    #[error("UDF handler failed: {0}")]
    Handler(#[source] Box<dyn std::error::Error + Send + Sync>),
}

impl Error {
    /// Wrap a handler's error or error message.
    pub fn handler(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self::Handler(error.into())
    }
}

/// A synchronous RowBinary executable-UDF runner.
///
/// Requires `format=RowBinary` and `send_chunk_header=true`. Input fields must
/// match the SQL argument order and types; no schema validation is performed.
/// The input and output streams may be borrowed, such as `stdin.lock()` and a
/// mutable `Vec<u8>`. The executor owns and reuses its row buffers.
pub struct Executor<I, W> {
    input: I,
    output: W,
}

impl<I: BufRead, W: Write> Executor<I, W> {
    /// Create an executor. No client or async runtime is required.
    pub fn new(input: I, output: W) -> Self {
        Self { input, output }
    }

    /// Process chunks until clean EOF between chunks.
    ///
    /// `R` describes the input row and `O` the output row. Their existing
    /// [`RowRead`] and [`RowWrite`] bounds retain the codec's `Row` requirements.
    /// The handler receives each decoded row by reference and returns exactly
    /// one output. Results can borrow from that reference and are serialized
    /// before it expires. The handler is never retried after being called.
    ///
    /// Empty chunks are flushed too. EOF inside a header or row is an error.
    /// Buffered input left after decoding the declared rows is also an error.
    /// Any error is fatal to the worker; previously written rows are not rolled back.
    ///
    /// For UDFs processing large rows (>8KiB), such as LLM prompts or text payloads,
    /// increase the input buffer capacity to accommodate typical encoded row
    /// sizes. Wrap `stdin.lock()` using [`std::io::BufReader::with_capacity`],
    /// for example with a 1 MiB buffer:
    ///
    /// ```no_run
    /// use clickhouse::{Row, udf};
    /// use serde::Deserialize;
    /// use std::io::{self, BufReader};
    ///
    /// #[derive(Row, Deserialize)]
    /// struct Input<'a> {
    ///     text: &'a str,
    /// }
    ///
    /// fn main() -> udf::Result<()> {
    ///     let stdin = io::stdin();
    ///     let stdout = io::stdout();
    ///     let input = BufReader::with_capacity(1024 * 1024, stdin.lock());
    ///     udf::Executor::new(input, stdout.lock())
    ///         .run::<Input<'_>, u64, _>(|row| Ok(row.text.len() as u64))
    /// }
    /// ```
    ///
    /// Incomplete rows are decoded again from their beginning after each refill.
    /// Large arrays received in small fragments can incur quadratic decoding
    /// work through repeated parsing and allocation of their prefixes. A larger
    /// input buffer can reduce retries, but pipe reads may still return fewer
    /// bytes than its capacity, so it does not eliminate this worst case.
    pub fn run<R, O, F>(self, mut handler: F) -> Result<()>
    where
        R: RowRead,
        O: RowWrite,
        F: for<'input, 'row> FnMut(&'row R::Value<'input>) -> Result<O::Value<'row>>,
    {
        let mut input = Input {
            reader: self.input,
            bytes: Vec::new(),
            position: 0,
        };
        let mut output = BufWriter::new(self.output);
        let mut encoded = Vec::new();

        // With `send_chunk_header=true`, each chunk begins with its row count.
        while let Some(header) = ChunkHeader::from_chunk(&mut input.reader)? {
            for _ in 0..header.row_count {
                // An incomplete row is retained and decoded again from its beginning
                // after each refill. A large array arriving in small, fixed-size
                // fragments can repeatedly decode and allocate the same prefix,
                // causing quadratic total decoding work as the row size grows.
                loop {
                    match process_row::<R, O, _>(
                        &input.bytes[input.position..],
                        &mut handler,
                        &mut encoded,
                    )? {
                        Some(consumed) => {
                            input.position += consumed;
                            output.write_all(&encoded)?;
                            break;
                        }
                        None => input.refill()?,
                    }
                }
            }
            let remaining = input.bytes.len() - input.position;
            if remaining != 0 {
                return Err(Error::TrailingBytes(remaining));
            }
            output.flush()?;
            input.bytes.clear();
            input.position = 0;
        }
        Ok(())
    }
}

// Complete all work that borrows the input within this call, so refilling needs no unsafe code.
fn process_row<R, O, F>(bytes: &[u8], f: &mut F, encoded: &mut Vec<u8>) -> Result<Option<usize>>
where
    R: RowRead,
    O: RowWrite,
    F: for<'input, 'row> FnMut(&'row R::Value<'input>) -> Result<O::Value<'row>>,
{
    let mut cursor = bytes;
    let row = match rowbinary::deserialize_row::<R::Value<'_>>(&mut cursor, None) {
        Ok(row) => row,
        Err(crate::error::Error::NotEnoughData) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let result = f(&row)?;
    encoded.clear();
    rowbinary::serialize_row_binary(&mut *encoded, &result)?;
    Ok(Some(bytes.len() - cursor.len()))
}

/// Executable-UDF chunk header enabled by `send_chunk_header`.
///
/// Encoded as an ASCII decimal row count followed by `\n`, independently of
/// the configured data format. Contains no byte lengths or schema information.
struct ChunkHeader {
    row_count: usize,
}

impl ChunkHeader {
    /// Read the next header, returning `None` only at EOF before any header bytes.
    fn from_chunk(buf: &mut impl BufRead) -> Result<Option<Self>> {
        let mut row_count = 0_usize;
        let mut has_digit = false;
        loop {
            let byte = match buf.fill_buf() {
                Ok([]) if !has_digit => return Ok(None),
                Ok([]) => return Err(unexpected_eof().into()),
                Ok(bytes) => bytes[0],
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
            };
            buf.consume(1);
            match byte {
                b'\n' if has_digit => return Ok(Some(Self { row_count })),
                b'0'..=b'9' => {
                    has_digit = true;
                    row_count = row_count
                        .checked_mul(10)
                        .and_then(|n| n.checked_add(usize::from(byte - b'0')))
                        .ok_or(Error::InvalidHeader("row count overflows usize"))?;
                }
                _ => {
                    return Err(Error::InvalidHeader(
                        "expected decimal row count and newline",
                    ));
                }
            }
        }
    }
}

struct Input<I> {
    reader: I,
    bytes: Vec<u8>,
    position: usize,
}

impl<I: BufRead> Input<I> {
    fn refill(&mut self) -> Result<()> {
        self.bytes.copy_within(self.position.., 0);
        self.bytes.truncate(self.bytes.len() - self.position);
        self.position = 0;
        loop {
            match self.reader.fill_buf() {
                Ok([]) => return Err(unexpected_eof().into()),
                Ok(available) => {
                    let count = available.len();
                    self.bytes.try_reserve(count)?;
                    self.bytes.extend_from_slice(available);
                    self.reader.consume(count);
                    return Ok(());
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }
}

fn unexpected_eof() -> io::Error {
    io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "incomplete UDF chunk header or row",
    )
}
