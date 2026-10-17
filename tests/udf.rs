use std::cell::Cell;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::rc::Rc;

use clickhouse::Row;
use clickhouse::udf::{Error, Executor};
use serde::{Deserialize, Serialize};

#[derive(Row, Deserialize)]
struct Input<'a> {
    #[serde(borrow, with = "serde_bytes")]
    data: &'a [u8],
}

#[derive(Row, Serialize)]
struct Output<'a> {
    #[serde(with = "serde_bytes")]
    data: &'a [u8],
    length: u32,
    ordinal: u32,
}

#[derive(Default)]
struct Writer {
    bytes: Vec<u8>,
    flushes: usize,
    fail_write: bool,
    fail_flush: bool,
    gate: Option<Rc<Cell<usize>>>,
}

impl Write for Writer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.fail_write {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        // Exercise write_all's handling of partial writes too.
        let count = bytes.len().min(2);
        self.bytes.extend_from_slice(&bytes[..count]);
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.fail_flush {
            return Err(io::Error::other("flush failed"));
        }
        self.flushes += 1;
        if let Some(gate) = &self.gate {
            gate.set(self.flushes);
        }
        Ok(())
    }
}

fn lengths(input: impl BufRead) -> Result<Writer, Error> {
    let mut writer = Writer::default();
    Executor::new(input, &mut writer).run::<Input<'_>, u64, _>(|row| Ok(row.data.len() as u64))?;
    Ok(writer)
}

#[test]
fn consecutive_chunks_fragmented_rows_and_borrowed_results() {
    // Two rows, an empty chunk, then one row. Includes non-UTF-8 and NUL bytes.
    let chunks: Vec<&'static [u8]> = vec![b"2\n\x03a\0\xff\x00", b"0\n", b"1\n\x02bc"];
    let mut expected = Vec::new();
    for (data, ordinal) in [(b"a\0\xff".as_slice(), 1_u32), (b"", 2), (b"bc", 3)] {
        expected.push(data.len() as u8);
        expected.extend(data);
        expected.extend((data.len() as u32).to_le_bytes());
        expected.extend(ordinal.to_le_bytes());
    }

    for capacity in [1, 2, 3, 8, 8192] {
        let gate = Rc::new(Cell::new(0));
        let input = Requests {
            chunks: chunks.clone(),
            chunk: 0,
            position: 0,
            gate: gate.clone(),
        };
        let mut writer = Writer {
            gate: Some(gate),
            ..Writer::default()
        };
        let mut ordinal = 0;
        Executor::new(BufReader::with_capacity(capacity, input), &mut writer)
            .run::<Input<'_>, Output<'_>, _>(|row| {
                ordinal += 1;
                Ok(Output {
                    data: row.data,
                    length: row.data.len() as u32,
                    ordinal,
                })
            })
            .unwrap();
        assert_eq!(ordinal, 3);
        assert_eq!(writer.bytes, expected);
        assert_eq!(writer.flushes, 3);
    }
}

#[test]
fn matching_configuration_consumes_all_chunk_bytes() {
    #[derive(Row, Deserialize)]
    struct TwoStrings<'a> {
        first: &'a str,
        second: &'a str,
    }

    // One row with two String arguments, matched by two Rust fields.
    let mut writer = Writer::default();
    Executor::new(&b"1\n\x01a\x01b"[..], &mut writer)
        .run::<TwoStrings<'_>, u64, _>(|row| {
            assert_eq!((row.first, row.second), ("a", "b"));
            Ok((row.first.len() + row.second.len()) as u64)
        })
        .unwrap();
    assert_eq!(writer.bytes, 2_u64.to_le_bytes());
    assert_eq!(writer.flushes, 1);
}

#[test]
fn mismatched_configuration_rejects_remaining_chunk_bytes() {
    // The same two String arguments, but Input declares only one field.
    let mut writer = Writer::default();
    let error = Executor::new(&b"1\n\x01a\x01b"[..], &mut writer)
        .run::<Input<'_>, u64, _>(|row| Ok(row.data.len() as u64))
        .unwrap_err();
    assert!(matches!(error, Error::TrailingBytes(2)));
    assert_eq!(writer.flushes, 0);
}

#[test]
fn eof_and_empty_chunks() {
    let empty = lengths(&b""[..]).unwrap();
    assert!(empty.bytes.is_empty());
    assert_eq!(empty.flushes, 0);
    let empty_chunks = lengths(&b"0\n0\n"[..]).unwrap();
    assert!(empty_chunks.bytes.is_empty());
    assert_eq!(empty_chunks.flushes, 2);
}

#[test]
fn zero_width_input_is_counted_by_rows() {
    let mut output = Vec::new();
    Executor::new(&b"3\n0\n"[..], &mut output)
        .run::<(), u8, _>(|_| Ok(9))
        .unwrap();
    assert_eq!(output, [9, 9, 9]);
}

#[test]
fn malformed_headers_are_fatal() {
    for wire in [
        b"\n".as_slice(),
        b"-1\n",
        b"+1\n",
        b" 1\n",
        b"1 \n",
        b"1\r\n",
        b"1x\n",
        b"18446744073709551616\n",
    ] {
        assert!(
            matches!(lengths(wire), Err(Error::InvalidHeader(_))),
            "{wire:?}"
        );
    }
}

#[test]
fn eof_inside_headers_or_rows_is_an_error() {
    for wire in [
        b"1".as_slice(),
        b"1\n",
        b"1\n\x03ab",
        b"2\n\x01a",
        b"0\n1",
        b"1\n\x80",
    ] {
        assert!(
            matches!(lengths(wire), Err(Error::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof),
            "{wire:?}"
        );
    }
}

#[test]
fn malformed_row_is_not_retried_as_incomplete() {
    let mut wire = b"1\n".to_vec();
    wire.extend([0xff; 10]);
    assert!(matches!(lengths(wire.as_slice()), Err(Error::Codec(_))));
    let mut output = Vec::new();
    assert!(matches!(
        Executor::new(&b"1\n\x02"[..], &mut output)
            .run::<bool, u8, _>(|_| panic!("invalid boolean must not reach handler")),
        Err(Error::Codec(clickhouse::error::Error::InvalidTagEncoding(
            2
        )))
    ));
    assert!(output.is_empty());
}

#[test]
fn handler_and_serialization_failures_stop_processing() {
    let mut calls = 0;
    let mut output = Vec::new();
    let error = Executor::new(&b"2\n\x00\x00"[..], &mut output)
        .run::<Input<'_>, u8, _>(|_| {
            calls += 1;
            Err(Error::handler("application failure"))
        })
        .unwrap_err();
    assert!(matches!(error, Error::Handler(_)));
    assert_eq!(calls, 1);
    assert!(output.is_empty());

    #[derive(Row)]
    struct FailingOutput {
        _unused: u8,
    }
    impl Serialize for FailingOutput {
        fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("cannot encode result"))
        }
    }
    let error = Executor::new(&b"1\n\x00"[..], &mut output)
        .run::<Input<'_>, FailingOutput, _>(|_| Ok(FailingOutput { _unused: 0 }))
        .unwrap_err();
    assert!(matches!(
        error,
        Error::Codec(clickhouse::error::Error::Custom(_))
    ));
    assert!(output.is_empty());
}

#[test]
fn output_io_failures_are_propagated() {
    for fail_write in [true, false] {
        let mut writer = Writer {
            fail_write,
            fail_flush: !fail_write,
            ..Writer::default()
        };
        assert!(matches!(
            Executor::new(&b"1\n\x00"[..], &mut writer).run::<Input<'_>, u64, _>(|_| Ok(1)),
            Err(Error::Io(_))
        ));
    }
}

struct Interrupted<I> {
    inner: I,
    interrupt: bool,
}

impl<I: BufRead> Read for Interrupted<I> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.inner.read(bytes)
    }
}

impl<I: BufRead> BufRead for Interrupted<I> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.interrupt = !self.interrupt;
        if self.interrupt {
            return Err(io::ErrorKind::Interrupted.into());
        }
        self.inner.fill_buf()
    }
    fn consume(&mut self, count: usize) {
        self.inner.consume(count);
    }
}

#[test]
fn interrupted_header_and_body_reads_are_retried() {
    let input = Interrupted {
        inner: BufReader::with_capacity(1, &b"1\n\x03abc"[..]),
        interrupt: false,
    };
    assert_eq!(lengths(input).unwrap().bytes, 3_u64.to_le_bytes());
}

// Simulate a pooled caller that will only supply the next request after the response flushes.
struct Requests {
    chunks: Vec<&'static [u8]>,
    chunk: usize,
    position: usize,
    gate: Rc<Cell<usize>>,
}

impl Read for Requests {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let available = self.fill_buf()?;
        let count = available.len().min(bytes.len());
        bytes[..count].copy_from_slice(&available[..count]);
        self.consume(count);
        Ok(count)
    }
}

impl BufRead for Requests {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if self.chunk < self.chunks.len() && self.position == self.chunks[self.chunk].len() {
            self.chunk += 1;
            self.position = 0;
        }
        if self.chunk == self.chunks.len() {
            return Ok(&[]);
        }
        if self.gate.get() < self.chunk {
            return Err(io::Error::other("next request read before response flush"));
        }
        Ok(&self.chunks[self.chunk][self.position..])
    }
    fn consume(&mut self, count: usize) {
        self.position += count;
    }
}

#[test]
fn flushes_each_response_before_reading_the_next_request() {
    let gate = Rc::new(Cell::new(0));
    let input = Requests {
        chunks: vec![b"1\n\x01a", b"0\n", b"1\n\x02bc"],
        chunk: 0,
        position: 0,
        gate: gate.clone(),
    };
    let mut output = Writer {
        gate: Some(gate),
        ..Writer::default()
    };
    Executor::new(input, &mut output)
        .run::<Input<'_>, u64, _>(|row| Ok(row.data.len() as u64))
        .unwrap();
    assert_eq!(
        output.bytes,
        [1_u64.to_le_bytes(), 2_u64.to_le_bytes()].concat()
    );
    assert_eq!(output.flushes, 3);
}
