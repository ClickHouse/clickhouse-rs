//! Execution of `INSERT` statements.
//!
//! The data of `RowBinary`, `RowBinaryWithNames`, `RowBinaryWithNamesAndTypes` and `Native`
//! is converted to blocks and sent while it is written. The server cannot read data in other
//! formats from the native protocol, so it is collected and sent after the query text,
//! as inline data of the `INSERT` statement.

use crate::error::{Error, Result};
use crate::native::Block;
use crate::native::from_row_binary::RowBinaryColumns;
use crate::native::reader::{BlockReader, ChunkSource, WireFormat};
use crate::native::string::MaybeUtf8;
use crate::native::varuint::ParseVarUInt;
use crate::request_body::RequestBody;
use crate::tcp::connection::{Connection, ServerPacket};
use crate::tcp::pool::Pool;
use crate::tcp::query::{Failure, drain, send_query};
use crate::tcp::{Request, trailing_format};
use crate::{Client, Compression};
use bytes::{Buf, Bytes, BytesMut};
use clickhouse_types::error::TypesError;
use http_body_util::BodyExt;
use std::ops::ControlFlow;
use std::sync::Arc;
use tokio::task::JoinHandle;

/// The amount of `RowBinary` data that is sent as one block.
const BLOCK_SIZE: usize = 1 << 20;

/// The format of the data that the client writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Input {
    RowBinary {
        names: bool,
        types: bool,
    },
    Native,
    /// Any other format, sent as inline data.
    Inline,
}

impl Input {
    fn new(sql: &str) -> Self {
        match trailing_format(sql) {
            Some("RowBinary") => Self::RowBinary {
                names: false,
                types: false,
            },
            Some("RowBinaryWithNames") => Self::RowBinary {
                names: true,
                types: false,
            },
            Some("RowBinaryWithNamesAndTypes") => Self::RowBinary {
                names: true,
                types: true,
            },
            Some("Native") => Self::Native,
            _ => Self::Inline,
        }
    }
}

/// Start an `INSERT` that reads its data from `body`.
pub(crate) fn spawn_insert(
    client: &Client,
    sql: String,
    body: RequestBody,
) -> Result<JoinHandle<Result<()>>> {
    let request = Request::new(client)?;
    let pool = client.tcp.clone();

    Ok(tokio::spawn(run(pool, request, sql, body)))
}

async fn run(pool: Arc<Pool>, request: Request, sql: String, mut body: RequestBody) -> Result<()> {
    let input = Input::new(&sql);

    let query = if input == Input::Inline {
        // Collect the data before the connection is taken from the pool.
        let mut query = BytesMut::from(sql.as_bytes());
        query.extend_from_slice(b"\n");

        while let Some(chunk) = next_chunk(&mut body).await? {
            query.extend_from_slice(&chunk);
        }

        query.freeze()
    } else {
        Bytes::from(sql)
    };

    let mut pooled = pool.acquire(&request.key, request.session.clone()).await?;

    let result = match pooled.conn() {
        Ok(conn) => execute(conn, &request, &query, input, &mut body).await,
        Err(e) => Err(Failure::broken(e)),
    };

    match result {
        Ok(()) => {
            pooled.release(true);
            Ok(())
        }
        Err(failure) => {
            pooled.release(failure.reusable);
            Err(failure.error)
        }
    }
}

async fn execute(
    conn: &mut Connection,
    request: &Request,
    query: &[u8],
    input: Input,
    body: &mut RequestBody,
) -> Result<(), Failure> {
    let compression = send_query(conn, request, query).await?;
    let compressed = compression.is_enabled();

    if input == Input::Inline {
        drain(conn, compression, Some(query)).await?;
        return Ok(());
    }

    let header = loop {
        match conn
            .read_packet(compressed)
            .await
            .map_err(Failure::broken)?
        {
            ServerPacket::Data(block) => break block,
            ServerPacket::Exception(error) => {
                return Err(Failure {
                    error,
                    reusable: true,
                });
            }
            ServerPacket::EndOfStream => {
                return Err(Failure {
                    error: Error::BadResponse(
                        "the server did not request data for the INSERT statement".into(),
                    ),
                    reusable: true,
                });
            }
            _ => {}
        }
    };

    // An error in this phase leaves the server waiting for data, so the connection is closed.
    match input {
        Input::RowBinary { names, types } => {
            let mut decoder =
                RowBinaryInput::new(&header, names, types).map_err(Failure::broken)?;

            while let Some(chunk) = next_chunk(body).await.map_err(Failure::broken)? {
                decoder.push(&chunk);

                while let Some(block) = decoder.next_block(false).map_err(Failure::broken)? {
                    send_block(conn, compression, &block).await?;
                }
            }

            while let Some(block) = decoder.next_block(true).map_err(Failure::broken)? {
                send_block(conn, compression, &block).await?;
            }
        }
        Input::Native => {
            let mut reader = BlockReader::with_format(BodySource { body }, WireFormat::NATIVE);

            while let Some(block) = reader.read_block().await.map_err(Failure::broken)? {
                send_block(conn, compression, &block).await?;
            }
        }
        Input::Inline => {}
    }

    conn.write_data(None, compression)
        .map_err(Failure::broken)?;
    conn.flush().await.map_err(Failure::broken)?;

    drain(conn, compression, None).await?;
    Ok(())
}

async fn send_block(
    conn: &mut Connection,
    compression: Compression,
    block: &Block,
) -> Result<(), Failure> {
    if block.num_rows() == 0 {
        return Ok(());
    }

    conn.write_data(Some(block), compression)
        .map_err(Failure::broken)?;
    conn.flush().await.map_err(Failure::broken)
}

/// Returns the next chunk of data, `None` at the end, or an error if the `INSERT` is aborted.
async fn next_chunk(body: &mut RequestBody) -> Result<Option<Bytes>> {
    loop {
        match body.frame().await {
            None => return Ok(None),
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    return Ok(Some(data));
                }
            }
            Some(Err(e)) => return Err(Error::Network(e)),
        }
    }
}

struct BodySource<'a> {
    body: &'a mut RequestBody,
}

impl ChunkSource for BodySource<'_> {
    async fn next_chunk(&mut self) -> Option<Result<Bytes>> {
        next_chunk(self.body).await.transpose()
    }
}

/// Converts `RowBinary` data to blocks.
struct RowBinaryInput {
    columns: RowBinaryColumns,
    /// The names of the columns that the server expects.
    names: Vec<MaybeUtf8>,
    header: HeaderState,
    pending: BytesMut,
    /// The size of the rows in the current block.
    block_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HeaderState {
    Expected { types: bool },
    Done,
}

impl RowBinaryInput {
    fn new(header: &Block, names: bool, types: bool) -> Result<Self> {
        Ok(Self {
            columns: RowBinaryColumns::new(header)?,
            names: header
                .columns()
                .iter()
                .map(|column| MaybeUtf8::from_bytes(Bytes::copy_from_slice(column.name_bytes())))
                .collect(),
            header: if names {
                HeaderState::Expected { types }
            } else {
                HeaderState::Done
            },
            pending: BytesMut::new(),
            block_size: 0,
        })
    }

    fn push(&mut self, chunk: &[u8]) {
        self.pending.extend_from_slice(chunk);
    }

    /// Read rows from the pending data and return a block when it is large enough,
    /// or at the `end` of data.
    fn next_block(&mut self, end: bool) -> Result<Option<Block>> {
        if let HeaderState::Expected { types } = self.header {
            match self.read_header(types)? {
                ControlFlow::Continue(()) => self.header = HeaderState::Done,
                ControlFlow::Break(()) if end => {
                    return Err(Error::DataFormat("RowBinary header is incomplete".into()));
                }
                ControlFlow::Break(()) => return Ok(None),
            }
        }

        let mut input = &self.pending[..];

        while self.block_size < BLOCK_SIZE {
            let before = input.len();

            if !self.columns.read_row(&mut input)? {
                break;
            }

            self.block_size += before - input.len();
        }

        let consumed = self.pending.len() - input.len();
        self.pending.advance(consumed);

        if end && !self.pending.is_empty() && self.block_size < BLOCK_SIZE {
            return Err(Error::DataFormat(
                format!(
                    "{} bytes of RowBinary data at the end do not form a complete row",
                    self.pending.len()
                )
                .into(),
            ));
        }

        if self.columns.num_rows() > 0 && (end || self.block_size >= BLOCK_SIZE) {
            self.block_size = 0;
            return self.columns.take_block().map(Some);
        }

        Ok(None)
    }

    /// Read the `RowBinaryWithNames[AndTypes]` header and check the column names.
    fn read_header(&mut self, types: bool) -> Result<ControlFlow<()>> {
        let mut input = &self.pending[..];

        let names = if types {
            match clickhouse_types::parse_rbwnat_columns_header(&mut input) {
                Ok(columns) => columns.into_iter().map(|column| column.name).collect(),
                Err(TypesError::NotEnoughData(_)) => return Ok(ControlFlow::Break(())),
                Err(e) => return Err(Error::InvalidColumnsHeader(e.into())),
            }
        } else {
            match read_names(&mut input)? {
                ControlFlow::Continue(names) => names,
                ControlFlow::Break(()) => return Ok(ControlFlow::Break(())),
            }
        };

        let expected: Vec<&[u8]> = self.names.iter().map(MaybeUtf8::as_bytes).collect();
        let actual: Vec<&[u8]> = names.iter().map(String::as_bytes).collect();

        if expected != actual {
            return Err(Error::SchemaMismatch(format!(
                "the columns of the data {:?} do not match the columns of the INSERT {:?}",
                names,
                self.names
                    .iter()
                    .map(|name| String::from_utf8_lossy(name.as_bytes()))
                    .collect::<Vec<_>>()
            )));
        }

        let consumed = self.pending.len() - input.len();
        self.pending.advance(consumed);
        Ok(ControlFlow::Continue(()))
    }
}

/// Read the header of `RowBinaryWithNames`: the number of columns and their names.
fn read_names(input: &mut &[u8]) -> Result<ControlFlow<(), Vec<String>>> {
    let Some(count) = read_varuint(input)? else {
        return Ok(ControlFlow::Break(()));
    };

    let mut names = Vec::new();

    for _ in 0..count {
        let Some(len) = read_varuint(input)? else {
            return Ok(ControlFlow::Break(()));
        };

        let Some((name, rest)) = usize::try_from(len)
            .ok()
            .and_then(|len| input.split_at_checked(len))
        else {
            return Ok(ControlFlow::Break(()));
        };

        names.push(String::from_utf8_lossy(name).into_owned());
        *input = rest;
    }

    Ok(ControlFlow::Continue(names))
}

fn read_varuint(input: &mut &[u8]) -> Result<Option<u64>> {
    match ParseVarUInt::default().feed(&mut *input) {
        Ok(ControlFlow::Break(value)) => Ok(Some(value)),
        Ok(ControlFlow::Continue(())) => Ok(None),
        Err(e) => Err(Error::DataFormat(e.into())),
    }
}
