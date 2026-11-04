//! An executable UDF returning the byte length of each input String.
//!
//! Configure format=RowBinary, send_chunk_header=true, one Clickhouse String argument,
//! and return_type=UInt64. Input fields must follow the SQL argument order.
//!
//! Build with `cargo build --release --example udf` for the server's OS and
//! architecture, and copy the resulting `udf` binary into its `user_scripts_path`.
//! Load this XML through `user_defined_executable_functions_config`:
//!
//! ```xml
//! <functions>
//!     <function>
//!         <type>executable_pool</type>
//!         <name>rust_byte_length</name>
//!         <command>udf</command>
//!         <argument>
//!             <name>data</name>
//!             <type>String</type>
//!         </argument>
//!         <return_type>UInt64</return_type>
//!         <format>RowBinary</format>
//!         <send_chunk_header>true</send_chunk_header>
//!     </function>
//! </functions>
//! ```
//!
//! ```sql
//! SELECT rust_byte_length('hello'); -- 5
//! ```

use clickhouse::{Row, udf};
use serde::Deserialize;

#[derive(Row, Deserialize)]
struct Input<'a> {
    #[serde(borrow, with = "serde_bytes")]
    data: &'a [u8],
}

fn main() -> udf::Result<()> {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    udf::Executor::new(stdin.lock(), stdout.lock())
        .run::<Input<'_>, u64, _>(|row| Ok(row.data.len() as u64))
}
