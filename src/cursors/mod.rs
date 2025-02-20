#[cfg(feature = "watch")]
pub(crate) use self::json::JsonCursor;
pub(crate) use self::raw::RawCursor;
pub use self::{bytes::BytesCursor, row::RowCursor};

mod bytes;
#[cfg(feature = "watch")]
mod json;
mod raw;
mod row;

// XXX: it was a workaround for https://github.com/rust-lang/rust/issues/51132,
//      but introduced #24 and must be fixed.
fn workaround_51132<'a, T: ?Sized>(ptr: &T) -> &'a T {
    // SAFETY: actually, it leads to unsoundness, see #24
    unsafe { &*(ptr as *const T) }
}
