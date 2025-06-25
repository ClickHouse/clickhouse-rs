use bytes::{Bytes, BytesMut};
use std::cell::{Cell, UnsafeCell};

#[derive(Default)]
pub(crate) struct BytesExt {
    bytes: UnsafeCell<Bytes>,
    cursor: Cell<usize>,
}

impl BytesExt {
    #[inline(always)]
    pub(crate) fn slice(&self) -> &[u8] {
        &self.bytes()[self.cursor.get()..]
    }

    #[inline(always)]
    pub(crate) fn remaining(&self) -> usize {
        self.bytes().len() - self.cursor.get()
    }

    #[inline(always)]
    pub(crate) fn set_remaining(&self, n: usize) {
        // We can use `bytes.advance()` here, but it's slower.
        self.cursor.set(self.bytes().len() - n);
    }

    #[cfg(any(test, feature = "lz4"))]
    #[inline(always)]
    pub(crate) fn advance(&mut self, n: usize) {
        debug_assert!(n <= self.remaining());

        // We can use `bytes.advance()` here, but it's slower.
        self.cursor.set(self.cursor.get() + n);
    }

    #[inline(always)]
    pub(crate) fn extend(&mut self, chunk: Bytes) {
        *self.bytes.get_mut() = merge_bytes(self.slice(), chunk);
        self.cursor.set(0);
    }

    #[inline(always)]
    pub(crate) unsafe fn extend_by_ref(&self, chunk: Bytes) {
        let bytes = &mut *self.bytes.get();
        *bytes = merge_bytes(self.slice(), chunk);
        self.cursor.set(0);
    }

    fn bytes(&self) -> &Bytes {
        unsafe { &*self.bytes.get() }
    }
}

fn merge_bytes(lhs: &[u8], rhs: Bytes) -> Bytes {
    if lhs.is_empty() {
        // Most of the time, we read the next chunk after consuming the previous one.
        rhs
    } else {
        // Some bytes are left in the buffer, we need to merge them with the next chunk.
        merge_bytes_slow(lhs, rhs)
    }
}

#[cold]
#[inline(never)]
fn merge_bytes_slow(lhs: &[u8], rhs: Bytes) -> Bytes {
    let new_len = lhs.len() + rhs.len();
    let mut new_bytes = BytesMut::with_capacity(new_len);
    let capacity = new_bytes.capacity();
    new_bytes.extend_from_slice(lhs);
    new_bytes.extend_from_slice(&rhs);
    debug_assert_eq!(new_bytes.capacity(), capacity);
    new_bytes.freeze()
}

#[test]
fn it_works() {
    let mut bytes = BytesExt::default();
    assert!(bytes.slice().is_empty());
    assert_eq!(bytes.remaining(), 0);

    bytes.extend(Bytes::from_static(b"hello"));
    assert_eq!(bytes.slice(), b"hello");
    assert_eq!(bytes.remaining(), 5);

    bytes.advance(3);
    assert_eq!(bytes.slice(), b"lo");
    assert_eq!(bytes.remaining(), 2);

    bytes.extend(Bytes::from_static(b"l"));
    assert_eq!(bytes.slice(), b"lol");
    assert_eq!(bytes.remaining(), 3);

    bytes.set_remaining(1);
    assert_eq!(bytes.slice(), b"l");
    assert_eq!(bytes.remaining(), 1);
}
