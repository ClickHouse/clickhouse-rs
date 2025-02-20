use bytes::{Bytes, BytesMut};

#[derive(Default)]
pub(crate) struct BytesExt {
    bytes: Bytes,
    cursor: usize,
}

impl BytesExt {
    #[inline(always)]
    pub(crate) fn slice(&self) -> &[u8] {
        &self.bytes[self.cursor..]
    }

    #[inline(always)]
    pub(crate) fn remaining(&self) -> usize {
        self.bytes.len() - self.cursor
    }

    #[inline(always)]
    pub(crate) fn is_empty(&self) -> bool {
        debug_assert!(self.cursor <= self.bytes.len());
        self.cursor >= self.bytes.len()
    }

    #[inline(always)]
    pub(crate) fn set_remaining(&mut self, n: usize) {
        // We can use `bytes.advance()` here, but it's slower.
        self.cursor = self.bytes.len() - n;
    }

    #[inline(always)]
    pub(crate) fn advance(&mut self, n: usize) {
        debug_assert!(n <= self.remaining());

        // We can use `bytes.advance()` here, but it's slower.
        self.cursor += n;
    }

    #[inline(always)]
    pub(crate) fn extend(&mut self, chunk: Bytes) {
        if self.is_empty() {
            // Most of the time, we read the next chunk after consuming the previous one.
            self.bytes = chunk;
            self.cursor = 0;
        } else {
            // Some bytes are left in the buffer, we need to merge them with the next chunk.
            self.extend_slow(chunk);
        }
    }

    #[cold]
    #[inline(never)]
    fn extend_slow(&mut self, chunk: Bytes) {
        let total = self.remaining() + chunk.len();
        let mut new_bytes = BytesMut::with_capacity(total);
        let capacity = new_bytes.capacity();
        new_bytes.extend_from_slice(self.slice());
        new_bytes.extend_from_slice(&chunk);
        debug_assert_eq!(new_bytes.capacity(), capacity);
        self.bytes = new_bytes.freeze();
        self.cursor = 0;
    }
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
