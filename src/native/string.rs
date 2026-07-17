use bytes::Bytes;
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};

#[derive(Clone, PartialEq, Eq)]
pub struct MaybeUtf8 {
    bytes: Bytes,
    // SAFETY: must not change for the lifetime of this object
    is_utf8: bool,
}

impl MaybeUtf8 {
    pub fn new<T: Into<Bytes>>(bytes: T) -> Self {
        Self::from(bytes.into())
    }
}

impl From<Bytes> for MaybeUtf8 {
    fn from(bytes: Bytes) -> Self {
        Self {
            is_utf8: str::from_utf8(&bytes).is_ok(),
            bytes,
        }
    }
}

impl From<String> for MaybeUtf8 {
    fn from(value: String) -> Self {
        Self {
            bytes: value.into(),
            is_utf8: true,
        }
    }
}

impl From<&'static str> for MaybeUtf8 {
    fn from(value: &'static str) -> Self {
        Self {
            bytes: value.into(),
            is_utf8: true,
        }
    }
}

impl MaybeUtf8 {
    pub fn is_utf8(&self) -> bool {
        self.is_utf8
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn try_as_str(&self) -> Option<&str> {
        // SAFETY: UTF-8 validity is checked on construction
        self.is_utf8
            .then(|| unsafe { str::from_utf8_unchecked(&self.bytes) })
    }
}

impl Hash for MaybeUtf8 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Produce the same hash as `&[u8]` or `&str`;
        // `impl Hash for str` adds a domain-separation postfix so its routine is separate.
        match self.try_as_str() {
            Some(s) => s.hash(state),
            None => self.bytes.hash(state),
        }
    }
}

/// Compares lexicographically regardless of UTF-8-ness.
impl PartialOrd for MaybeUtf8 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialOrd<str> for MaybeUtf8 {
    fn partial_cmp(&self, other: &str) -> Option<Ordering> {
        Some(self.as_bytes().cmp(other.as_bytes()))
    }
}

impl PartialOrd<[u8]> for MaybeUtf8 {
    fn partial_cmp(&self, other: &[u8]) -> Option<Ordering> {
        Some(self.as_bytes().cmp(other))
    }
}

impl Ord for MaybeUtf8 {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_bytes().cmp(other.as_bytes())
    }
}

impl PartialEq<str> for MaybeUtf8 {
    fn eq(&self, other: &str) -> bool {
        self.try_as_str().is_some_and(|s| s == other)
    }
}

impl PartialEq<[u8]> for MaybeUtf8 {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_bytes().eq(other)
    }
}

impl Debug for MaybeUtf8 {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.try_as_str() {
            Some(s) => Debug::fmt(s, f),
            None => write!(f, "\"{}\"", self.bytes.escape_ascii()),
        }
    }
}

impl hashbrown::Equivalent<MaybeUtf8> for str {
    fn equivalent(&self, key: &MaybeUtf8) -> bool {
        key == self
    }
}
