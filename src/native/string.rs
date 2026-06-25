use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Clone, PartialEq, Eq)]
pub struct MaybeUtf8(Repr);

#[derive(Clone, PartialEq, Eq)]
enum Repr {
    Utf8(Arc<str>),
    NotUtf8(Arc<[u8]>),
}

impl From<&[u8]> for MaybeUtf8 {
    fn from(value: &[u8]) -> Self {
        Self(
            str::from_utf8(value)
                .map_or_else(|_| Repr::NotUtf8(value.into()), |s| Repr::Utf8(s.into())),
        )
    }
}

impl MaybeUtf8 {
    pub fn as_bytes(&self) -> &[u8] {
        match &self.0 {
            Repr::Utf8(s) => s.as_bytes(),
            Repr::NotUtf8(s) => s,
        }
    }

    pub fn try_as_str(&self) -> Option<&str> {
        // The invariants of the type guarantee that we don't need to re-validate here
        match &self.0 {
            Repr::Utf8(s) => Some(s),
            Repr::NotUtf8(_) => None,
        }
    }
}

impl Hash for MaybeUtf8 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Produce the same hash as `&[u8]` or `&str`;
        // `impl Hash for str` adds a domain-separation postfix so its routine is separate.
        match &self.0 {
            Repr::Utf8(s) => s.hash(state),
            Repr::NotUtf8(s) => s.hash(state),
        }
    }
}

/// Compares lexicographically regardless of UTF-8-ness.
impl PartialOrd for MaybeUtf8 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.as_bytes().cmp(other.as_bytes()))
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
        match &self.0 {
            Repr::Utf8(s) => Debug::fmt(s, f),
            Repr::NotUtf8(s) => {
                write!(f, "\"{}\"", s.escape_ascii())
            }
        }
    }
}
