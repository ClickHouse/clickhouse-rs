#[cfg(feature = "lz4")]
pub(crate) mod lz4;

#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub enum Compression {
    None,
    #[cfg(feature = "lz4")]
    Lz4,
    #[cfg(feature = "lz4")]
    Lz4Hc(i32),
    #[cfg(feature = "lz4")]
    Lz4Fast(i32),
}

impl Default for Compression {
    #[cfg(feature = "lz4")]
    #[inline]
    fn default() -> Self {
        Compression::Lz4
    }

    #[cfg(not(feature = "lz4"))]
    #[inline]
    fn default() -> Self {
        Compression::None
    }
}

impl Compression {
    #[cfg(feature = "lz4")]
    pub(crate) fn is_lz4(&self) -> bool {
        *self != Compression::None
    }

    #[cfg(not(feature = "lz4"))]
    pub(crate) fn is_lz4(&self) -> bool {
        false
    }
}
