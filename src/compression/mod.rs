#[cfg(feature = "lz4")]
pub(crate) mod lz4;

#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub enum Compression {
    None,
    #[cfg(feature = "lz4")]
    Lz4,
    /// High compression levels are useful in networks with low bandwidth.
    /// Affects only INSERTs, because others are compressed by the server.
    /// Possible levels: [1, 12]. Recommended level range: [4, 9].
    #[cfg(feature = "lz4")]
    Lz4Hc(i32),
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
    pub(crate) fn is_lz4(&self) -> bool {
        *self != Compression::None
    }
}
