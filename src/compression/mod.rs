#[cfg(feature = "lz4")]
pub mod lz4;

#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub enum Compression {
    None,
    #[cfg(feature = "lz4")]
    Lz4,
    #[cfg(feature = "gzip")]
    Gzip,
    #[cfg(feature = "zlib")]
    Zlib,
    #[cfg(feature = "brotli")]
    Brotli,
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
    pub fn encoding(&self) -> Option<&'static str> {
        match self {
            Compression::None => Option::None,
            #[cfg(feature = "lz4")]
            Compression::Lz4 => Option::None,
            #[cfg(feature = "gzip")]
            Compression::Gzip => Some("gzip"),
            #[cfg(feature = "zlib")]
            Compression::Zlib => Some("deflate"),
            #[cfg(feature = "brotli")]
            Compression::Brotli => Some("br"),
        }
    }
}
