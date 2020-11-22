pub mod lz4;

#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub enum Compression {
    None,
    Lz4,
    #[cfg(feature = "gzip")]
    Gzip,
    #[cfg(feature = "zlib")]
    Zlib,
    #[cfg(feature = "brotli")]
    Brotli,
}

impl Default for Compression {
    #[inline]
    fn default() -> Self {
        Compression::Lz4
    }
}

impl Compression {
    pub fn encoding(&self) -> Option<&'static str> {
        match self {
            Compression::None => Option::None,
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
