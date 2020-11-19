use Compression::*;

#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub enum Compression {
    None,
    #[cfg(feature = "gzip")]
    Gzip,
    #[cfg(feature = "zlib")]
    Zlib,
    #[cfg(feature = "brotli")]
    Brotli,
}

impl Compression {
    pub fn encoding(&self) -> Option<&'static str> {
        match self {
            None => Option::None,
            #[cfg(feature = "gzip")]
            Gzip => Some("gzip"),
            #[cfg(feature = "zlib")]
            Zlib => Some("deflate"),
            #[cfg(feature = "brotli")]
            Brotli => Some("br"),
        }
    }
}
