//! Extension traits for reading/writing ClickHouse native wire protocol primitives.
//!
//! Provides VarUInt and length-prefixed string encoding used by the native TCP protocol.

use std::io::IoSlice;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::{Error, Result};
use crate::native::protocol::MAX_STRING_SIZE;

/// Extension trait on AsyncRead for ClickHouse wire protocol.
pub(crate) trait ClickHouseRead: AsyncRead + Unpin + Send + Sync {
    fn read_var_uint(&mut self) -> impl Future<Output = Result<u64>> + Send + '_;

    fn read_string(&mut self) -> impl Future<Output = Result<Vec<u8>>> + Send + '_;

    fn read_utf8_string(&mut self) -> impl Future<Output = Result<String>> + Send + '_ {
        async {
            let bytes = self.read_string().await?;
            String::from_utf8(bytes)
                .map_err(|e| Error::BadResponse(format!("native protocol: invalid utf8: {e}")))
        }
    }
}

impl<T: AsyncRead + Unpin + Send + Sync> ClickHouseRead for T {
    async fn read_var_uint(&mut self) -> Result<u64> {
        let mut out = 0u64;
        for i in 0..9u64 {
            let mut octet = [0u8];
            self.read_exact(&mut octet[..]).await?;
            out |= u64::from(octet[0] & 0x7F) << (7 * i);
            if (octet[0] & 0x80) == 0 {
                break;
            }
        }
        Ok(out)
    }

    async fn read_string(&mut self) -> Result<Vec<u8>> {
        #[allow(clippy::cast_possible_truncation)]
        let len = self.read_var_uint().await? as usize;
        if len > MAX_STRING_SIZE {
            return Err(Error::BadResponse(format!(
                "native protocol: string too large: {len} > {MAX_STRING_SIZE}"
            )));
        }
        if len == 0 {
            return Ok(vec![]);
        }
        let mut buf = vec![0u8; len];
        self.read_exact(&mut buf).await?;
        Ok(buf)
    }
}

/// Extension trait on AsyncWrite for ClickHouse wire protocol.
pub(crate) trait ClickHouseWrite: AsyncWrite + Unpin + Send + Sync {
    fn write_var_uint(&mut self, value: u64) -> impl Future<Output = Result<()>> + Send + '_;

    fn write_string<V: AsRef<[u8]> + Send>(
        &mut self,
        value: V,
    ) -> impl Future<Output = Result<()>> + Send + use<'_, Self, V>;

    /// Write multiple buffers in one syscall (vectored I/O).
    fn write_vectored_all<'a>(
        &'a mut self,
        bufs: &'a mut [IoSlice<'a>],
    ) -> impl Future<Output = Result<()>> + Send + 'a;
}

impl<T: AsyncWrite + Unpin + Send + Sync> ClickHouseWrite for T {
    async fn write_var_uint(&mut self, mut value: u64) -> Result<()> {
        let mut buf = [0u8; 9]; // Max 9 bytes for u64
        let mut pos = 0;

        #[allow(clippy::cast_possible_truncation)]
        while pos < 9 {
            let mut byte = value & 0x7F;
            value >>= 7;
            if value > 0 {
                byte |= 0x80;
            }
            buf[pos] = byte as u8;
            pos += 1;
            if value == 0 {
                break;
            }
        }
        self.write_all(&buf[..pos]).await?;
        Ok(())
    }

    async fn write_string<V: AsRef<[u8]> + Send>(&mut self, value: V) -> Result<()> {
        let value = value.as_ref();
        self.write_var_uint(value.len() as u64).await?;
        self.write_all(value).await?;
        Ok(())
    }

    async fn write_vectored_all<'a>(&'a mut self, bufs: &'a mut [IoSlice<'a>]) -> Result<()> {
        let total: usize = bufs.iter().map(|b| b.len()).sum();
        if total == 0 {
            return Ok(());
        }

        let mut written = 0usize;
        while written < total {
            let mut remaining_bufs: Vec<IoSlice<'_>> =
                bufs.iter().skip_while(|b| b.is_empty()).map(|b| IoSlice::new(b)).collect();

            if remaining_bufs.is_empty() {
                break;
            }

            let mut to_skip = written;
            for buf in &mut remaining_bufs {
                if to_skip == 0 {
                    break;
                }
                let buf_len = buf.len();
                if to_skip >= buf_len {
                    to_skip -= buf_len;
                    *buf = IoSlice::new(&[]);
                } else {
                    break;
                }
            }

            let active_bufs: Vec<IoSlice<'_>> =
                remaining_bufs.into_iter().filter(|b| !b.is_empty()).collect();

            if active_bufs.is_empty() {
                break;
            }

            match self.write_vectored(&active_bufs).await {
                Ok(0) => {
                    return Err(Error::Network(Box::new(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "write_vectored returned 0",
                    ))));
                }
                Ok(n) => written += n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e.into()),
            }
        }

        Ok(())
    }
}

/// Sync extension trait on `bytes::Buf` for ClickHouse wire protocol.
pub(crate) trait ClickHouseBytesRead: bytes::Buf {
    fn try_get_var_uint(&mut self) -> Result<u64>;
    fn try_get_string(&mut self) -> Result<bytes::Bytes>;
}

impl<T: bytes::Buf> ClickHouseBytesRead for T {
    #[inline]
    fn try_get_var_uint(&mut self) -> Result<u64> {
        if !self.has_remaining() {
            return Err(Error::NotEnoughData);
        }
        let b = self.get_u8();
        let mut out = u64::from(b & 0x7F);
        if (b & 0x80) == 0 {
            return Ok(out);
        }

        for i in 1..9 {
            if !self.has_remaining() {
                return Err(Error::NotEnoughData);
            }
            let b = self.get_u8();
            out |= u64::from(b & 0x7F) << (7 * i);
            if (b & 0x80) == 0 {
                return Ok(out);
            }
        }

        Ok(out)
    }

    #[inline]
    fn try_get_string(&mut self) -> Result<bytes::Bytes> {
        #[allow(clippy::cast_possible_truncation)]
        let len = self.try_get_var_uint()? as usize;

        if len > MAX_STRING_SIZE {
            return Err(Error::BadResponse(format!(
                "native protocol: string too large: {len}"
            )));
        }

        if len == 0 {
            return Ok(bytes::Bytes::new());
        }

        if self.remaining() < len {
            return Err(Error::NotEnoughData);
        }

        Ok(self.copy_to_bytes(len))
    }
}

/// Sync extension trait on `bytes::BufMut` for ClickHouse wire protocol.
pub(crate) trait ClickHouseBytesWrite: bytes::BufMut {
    fn put_var_uint(&mut self, value: u64);
    fn put_string<V: AsRef<[u8]>>(&mut self, value: V);
}

impl<T: bytes::BufMut> ClickHouseBytesWrite for T {
    fn put_var_uint(&mut self, mut value: u64) {
        let mut buf = [0u8; 9];
        let mut pos = 0;

        #[allow(clippy::cast_possible_truncation)]
        while pos < 9 {
            let mut byte = value & 0x7F;
            value >>= 7;
            if value > 0 {
                byte |= 0x80;
            }
            buf[pos] = byte as u8;
            pos += 1;
            if value == 0 {
                break;
            }
        }

        self.put_slice(&buf[..pos]);
    }

    fn put_string<V: AsRef<[u8]>>(&mut self, value: V) {
        let value = value.as_ref();
        self.put_var_uint(value.len() as u64);
        self.put_slice(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Bytes, BytesMut};

    #[test]
    fn test_var_uint_roundtrip_sync() {
        // Note: ClickHouse varint uses 7 bits × 9 bytes = 63 bits max
        let test_values: &[u64] = &[0, 1, 127, 128, 255, 256, 16383, 16384, (1 << 63) - 1];
        for &val in test_values {
            let mut buf = BytesMut::new();
            buf.put_var_uint(val);
            let mut reader = buf.freeze();
            let decoded = reader.try_get_var_uint().unwrap();
            assert_eq!(val, decoded, "roundtrip failed for {val}");
        }
    }

    #[test]
    fn test_string_roundtrip_sync() {
        let test_strings: &[&[u8]] = &[b"", b"hello", b"hello world", &[0u8; 1000]];
        for &val in test_strings {
            let mut buf = BytesMut::new();
            buf.put_string(val);
            let mut reader = buf.freeze();
            let decoded = reader.try_get_string().unwrap();
            assert_eq!(val, &decoded[..], "roundtrip failed");
        }
    }

    #[tokio::test]
    async fn test_var_uint_roundtrip_async() {
        let test_values: &[u64] = &[0, 1, 127, 128, 255, 256, 16383, 16384, (1 << 63) - 1];
        for &val in test_values {
            let mut buf = Vec::new();
            buf.write_var_uint(val).await.unwrap();
            let mut reader = std::io::Cursor::new(buf);
            let decoded = reader.read_var_uint().await.unwrap();
            assert_eq!(val, decoded, "async roundtrip failed for {val}");
        }
    }

    #[tokio::test]
    async fn test_string_roundtrip_async() {
        let test_strings: &[&[u8]] = &[b"", b"hello", b"hello world"];
        for &val in test_strings {
            let mut buf = Vec::new();
            buf.write_string(val).await.unwrap();
            let mut reader = std::io::Cursor::new(buf);
            let decoded = reader.read_string().await.unwrap();
            assert_eq!(val, &decoded[..], "async roundtrip failed");
        }
    }

    #[test]
    fn test_eof_returns_error() {
        let mut buf = Bytes::new();
        let result = buf.try_get_var_uint();
        assert!(result.is_err());
    }
}
