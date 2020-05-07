use bytes::BufMut;
use serde::{
    ser::{Impossible, SerializeSeq, SerializeStruct, SerializeTuple, Serializer},
    Serialize,
};

use crate::error::{Error, Result};

/// A serializer for the RowBinary format.
///
/// See https://clickhouse.yandex/docs/en/interfaces/formats/#rowbinary for details.
pub struct RowBinarySerializer<B> {
    buffer: B,
}

/// Serializes `value` using the RowBinary format and writes to `buffer`.
pub fn serialize_into(buffer: impl BufMut, value: &impl Serialize) -> Result<()> {
    let mut serializer = RowBinarySerializer { buffer };
    value.serialize(&mut serializer)?;
    Ok(())
}

impl<'a, B: BufMut> Serializer for &'a mut RowBinarySerializer<B> {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Impossible<(), Error>;
    type SerializeTupleVariant = Impossible<(), Error>;
    type SerializeMap = Impossible<(), Error>;
    type SerializeStruct = Self;
    type SerializeStructVariant = Impossible<(), Error>;

    #[inline]
    fn serialize_bool(self, _v: bool) -> Result<()> {
        todo!();
    }

    #[inline]
    fn serialize_i8(self, v: i8) -> Result<()> {
        self.buffer.put_i8(v);
        Ok(())
    }

    #[inline]
    fn serialize_i16(self, v: i16) -> Result<()> {
        self.buffer.put_i16_le(v);
        Ok(())
    }

    #[inline]
    fn serialize_i32(self, v: i32) -> Result<()> {
        self.buffer.put_i32_le(v);
        Ok(())
    }

    #[inline]
    fn serialize_i64(self, v: i64) -> Result<()> {
        self.buffer.put_i64_le(v);
        Ok(())
    }

    #[inline]
    fn serialize_u8(self, v: u8) -> Result<()> {
        self.buffer.put_u8(v);
        Ok(())
    }

    #[inline]
    fn serialize_u16(self, v: u16) -> Result<()> {
        self.buffer.put_u16_le(v);
        Ok(())
    }

    #[inline]
    fn serialize_u32(self, v: u32) -> Result<()> {
        self.buffer.put_u32_le(v);
        Ok(())
    }

    #[inline]
    fn serialize_u64(self, v: u64) -> Result<()> {
        self.buffer.put_u64_le(v);
        Ok(())
    }

    #[inline]
    fn serialize_f32(self, _v: f32) -> Result<()> {
        todo!();
    }

    #[inline]
    fn serialize_f64(self, _v: f64) -> Result<()> {
        todo!();
    }

    #[inline]
    fn serialize_char(self, _v: char) -> Result<()> {
        todo!();
    }

    #[inline]
    fn serialize_str(self, v: &str) -> Result<()> {
        put_unsigned_leb128(&mut self.buffer, v.len() as u64);
        self.buffer.put_slice(v.as_bytes());
        Ok(())
    }

    #[inline]
    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        put_unsigned_leb128(&mut self.buffer, v.len() as u64);
        self.buffer.put_slice(v);
        Ok(())
    }

    #[inline]
    fn serialize_none(self) -> Result<()> {
        self.buffer.put_u8(1);
        Ok(())
    }

    #[inline]
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<()> {
        self.buffer.put_u8(0);
        value.serialize(self)
    }

    #[inline]
    fn serialize_unit(self) -> Result<()> {
        todo!();
    }

    #[inline]
    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        todo!();
    }

    #[inline]
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<()> {
        todo!();
    }

    #[inline]
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<()> {
        value.serialize(self)
    }

    #[inline]
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<()> {
        todo!();
    }

    #[inline]
    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        let len = len.ok_or(Error::SequenceMustHaveLength)?;
        put_unsigned_leb128(&mut self.buffer, len as u64);
        Ok(self)
    }

    #[inline]
    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        Ok(self)
    }

    #[inline]
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        todo!();
    }

    #[inline]
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        todo!();
    }

    #[inline]
    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        todo!();
    }

    #[inline]
    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        Ok(self)
    }

    #[inline]
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        todo!();
    }
}

impl<'a, B: BufMut> SerializeStruct for &'a mut RowBinarySerializer<B> {
    type Ok = ();
    type Error = Error;

    #[inline]
    fn serialize_field<T: Serialize + ?Sized>(&mut self, _: &'static str, value: &T) -> Result<()> {
        value.serialize(&mut **self)
    }

    #[inline]
    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, B: BufMut> SerializeSeq for &'a mut RowBinarySerializer<B> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, B: BufMut> SerializeTuple for &'a mut RowBinarySerializer<B> {
    type Ok = ();
    type Error = Error;

    #[inline]
    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    #[inline]
    fn end(self) -> Result<()> {
        Ok(())
    }
}

fn put_unsigned_leb128(mut buffer: impl BufMut, mut value: u64) {
    while {
        let mut byte = value as u8 & 0x7f;
        value >>= 7;

        if value != 0 {
            byte |= 0x80;
        }

        buffer.put_u8(byte);

        value != 0
    } {}
}

#[test]
fn it_serializes_unsigned_leb128() {
    let mut vec = Vec::new();

    put_unsigned_leb128(&mut vec, 624_485);

    assert_eq!(vec, [0xe5, 0x8e, 0x26]);
}

#[test]
fn it_serializes() {
    #[derive(Serialize)]
    struct Timestamp(u32);

    #[derive(Serialize)]
    struct FixedPoint(i64);

    #[derive(Serialize)]
    struct Sample<'a> {
        int64: i64,
        int32: i32,
        int8: i8,
        uint64: u64,
        uint32: u32,
        uint8: u8,
        datetime: Timestamp,
        decimal: FixedPoint,
        string: &'a str,
        #[serde(serialize_with = "serialize_as_bytes")]
        bytes: &'a [u8],
        opt_fp: Option<FixedPoint>,
        opt_dt: Option<Timestamp>,
        fixed: [u8; 4],
        array: Vec<i8>,
    }

    let sample = Sample {
        int64: -6442,
        int32: -3242,
        int8: -42,
        uint64: 6442,
        uint32: 3242,
        uint8: 42,
        datetime: Timestamp(2_301_990_162),
        decimal: FixedPoint(4242 * 10_000_000),
        string: "01234",
        bytes: &[0, 1, 2, 3, 4],
        opt_fp: None,
        opt_dt: Some(Timestamp(2_301_990_162)),
        fixed: [b'B', b'T', b'C', 0],
        array: vec![-42, 42, -42, 42],
    };

    let expected = &[
        // [Int64] -6442
        0xd6, 0xe6, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, /**/
        // [Int32] -3242
        0x56, 0xf3, 0xff, 0xff, /**/
        // [Int8] -42
        0xd6, /**/
        // [UInt64] 6442
        0x2a, 0x19, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /**/
        // [UInt32] 3242
        0xaa, 0x0c, 0x00, 0x00, /**/
        // [UInt8] 42
        0x2a, /**/
        // [DateTime] 2042-12-12 12:42:42
        //       (ts: 2301990162)
        0x12, 0x95, 0x35, 0x89, /**/
        // [Decimal64(9)] 42.420000000
        0x00, 0xd5, 0x6d, 0xe0, 0x09, 0x00, 0x00, 0x00, /**/
        // [String] 5 "01234"
        0x05, 0x30, 0x31, 0x32, 0x33, 0x34, /**/
        // [String] 5 [0, 1, 2, 3, 4]
        0x05, 0x00, 0x01, 0x02, 0x03, 0x04, /**/
        // [Nullable(Decimal64(9))] NULL
        0x01, /**/
        // [Nullable(DateTime)] 2042-12-12 12:42:42
        //       (ts: 2301990162)
        0x00, 0x12, 0x95, 0x35, 0x89, /**/
        // [FixedString(4)] [b'B', b'T', b'C', 0]
        0x42, 0x54, 0x43, 0x00, /**/
        // [Array(Int32)] [-42, 42, -42, 42]
        0x04, 0xd6, 0x2a, 0xd6, 0x2a, /**/
    ][..];

    let mut actual = Vec::new();

    serialize_into(&mut actual, &sample).unwrap();

    assert_eq!(actual, expected);

    fn serialize_as_bytes<S: Serializer>(
        bytes: impl AsRef<[u8]>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(bytes.as_ref())
    }
}
