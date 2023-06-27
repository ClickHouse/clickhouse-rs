use serde::Deserialize;
use serde::Serialize;
use serde::Serializer;
use serde::ser::SerializeStruct;

/// Wrapper type for a FixedString type in Clickhouse
/// Uses custom serializing handling in SerializeStruct impl for RowBinarySerializer
/// Forgoes the LEB128 encoding and just encodes the raw byte string
/// For deserializing the type FixedString(n) with a `query()`, wrap toString(...) around the value
/// For example:
/// 
/// CREATE TABLE test (
///     t1 String,
///     t2 FixedString(50)
/// ) ...
/// 
/// query("SELECT t1, toString(t2) FROM test;").fetch...
#[derive(Deserialize, Debug)]
pub struct FixedString {
    pub string: String
}

impl Serialize for FixedString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> 
    where 
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FixedString", 1)?;
        state.serialize_field("FixedString", &self.string)?;
        state.end()
    }
}

impl FixedString {
    pub fn new(string: String) -> Self {
        FixedString { string }
    }
}