use serde::de::Error;
use serde::{Deserialize, Deserializer};

#[derive(Debug, serde::Deserialize)]
pub(crate) struct Summary {
    #[serde(deserialize_with = "int_in_string")]
    pub read_rows: Option<usize>,
    #[serde(deserialize_with = "int_in_string")]
    pub read_bytes: Option<usize>,
    #[serde(deserialize_with = "int_in_string")]
    pub written_rows: Option<usize>,
    #[serde(deserialize_with = "int_in_string")]
    pub written_bytes: Option<usize>,
    #[serde(deserialize_with = "int_in_string")]
    pub total_rows_to_read: Option<usize>,
    #[serde(deserialize_with = "int_in_string")]
    pub result_rows: Option<usize>,
    #[serde(deserialize_with = "int_in_string")]
    pub result_bytes: Option<usize>,
    #[serde(deserialize_with = "int_in_string")]
    pub elapsed_ns: Option<usize>,
}

fn int_in_string<'de, D>(deser: D) -> Result<Option<usize>, D::Error>
where
    D: Deserializer<'de>,
{
    if let Some(string) = Option::<&str>::deserialize(deser)? {
        let value: usize = string
            .parse()
            .map_err(|_| D::Error::custom("invalid integer"))?;
        Ok(Some(value))
    } else {
        Ok(None)
    }
}
