pub(crate) use de::deserialize_from;
pub(crate) use ser::serialize_into;
pub(crate) use validation::StructMetadata;

mod de;
mod ser;
#[cfg(test)]
mod tests;
mod utils;
mod validation;
