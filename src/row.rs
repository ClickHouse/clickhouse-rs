use crate::sql;
use serde::Deserialize;

#[derive(Debug, Clone, PartialEq)]
pub enum RowKind {
    Primitive,
    Struct,
    Tuple,
    Vec,
}

/// Represents a row that can be used in queries.
///
/// Implemented for:
/// * All `#[derive(Row)]` items
/// * `(P1, P2, ...)` where P* is a primitive type or string
///
/// Do not implement this trait directly, use `#[derive(Row)]` instead.
///
/// In order to write a generic code over rows, see `ReadRow`.
pub trait Row {
    // NOTE: all properties are unstable and, hence, not following semver.

    #[doc(hidden)]
    const NAME: &'static str;
    // TODO: different list for SELECT/INSERT (de/ser)
    #[doc(hidden)]
    const COLUMN_NAMES: &'static [&'static str];
    #[doc(hidden)]
    const COLUMN_COUNT: usize;
    #[doc(hidden)]
    const KIND: RowKind;
    #[doc(hidden)]
    type Value<'a>: Row;
}

/// Represents a row that can be read from the database.
///
/// This trait is implemented automatically and useful for writing generic code.
pub trait ReadRow: for<'a> Row<Value<'a>: Deserialize<'a>> {}
impl<R> ReadRow for R where R: for<'a> Row<Value<'a>: Deserialize<'a>> {}

/// Represents a row not holding any references.
///
/// This trait is implemented automatically and useful for writing generic code.
pub trait RowOwned: 'static + for<'a> Row<Value<'a> = Self> {}
impl<R> RowOwned for R where R: 'static + for<'a> Row<Value<'a> = R> {}

// Actually, it's not public now.
pub trait Primitive {}

macro_rules! impl_primitive_for {
    ($t:ty, $($other:tt)*) => {
        impl Primitive for $t {}
        impl_primitive_for!($($other)*);
    };
    () => {};
}

// TODO: char? &str? SocketAddr? Path? Duration? NonZero*?
impl_primitive_for![
    bool, String, u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, f32, f64,
];

macro_rules! count_tokens {
    () => { 0 };
    ($head:tt $($tail:tt)*) => { 1 + count_tokens!($($tail)*) };
}

/// Two forms are supported:
/// * (P1, P2, ...)
/// * (SomeRow, P1, P2, ...)
///
/// The second one is useful for queries like
/// `SELECT ?fields, count() FROM ... GROUP BY ?fields`.
macro_rules! impl_row_for_tuple {
    ($i:ident $($other:ident)+) => {
        impl<$i: Row, $($other: Primitive),+> Row for ($i, $($other),+) {
            const NAME: &'static str = $i::NAME;
            const COLUMN_NAMES: &'static [&'static str] = $i::COLUMN_NAMES;
            const COLUMN_COUNT: usize = $i::COLUMN_COUNT + count_tokens!($($other)*);
            const KIND: RowKind = RowKind::Tuple;

            type Value<'a> = Self;
        }

        impl_row_for_tuple!($($other)+);
    };
    ($i:ident) => {};
}

// TODO: revise this?
impl Primitive for () {}

impl<P: Primitive> Row for P {
    const NAME: &'static str = stringify!(P);
    const COLUMN_NAMES: &'static [&'static str] = &[];
    const COLUMN_COUNT: usize = 1;
    const KIND: RowKind = RowKind::Primitive;

    type Value<'a> = Self;
}

impl_row_for_tuple!(T0 T1 T2 T3 T4 T5 T6 T7 T8);

impl<T> Row for Vec<T> {
    const NAME: &'static str = "Vec";
    const COLUMN_NAMES: &'static [&'static str] = &[];
    const COLUMN_COUNT: usize = 1;
    const KIND: RowKind = RowKind::Vec;

    type Value<'a> = Self;
}

/// Collects all field names in depth and joins them with comma.
pub(crate) fn join_column_names<R: Row>() -> Option<String> {
    if R::COLUMN_NAMES.is_empty() {
        return None;
    }

    let out = R::COLUMN_NAMES
        .iter()
        .enumerate()
        .fold(String::new(), |mut res, (idx, name)| {
            if idx > 0 {
                res.push(',');
            }
            sql::escape::identifier(name, &mut res).expect("impossible");
            res
        });

    Some(out)
}

#[cfg(test)]
mod tests {
    // XXX: need for `derive(Row)`. Provide `row(crate = ..)` instead.
    use crate as clickhouse;
    use clickhouse::Row;

    use super::*;

    #[test]
    fn it_grabs_simple_struct() {
        #[derive(Row)]
        #[allow(dead_code)]
        struct Simple1 {
            one: u32,
        }

        #[derive(Row)]
        #[allow(dead_code)]
        struct Simple2 {
            one: u32,
            two: u32,
        }

        assert_eq!(join_column_names::<Simple1>().unwrap(), "`one`");
        assert_eq!(join_column_names::<Simple2>().unwrap(), "`one`,`two`");
    }

    #[test]
    fn it_grabs_mix() {
        #[derive(Row)]
        struct SomeRow {
            _a: u32,
        }

        assert_eq!(join_column_names::<(SomeRow, u32)>().unwrap(), "`_a`");
    }

    #[test]
    fn it_supports_renaming() {
        use serde::Serialize;

        #[derive(Row, Serialize)]
        #[allow(dead_code)]
        struct TopLevel {
            #[serde(rename = "two")]
            one: u32,
        }

        assert_eq!(join_column_names::<TopLevel>().unwrap(), "`two`");
    }

    #[test]
    fn it_skips_serializing() {
        use serde::Serialize;

        #[derive(Row, Serialize)]
        #[allow(dead_code)]
        struct TopLevel {
            one: u32,
            #[serde(skip_serializing)]
            two: u32,
        }

        assert_eq!(join_column_names::<TopLevel>().unwrap(), "`one`");
    }

    #[test]
    fn it_skips_deserializing() {
        use serde::Deserialize;

        #[derive(Row, Deserialize)]
        #[allow(dead_code)]
        struct TopLevel {
            one: u32,
            #[serde(skip_deserializing)]
            two: u32,
        }

        assert_eq!(join_column_names::<TopLevel>().unwrap(), "`one`");
    }

    #[test]
    fn it_rejects_other() {
        #[allow(dead_code)]
        #[derive(Row)]
        struct NamedTuple(u32, u32);

        assert_eq!(join_column_names::<u32>(), None);
        assert_eq!(join_column_names::<(u32, u64)>(), None);
        assert_eq!(join_column_names::<NamedTuple>(), None);
    }

    #[test]
    fn it_handles_raw_identifiers() {
        use serde::Serialize;

        #[derive(Row, Serialize)]
        #[allow(dead_code)]
        struct MyRow {
            r#type: u32,
            #[serde(rename = "if")]
            r#match: u32,
        }

        assert_eq!(join_column_names::<MyRow>().unwrap(), "`type`,`if`");
    }
}
