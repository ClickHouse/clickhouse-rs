use crate::sql;

pub trait Row {
    const COLUMN_NAMES: &'static [&'static str];

    // TODO: count
    // TODO: different list for SELECT/INSERT (de/ser)
}

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

macro_rules! impl_row_for_tuple {
    ($i:ident $($other:ident)+) => {
        /// Two forms are supported:
        /// * (P1, P2, ...)
        /// * (SomeRow, P1, P2, ...)
        ///
        /// The second one is useful for queries like
        /// `SELECT ?fields, count() FROM .. GROUP BY ?fields`.
        impl<$i: Row, $($other: Primitive),+> Row for ($i, $($other),+) {
            const COLUMN_NAMES: &'static [&'static str] = $i::COLUMN_NAMES;
        }

        impl_row_for_tuple!($($other)+);
    };
    ($i:ident) => {};
}

// TODO: revise this?
impl Primitive for () {}

impl<P: Primitive> Row for P {
    const COLUMN_NAMES: &'static [&'static str] = &[];
}

impl_row_for_tuple!(T0 T1 T2 T3 T4 T5 T6 T7 T8);

impl<T> Row for Vec<T> {
    const COLUMN_NAMES: &'static [&'static str] = &[];
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
