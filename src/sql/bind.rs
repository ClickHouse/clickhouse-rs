use std::fmt;

use sealed::sealed;
use serde::Serialize;

use super::{escape, ser};

#[sealed]
pub trait Bind {
    #[doc(hidden)]
    fn write(&self, dst: &mut impl fmt::Write) -> Result<(), String>;
}

#[sealed]
impl<S: Serialize> Bind for S {
    #[inline]
    fn write(&self, mut dst: &mut impl fmt::Write) -> Result<(), String> {
        ser::write_arg(&mut dst, self)
    }
}

/// Bound the provided string as an identifier.
/// It can be used for table names, for instance.
#[derive(Copy, Clone)]
pub struct Identifier<'a>(pub &'a str);

#[sealed]
impl Bind for Identifier<'_> {
    #[inline]
    fn write(&self, dst: &mut impl fmt::Write) -> Result<(), String> {
        escape::identifier(self.0, dst).map_err(|err| err.to_string())
    }
}

/// A variant on `Identifier` which supports qualifying an identifier. For example,
/// `QualifiedIdentifier("foo", "bar")` will emit the SQL `\`foo\`.\`bar\``.
#[derive(Clone, Copy)]
pub struct QualifiedIdentifier<'a>(pub &'a str, pub &'a str);

#[sealed]
impl Bind for QualifiedIdentifier<'_> {
    #[inline]
    fn write(&self, dst: &mut impl fmt::Write) -> Result<(), String> {
        if !self.0.is_empty() {
            escape::identifier(self.0, dst).map_err(|err| err.to_string())?;
            dst.write_char('.').map_err(|err| err.to_string())?;
        }
        escape::identifier(self.1, dst).map_err(|err| err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::{Bind, QualifiedIdentifier};

    fn bind_to_string(b: impl Bind) -> String {
        let mut s = String::new();
        b.write(&mut s).expect("bind should succeed");
        s
    }

    #[test]
    fn test_qualified_identifier() {
        assert_eq!(
            bind_to_string(QualifiedIdentifier("foo", "bar baz")),
            "`foo`.`bar baz`"
        );
        assert_eq!(
            bind_to_string(QualifiedIdentifier("", "bar baz")),
            "`bar baz`"
        );

        assert_eq!(
            bind_to_string(QualifiedIdentifier("`'.", ".................````")),
            "`\\`\\'.`.`.................\\`\\`\\`\\``"
        );

        assert_eq!(
            bind_to_string(QualifiedIdentifier("クリック", "ハウス")),
            "`クリック`.`ハウス`"
        );

        assert_eq!(bind_to_string(QualifiedIdentifier(" ", " ")), "` `.` `");
    }
}
