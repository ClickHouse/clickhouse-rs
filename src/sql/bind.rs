use std::fmt;

use super::escape;
use crate::sealed::Sealed;

pub trait Bind: Sealed {
    #[doc(hidden)]
    fn reserve(&self) -> usize;

    #[doc(hidden)]
    fn write(&self, dst: impl fmt::Write) -> fmt::Result;
}

macro_rules! impl_num {
    ($ty:ty, $reserve:literal) => {
        impl Sealed for $ty {}

        impl Bind for $ty {
            #[inline]
            fn reserve(&self) -> usize {
                $reserve
            }

            #[inline]
            fn write(&self, mut dst: impl fmt::Write) -> fmt::Result {
                write!(dst, "{}", self)
            }
        }
    };
}

impl_num!(i8, 4);
impl_num!(u8, 3);
impl_num!(i16, 6);
impl_num!(u16, 5);
impl_num!(i32, 11);
impl_num!(u32, 10);
impl_num!(i64, 20);
impl_num!(u64, 20);
impl_num!(i128, 40);
impl_num!(u128, 39);

impl Sealed for &str {}

impl Bind for &str {
    #[inline]
    fn reserve(&self) -> usize {
        self.len()
    }

    #[inline]
    fn write(&self, dst: impl fmt::Write) -> fmt::Result {
        escape::string(self, dst)
    }
}

impl Sealed for String {}

impl Bind for String {
    #[inline]
    fn reserve(&self) -> usize {
        self.len()
    }

    #[inline]
    fn write(&self, dst: impl fmt::Write) -> fmt::Result {
        escape::string(self, dst)
    }
}

impl<'a, T: Bind> Sealed for &'a [T] {}

impl<'a, T: Bind> Bind for &'a [T] {
    #[inline]
    fn reserve(&self) -> usize {
        let commas_count = self.len().saturating_sub(1);
        self.iter().map(Bind::reserve).sum::<usize>() + commas_count + 2
    }

    #[inline]
    fn write(&self, mut dst: impl fmt::Write) -> fmt::Result {
        write!(&mut dst, "[")?;
        let mut iter = self.iter();
        if let Some(item) = iter.next() {
            item.write(&mut dst)?;
        }
        for item in iter {
            write!(&mut dst, ",")?;
            item.write(&mut dst)?;
        }
        write!(dst, "]")
    }
}

/// Bound the provided string as an identifier.
/// It can be used for table names, for instance.
pub struct Identifier<'a>(pub &'a str);

impl<'a> Sealed for Identifier<'a> {}

impl<'a> Bind for Identifier<'a> {
    #[inline]
    fn reserve(&self) -> usize {
        self.0.len() + 2
    }

    #[inline]
    fn write(&self, dst: impl fmt::Write) -> fmt::Result {
        escape::identifier(self.0, dst)
    }
}
