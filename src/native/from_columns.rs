use crate::native::Block;
use std::error::Error;
use std::mem::MaybeUninit;
use std::cmp;

pub trait FromColumns<'b> {
    fn from_columns<O: UninitBuf<Self>>(block: &'b Block, out: O) -> Result<O::Init, Box<dyn Error>> where Self: Sized;
}

pub trait UninitBuf<T>: Sized {
    /// Buffer type containing only initialized data.
    type Init;

    /// Return the written length.
    fn written_len(&self) -> usize;

    /// Check if the buffer capacity can accept `expected` items.
    ///
    /// If capacity can be dynamically reserved, allocate space for `expected` items.
    ///
    /// Return the total available capacity, after allocating if applicable, which may be less
    /// than `expected`.
    fn check_capacity(&mut self, expected: usize) -> usize;

    /// Push a value.
    ///
    /// # Panics
    /// May panic if the value would exceed a fixed capacity.
    /// Use [`Self::check_capacity()`] to verify or allocate additional capacity, if applicable.
    fn push(&mut self, val: T);

    /// Return a view into the initialized capacity.
    fn into_init(self) -> Self::Init;
}

impl<T> UninitBuf<T> for Vec<T> {
    type Init = Self;

    #[inline(always)]
    fn written_len(&self) -> usize {
        self.len()
    }

    #[inline(always)]
    fn check_capacity(&mut self, expected: usize) -> usize {
        self.reserve(expected);
        self.capacity() - self.len()
    }

    #[inline(always)]
    fn push(&mut self, val: T) {
        self.push(val);
    }

    #[inline(always)]
    fn into_init(self) -> Self::Init {
        self
    }
}

impl<T> UninitBuf<T> for &'_ mut Vec<T> {
    type Init = Self;

    #[inline(always)]
    fn written_len(&self) -> usize {
        Vec::written_len(self)
    }
    #[inline(always)]
    fn check_capacity(&mut self, expected: usize) -> usize {
        Vec::check_capacity(self, expected)
    }


    #[inline(always)]
    fn push(&mut self, val: T) {
        Vec::push(self, val);
    }

    #[inline(always)]
    fn into_init(self) -> Self::Init {
        self
    }
}

pub struct UninitSliceWriter<'a, T> {
    written_len: usize,
    slice: &'a mut [MaybeUninit<T>],
}

impl<'a, T> UninitSliceWriter<'a, T> {
    pub fn new(slice: &'a mut [MaybeUninit<T>]) -> Self {
        UninitSliceWriter {
            written_len: 0,
            slice,
        }
    }
}

impl<'a, T> UninitBuf<T> for UninitSliceWriter<'a, T> {
    type Init = &'a mut [T];

    fn written_len(&self) -> usize {
        self.written_len
    }

    fn check_capacity(&mut self, expected: usize) -> usize {
        cmp::min(expected, self.slice.len())
    }

    fn push(&mut self, val: T) {
        // SAFETY: if `self.written_len` is the length of the array,
        // don't increment it further or else `Self::into_init()` is UB.
        self.slice[self.written_len].write(val);
        self.written_len += 1;
    }

    fn into_init(self) -> Self::Init {
        // SAFETY: up to `self.written_len` is initialized
        unsafe {
            self.slice[..self.written_len].assume_init_mut()
        }
    }
}
