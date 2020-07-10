use std::collections::VecDeque;

use bytes::Buf;

#[derive(Default)]
pub struct BufList<T> {
    bufs: VecDeque<T>,
    rem: usize,
    cursor: usize,
}

impl<T: Buf> BufList<T> {
    #[inline]
    pub fn push(&mut self, buf: T) {
        let rem = buf.remaining();

        if rem > 0 {
            self.bufs.push_back(buf);
            self.rem += rem;
        }
    }

    #[inline]
    pub fn bufs_cnt(&self) -> usize {
        self.bufs.len()
    }

    pub fn commit(&mut self) {
        while self.cursor > 0 {
            let front = &mut self.bufs[0];
            let rem = front.remaining();

            if rem > self.cursor {
                front.advance(self.cursor);
                self.cursor = 0;
            } else {
                front.advance(rem);
                self.cursor -= rem;
                self.bufs.pop_front();
            }
        }
    }

    pub fn rollback(&mut self) {
        self.rem += self.cursor;
        self.cursor = 0;
    }
}

impl<T: Buf> Buf for BufList<T> {
    #[inline]
    fn remaining(&self) -> usize {
        self.rem
    }

    #[inline]
    fn bytes(&self) -> &[u8] {
        let mut cnt = self.cursor;

        for buf in &self.bufs {
            let bytes = buf.bytes();
            if bytes.len() > cnt {
                return &bytes[cnt..];
            }
            cnt -= bytes.len();
        }

        b""
    }

    #[inline]
    fn advance(&mut self, cnt: usize) {
        self.rem -= cnt;
        self.cursor += cnt;
    }
}

#[test]
fn it_advances() {
    let mut list = BufList::<&[_]>::default();
    list.push(&[1, 2, 3]);
    list.push(&[]);
    list.push(&[4, 5, 6]);
    list.push(&[7, 8, 9]);
    list.push(&[]);

    assert_eq!(list.bufs_cnt(), 3);
    list.advance(1);
    list.commit();
    assert_eq!(list.bufs_cnt(), 3);
    list.advance(4);
    list.commit();
    assert_eq!(list.bufs_cnt(), 2);
    list.advance(4);
    list.commit();
    assert_eq!(list.bufs_cnt(), 0);
}

#[test]
fn it_copies_to_slice() {
    let mut list = BufList::<&[_]>::default();
    list.push(&[1, 2, 3]);
    list.push(&[]);
    list.push(&[4, 5, 6]);
    list.push(&[7, 8, 9]);
    list.push(&[]);
    assert_eq!(list.bufs_cnt(), 3);

    let mut result = vec![0; 9];

    list.copy_to_slice(&mut result[0..1]);
    list.commit();
    assert_eq!(result, [1, 0, 0, 0, 0, 0, 0, 0, 0]);
    assert_eq!(list.bufs_cnt(), 3);

    list.copy_to_slice(&mut result[1..5]);
    list.commit();
    assert_eq!(result, [1, 2, 3, 4, 5, 0, 0, 0, 0]);
    assert_eq!(list.bufs_cnt(), 2);

    list.copy_to_slice(&mut result[5..]);
    list.commit();
    assert_eq!(result, [1, 2, 3, 4, 5, 6, 7, 8, 9]);
    assert_eq!(list.bufs_cnt(), 0);
}

#[test]
fn it_does_rollback() {
    let mut list = BufList::<&[_]>::default();
    list.push(&[1, 2, 3]);
    list.push(&[4, 5]);

    let mut result = vec![0; 5];
    list.copy_to_slice(&mut result[0..2]);
    assert_eq!(result, [1, 2, 0, 0, 0]);
    list.commit();
    list.copy_to_slice(&mut result[2..4]);
    assert_eq!(result, [1, 2, 3, 4, 0]);
    list.copy_to_slice(&mut result[4..]);
    assert_eq!(result, [1, 2, 3, 4, 5]);

    list.rollback();

    let mut result = vec![0; 3];
    list.copy_to_slice(&mut result);
    assert_eq!(result, [3, 4, 5]);
}
