use std::collections::VecDeque;

use bytes::Buf;

#[derive(Debug, Default)]
pub(crate) struct BufList<T> {
    next_buf: Option<T>,
    bufs: VecDeque<T>,
    rem: usize,
    cursor: usize,
}

impl<T: Buf> BufList<T> {
    #[inline]
    pub(crate) fn push(&mut self, buf: T) {
        let rem = buf.remaining();
        if rem == 0 {
            return;
        }

        if self.next_buf.is_none() {
            self.next_buf = Some(buf);
        } else {
            self.bufs.push_back(buf);
        }

        self.rem += rem;
    }

    #[inline]
    pub(crate) fn bufs_cnt(&self) -> usize {
        self.next_buf.is_some() as usize + self.bufs.len()
    }

    #[inline]
    pub(crate) fn commit(&mut self) {
        while self.cursor > 0 {
            let front = self.next_buf.as_mut().unwrap();
            let rem = front.remaining();

            if rem > self.cursor {
                front.advance(self.cursor);
                self.cursor = 0;
            } else {
                front.advance(rem);
                self.cursor -= rem;
                self.next_buf = self.bufs.pop_front();
            }
        }
    }

    pub(crate) fn rollback(&mut self) {
        self.rem += self.cursor;
        self.cursor = 0;
    }

    #[cold]
    fn chunk_slow(&self) -> &[u8] {
        let mut cnt = self.cursor - self.next_buf.as_ref().map_or(0, |b| b.chunk().len());

        for buf in &self.bufs {
            let bytes = buf.chunk();
            if bytes.len() > cnt {
                return &bytes[cnt..];
            }
            cnt -= bytes.len();
        }

        b""
    }
}

impl<T: Buf> Buf for BufList<T> {
    #[inline]
    fn remaining(&self) -> usize {
        self.rem
    }

    #[inline]
    fn chunk(&self) -> &[u8] {
        if let Some(buf) = &self.next_buf {
            let bytes = buf.chunk();
            if bytes.len() > self.cursor {
                return &bytes[self.cursor..];
            }
        }

        self.chunk_slow()
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
