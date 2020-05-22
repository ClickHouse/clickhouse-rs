use std::collections::VecDeque;

use bytes::Buf;

#[derive(Default)]
pub struct BufList<T> {
    bufs: VecDeque<T>,
    rem: usize,
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
}

impl<T: Buf> Buf for BufList<T> {
    #[inline]
    fn remaining(&self) -> usize {
        self.rem
    }

    #[inline]
    fn bytes(&self) -> &[u8] {
        self.bufs.front().map(Buf::bytes).unwrap_or_default()
    }

    #[inline]
    fn advance(&mut self, mut cnt: usize) {
        self.rem -= cnt;

        while cnt > 0 {
            let front = &mut self.bufs[0];
            let rem = front.remaining();
            if rem > cnt {
                front.advance(cnt);
                return;
            } else {
                front.advance(rem);
                cnt -= rem;
            }

            self.bufs.pop_front();
        }
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
    assert_eq!(list.bufs_cnt(), 3);
    list.advance(4);
    assert_eq!(list.bufs_cnt(), 2);
    list.advance(4);
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
    assert_eq!(result, [1, 0, 0, 0, 0, 0, 0, 0, 0]);
    assert_eq!(list.bufs_cnt(), 3);

    list.copy_to_slice(&mut result[1..5]);
    assert_eq!(result, [1, 2, 3, 4, 5, 0, 0, 0, 0]);
    assert_eq!(list.bufs_cnt(), 2);

    list.copy_to_slice(&mut result[5..]);
    assert_eq!(result, [1, 2, 3, 4, 5, 6, 7, 8, 9]);
    assert_eq!(list.bufs_cnt(), 0);
}
