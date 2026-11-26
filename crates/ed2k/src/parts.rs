//! Which parts of a file are here, which one a peer is fetching now, and what each part must
//! hash to.
//!
//! A part is eMule's unit: 9,728,000 bytes (the last one shorter). Every part is verified on its
//! own against the hash set the file was shared with, so a peer that sends rubbish costs one
//! part and not the file, and a download that was stopped anywhere on a part resumes cleanly.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::{Ed2kHash, PART_SIZE};

/// The parts of one download.
pub struct Parts {
    size: u64,
    count: usize,
    root: Ed2kHash,
    done: Vec<AtomicBool>,
    claimed: Vec<AtomicBool>,
    /// Where the search for a free part goes on from (so peers spread over the file instead of
    /// all queueing on its first unfinished part).
    cursor: AtomicUsize,
    remaining: AtomicUsize,
    /// The part hashes a peer sent, once one has and they checked out against [`Self::root`].
    hashes: Mutex<Option<Vec<[u8; 16]>>>,
}

impl Parts {
    /// The parts of a file of `size` bytes, `done` from a previous run (all false when there is
    /// nothing to resume).
    pub fn new(size: u64, root: Ed2kHash, done: Vec<bool>) -> Self {
        let count = count_of(size);
        let done: Vec<bool> = (0..count).map(|i| done.get(i).copied().unwrap_or(false)).collect();
        let remaining = done.iter().filter(|d| !**d).count();
        Self {
            size,
            count,
            root,
            done: done.iter().map(|d| AtomicBool::new(*d)).collect(),
            claimed: (0..count).map(|_| AtomicBool::new(false)).collect(),
            cursor: AtomicUsize::new(0),
            remaining: AtomicUsize::new(remaining),
            hashes: Mutex::new(None),
        }
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn root(&self) -> Ed2kHash {
        self.root
    }

    pub fn count(&self) -> usize {
        self.count
    }

    /// The bytes part `i` holds: `[start, end)`.
    pub fn range(&self, i: usize) -> (u64, u64) {
        let start = i as u64 * PART_SIZE;
        let end = (start + PART_SIZE).min(self.size);
        (start, end)
    }

    /// Take the next part nobody is fetching. `None` when every part is here or being fetched.
    pub fn claim(&self) -> Option<usize> {
        if self.remaining.load(Ordering::Acquire) == 0 {
            return None;
        }
        // One pass over the parts from the cursor; if every free one was taken meanwhile, a
        // second pass is what a caller's next claim() does.
        for _ in 0..self.count {
            let i = self.cursor.fetch_add(1, Ordering::Relaxed) % self.count;
            if self.done[i].load(Ordering::Relaxed) {
                continue;
            }
            if self
                .claimed[i]
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Some(i);
            }
        }
        None
    }

    /// Part `i` is here and verified.
    pub fn finish(&self, i: usize) {
        self.claimed[i].store(false, Ordering::Release);
        if !self.done[i].swap(true, Ordering::AcqRel) {
            self.remaining.fetch_sub(1, Ordering::AcqRel);
        }
    }

    /// Part `i` was not got (the peer went away, the bytes didn't verify): it is free again.
    pub fn release(&self, i: usize) {
        self.claimed[i].store(false, Ordering::Release);
    }

    pub fn is_done(&self, i: usize) -> bool {
        self.done.get(i).is_some_and(|d| d.load(Ordering::Relaxed))
    }

    pub fn done_count(&self) -> usize {
        self.count - self.remaining.load(Ordering::Acquire)
    }

    pub fn remaining(&self) -> usize {
        self.remaining.load(Ordering::Acquire)
    }

    pub fn all_done(&self) -> bool {
        self.remaining() == 0
    }

    /// Every part's state, as the progress file keeps it.
    pub fn snapshot(&self) -> Vec<bool> {
        self.done.iter().map(|d| d.load(Ordering::Relaxed)).collect()
    }

    /// What part `i` must hash to: the hash set's entry once a peer sent one, else the file's
    /// own hash when there is only one part (`Some(None)`-style "unknown" reads as `None`).
    pub fn expected(&self, i: usize) -> Option<[u8; 16]> {
        if self.count == 1 {
            return Some(*self.root.as_bytes());
        }
        self.hashes
            .lock()
            .ok()
            .and_then(|h| h.as_ref().and_then(|hs| hs.get(i).copied()))
    }

    /// The hash set a peer sent, if it checks out against the file's hash: kept, so every part
    /// can be verified. A set that does not check out is dropped (the peer may be lying, and the
    /// file's hash is the only thing the link guarantees).
    pub fn offer_hashes(&self, hashes: Vec<[u8; 16]>) -> bool {
        if hashes.len() != self.count {
            return false;
        }
        if !rucio_emule::ed2k::verify_part_hashes(&hashes, &self.root) {
            return false;
        }
        if let Ok(mut h) = self.hashes.lock() {
            // The first set that checks out wins; a later peer's is not needed.
            if h.is_none() {
                *h = Some(hashes);
            }
        }
        true
    }

    /// The part hashes, once known.
    pub fn hashes(&self) -> Option<Vec<[u8; 16]>> {
        self.hashes.lock().ok().and_then(|h| h.clone())
    }
}

/// How many parts a file of `size` bytes has (one, at least: an empty file is one empty part).
pub fn count_of(size: u64) -> usize {
    if size == 0 {
        return 1;
    }
    size.div_ceil(PART_SIZE) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parts(size: u64) -> Parts {
        let root = Ed2kHash::from_hex("0123456789abcdef0123456789abcdef").expect("hash");
        let count = count_of(size);
        Parts::new(size, root, vec![false; count])
    }

    #[test]
    fn parts_cover_the_file_and_the_last_one_is_shorter() {
        let size = PART_SIZE * 2 + 1000;
        let p = parts(size);
        assert_eq!(p.count(), 3);
        assert_eq!(p.range(0), (0, PART_SIZE));
        assert_eq!(p.range(1), (PART_SIZE, PART_SIZE * 2));
        assert_eq!(p.range(2), (PART_SIZE * 2, size));
        assert_eq!(parts(PART_SIZE).count(), 1);
        assert_eq!(parts(PART_SIZE + 1).count(), 2);
        assert_eq!(parts(0).count(), 1);
    }

    #[test]
    fn a_part_is_claimed_once_and_freed_when_a_peer_fails() {
        let p = parts(PART_SIZE * 2);
        let a = p.claim().expect("first");
        let b = p.claim().expect("second");
        assert_ne!(a, b);
        assert_eq!(p.claim(), None, "two parts, two claims");
        p.release(a);
        assert_eq!(p.claim(), Some(a));
        p.finish(a);
        p.finish(b);
        assert!(p.all_done());
        assert_eq!(p.done_count(), 2);
        assert_eq!(p.claim(), None);
        assert!(!p.is_done(0) || !p.is_done(1) || p.done_count() == 2);
    }

    #[test]
    fn a_single_part_file_expects_its_own_hash() {
        let p = parts(1000);
        assert_eq!(p.expected(0), Some(*p.root().as_bytes()));
    }

    #[test]
    fn a_set_that_does_not_check_out_is_refused() {
        let size = PART_SIZE + 10;
        let p = parts(size);
        assert_eq!(p.expected(0), None, "no set yet, nothing to check against");
        assert!(!p.offer_hashes(vec![[0u8; 16], [1u8; 16]]) );
        assert!(!p.offer_hashes(vec![[0u8; 16]]), "wrong count");
        assert_eq!(p.expected(0), None);
        assert!(p.hashes().is_none());
    }

    #[test]
    fn a_resumed_download_skips_what_is_done() {
        let p = Parts::new(
            PART_SIZE * 2,
            Ed2kHash::from_hex("0123456789abcdef0123456789abcdef").expect("hash"),
            vec![true, false],
        );
        assert_eq!(p.done_count(), 1);
        assert_eq!(p.remaining(), 1);
        assert_eq!(p.claim(), Some(1));
    }
}
