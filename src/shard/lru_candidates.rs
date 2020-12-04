use smallvec::SmallVec;
use std::fmt;

// notes:
// - std primitive slice binary search by

pub(super) enum InsertionStatus<Key> {
    /// The candidate was inserted, and may have replaced a Key.
    Inserted(Option<Key>),
    /// The candidate was ineligible for insertion, as it was not more eligible
    /// for eviction compared to the other candidates in the pool.
    Ineligible,
}

impl<K: fmt::Debug> fmt::Debug for InsertionStatus<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InsertionStatus::Inserted(k) => write!(f, "InsertionStatus::Inserted({:?})", k),
            InsertionStatus::Ineligible => write!(f, "InsertionStatus::Ineligible"),
        }
    }
}

pub(super) struct LruCandidates<Key> {
    candidates: SmallVec<[(Key, u64); 32]>,
}

impl<Key> Default for LruCandidates<Key> {
    fn default() -> Self {
        Self {
            candidates: SmallVec::new(),
        }
    }
}

impl<Key: Eq + Clone> LruCandidates<Key> {
    #[inline]
    pub fn capacity(&self) -> usize {
        self.candidates.inline_size()
    }

    pub fn len(&self) -> usize {
        self.candidates.len()
    }

    #[inline]
    pub fn try_insert_candidate(&mut self, key: &Key, access_offset: u64) -> InsertionStatus<Key> {
        // We have free space in our candidates array, this means we'll unconditionally insert it,
        // our priority here is to have our candidate array filled up, so if there's an empty
        // slot, we'll take it, and if any more eligible candidates come along, we'll leave.
        if self.candidates.len() < self.candidates.inline_size() {
            self.insert_sorted(key, access_offset);
            return InsertionStatus::Inserted(None);
        }

        // We are now in the situation where the candidates vec is full, and we need to see if we can figure
        // out a more "eligible" candidate.
        let max = self.max_access_offset();
        // we are better than the end of the array.
        if access_offset < max {
            let replaced = self.candidates.pop().expect("candidates are empty");
            self.insert_sorted(key, access_offset);
            InsertionStatus::Inserted(Some(replaced.0))
        } else {
            InsertionStatus::Ineligible
        }
    }

    #[inline]
    pub fn take_best_candidate(&mut self) -> Option<Key> {
        self.candidates.pop().map(|k| k.0)
    }

    #[inline]
    pub fn remove_candidate_by_access_offset(&mut self, access_offset: u64) -> Option<Key> {
        if let Ok(index) = self.binary_search(access_offset) {
            Some(self.candidates.remove(index).0)
        } else {
            None
        }
    }

    #[inline]
    fn binary_search(&self, access_offset: u64) -> Result<usize, usize> {
        let slice = self.candidates.as_slice();
        slice.binary_search_by(|k| k.1.cmp(&access_offset).reverse())
    }

    #[inline]
    fn insert_sorted(&mut self, key: &Key, access_offset: u64) -> bool {
        if let Err(index) = self.binary_search(access_offset) {
            self.candidates.insert(index, (key.clone(), access_offset));
            true
        } else {
            false
        }
    }

    #[inline]
    fn max_access_offset(&self) -> u64 {
        let slice = self.candidates.as_slice();
        slice.first().expect("candidates are empty").1
    }
}

#[cfg(test)]
mod test {
    use super::{InsertionStatus, LruCandidates};

    #[test]
    fn test_lru_candidates_fill_and_drain() {
        let mut candidates = LruCandidates::default();
        assert_eq!(candidates.len(), 0);
        assert_eq!(candidates.capacity(), 32);

        // Insert capacity items, it should insert w/o replacing any keys.
        for k in 0..candidates.capacity() {
            let res = candidates.try_insert_candidate(&k, 1000u64 + k as u64);
            match res {
                InsertionStatus::Inserted(None) => {}
                _ => panic!(),
            }
        }

        assert_eq!(candidates.len(), 32);

        // Try draining the best candidates...
        for k in 0..candidates.capacity() {
            let best_candidate = candidates.take_best_candidate().unwrap();
            assert_eq!(best_candidate, k);
            assert_eq!(candidates.len(), 32 - (k + 1));
        }

        assert_eq!(candidates.len(), 0);
    }

    #[test]
    fn test_lru_candidates_worse_candidates_are_ineligible() {
        let mut candidates = LruCandidates::default();
        // Insert capacity items, it should insert w/o replacing any keys.
        for k in 0..candidates.capacity() {
            let res = candidates.try_insert_candidate(&k, 1000u64 + k as u64);
            match res {
                InsertionStatus::Inserted(None) => {}
                _ => panic!(),
            }
        }

        // Try inserting worst candidates...
        for k in 0..candidates.capacity() {
            let k = 500 + k;
            let res = candidates.try_insert_candidate(&k, 1000u64 + k as u64);
            match res {
                InsertionStatus::Ineligible => {}
                _ => panic!(),
            }
        }
        assert_eq!(candidates.len(), 32);
    }

    #[test]
    fn test_lru_candidates_promotes_better_candidates() {
        let mut candidates = LruCandidates::default();
        // Insert capacity items, it should insert w/o replacing any keys.
        for k in 0..candidates.capacity() {
            let res = candidates.try_insert_candidate(&k, 1000u64 + (k * 2) as u64);
            match res {
                InsertionStatus::Inserted(None) => {}
                _ => panic!(),
            }
        }

        // Should replace the best candidate with a new best.
        let res = candidates.try_insert_candidate(&9000, 100);
        match res {
            InsertionStatus::Inserted(Some(0)) => {}
            _ => panic!(),
        }

        // Should replace the best candidate with a new best.
        let res = candidates.try_insert_candidate(&9001, 101);
        match res {
            InsertionStatus::Inserted(Some(9000)) => {}
            _ => panic!(),
        }

        // Should put it somewhere in the middle...
        let res = candidates.try_insert_candidate(&9001, 1009);
        match res {
            InsertionStatus::Inserted(Some(9001)) => {}
            _ => panic!(),
        }

        assert_eq!(candidates.len(), 32);

        let mut best = vec![];
        while let Some(n) = candidates.take_best_candidate() {
            best.push(n);
        }
        assert_eq!(
            best,
            vec![
                1, 2, 3, 4, 9001, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
                22, 23, 24, 25, 26, 27, 28, 29, 30, 31
            ]
        );
    }

    #[test]
    fn test_remove_candidate_by_access_offset() {
        let mut candidates = LruCandidates::default();
        // Insert capacity items, it should insert w/o replacing any keys.
        for k in 0..candidates.capacity() {
            let res = candidates.try_insert_candidate(&k, 1000u64 + k as u64);
            match res {
                InsertionStatus::Inserted(None) => {}
                _ => panic!(),
            }
        }
        // Remove em all.
        for k in 0..candidates.capacity() {
            assert_eq!(
                candidates.remove_candidate_by_access_offset(1000u64 + k as u64),
                Some(k)
            );
        }
    }
}
