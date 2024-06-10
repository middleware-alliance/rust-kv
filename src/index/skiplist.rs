use crate::data::log_record::LogRecordPos;
use crate::index::{IndexIterator, Indexer};
use crate::options::IteratorOptions;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use std::sync::Arc;

/// SkipList is a probabilistic data structure
/// that allows for efficient insertion, deletion, and search operations.
pub struct SkipList {
    skl: Arc<SkipMap<Vec<u8>, LogRecordPos>>,
}

impl SkipList {
    /// Create a new SkipList
    pub fn new() -> Self {
        Self {
            skl: Arc::new(SkipMap::new()),
        }
    }
}

impl Indexer for SkipList {
    /// Insert a key-value pair into the SkipList.
    /// If the key already exists, update the value.
    /// Return the previous value if the key exists, None otherwise.
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool {
        let mut result = None;
        if let Some(entry) = self.skl.get(&key) {
            result = Some(*entry.value());
        }
        self.skl.insert(key, pos);
        true
    }

    /// Get the value of a key from the SkipList.
    /// Return None if the key does not exist.
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        if let Some(entry) = self.skl.get(&key) {
            return Some(*entry.value());
        }
        None
    }

    /// Delete a key-value pair from the SkipList.
    /// Return the value if the key exists, None otherwise.
    fn delete(&self, key: Vec<u8>) -> bool {
        if let Some(entry) = self.skl.remove(&key) {
            Some(*entry.value());
        }
        true
    }

    /// List all keys in the SkipList.
    fn list_keys(&self) -> crate::errors::Result<Vec<Bytes>> {
        let mut keys = Vec::with_capacity(self.skl.len());
        for entry in self.skl.iter() {
            keys.push(Bytes::copy_from_slice(entry.key()));
        }
        Ok(keys)
    }

    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator> {
        let mut items = Vec::with_capacity(self.skl.len());
        for entry in self.skl.iter() {
            items.push((entry.key().clone(), *entry.value()));
        }
        Box::new(SkipListIterator {
            items,
            curr_index: 0,
            options,
        })
    }
}

/// SkipListIterator is an iterator over the key-value pairs in the SkipList.
pub struct SkipListIterator {
    items: Vec<(Vec<u8>, LogRecordPos)>, // (key, value) pairs
    curr_index: usize,                   // current index in the items vector
    options: IteratorOptions,            // iterator options
}

/// SkipListIterator is an iterator over the key-value pairs in the SkipList.
impl IndexIterator for SkipListIterator {
    /// Return the current key-value pair.
    fn rewind(&mut self) {
        self.curr_index = 0;
    }

    /// Seek to the first key-value pair with a key greater than or equal to the given key.
    fn seek(&mut self, key: Vec<u8>) {
        /*self.curr_index = match self.items.binary_search_by(|(x, _)| {
            if self.options.reverse {
                x.cmp(&key).reverse()
            } else {
                x.cmp(&key)
            }
        }) {
            Ok(equal_val) => equal_val,
            Err(insert_val) => insert_val,
        };*/
        self.curr_index = self
            .items
            .binary_search_by(|(x, _)| {
                if self.options.reverse {
                    x.cmp(&key).reverse()
                } else {
                    x.cmp(&key)
                }
            })
            .unwrap_or_else(|insert_val| insert_val);
    }

    /// Return the next key-value pair.
    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)> {
        if self.curr_index == self.items.len() {
            return None;
        }
        while let Some(item) = self.items.get(self.curr_index) {
            self.curr_index += 1;
            let prefix = &self.options.prefix;
            if prefix.is_empty() || item.0.starts_with(&prefix) {
                return Some((&item.0, &item.1));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_skl_put() {
        let skl = SkipList::new();
        let res1 = skl.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res1);
        let res2 = skl.put(
            "acdd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res2);
        let res3 = skl.put(
            "bbae".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res3);
        let res4 = skl.put(
            "ddee".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res4);

        let res5 = skl.put(
            "ddee".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 93,
                offset: 22,
            },
        );
        assert!(res5);
    }

    #[test]
    fn test_skl_get() {
        let skl = SkipList::new();

        let v1 = skl.get(b"not exists".to_vec());
        assert!(v1.is_none());

        let res1 = skl.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res1);
        let v2 = skl.get(b"aacd".to_vec());
        assert!(v2.is_some());

        let res2 = skl.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 11,
                offset: 990,
            },
        );
        assert!(res2);
        let v3 = skl.get(b"aacd".to_vec());
        assert!(v3.is_some());
    }

    #[test]
    fn test_skl_delete() {
        let skl = SkipList::new();

        let r1 = skl.delete(b"not exists".to_vec());
        assert!(r1);

        let res1 = skl.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res1);

        let r2 = skl.delete(b"aacd".to_vec());
        assert!(r2);

        let v2 = skl.get(b"aacd".to_vec());
        assert!(v2.is_none());
    }

    #[test]
    fn test_skl_list_keys() {
        let skl = SkipList::new();

        let keys1 = skl.list_keys();
        assert_eq!(keys1.ok().unwrap().len(), 0);

        let res1 = skl.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res1);
        let res2 = skl.put(
            "acdd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res2);
        let res3 = skl.put(
            "bbae".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res3);
        let res4 = skl.put(
            "ddee".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res4);

        let keys2 = skl.list_keys();
        assert_eq!(keys2.ok().unwrap().len(), 4);
    }

    #[test]
    fn test_skl_iterator() {
        let skl = SkipList::new();

        let res1 = skl.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res1);
        let res2 = skl.put(
            "acdd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res2);
        let res3 = skl.put(
            "bbae".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res3);
        let res4 = skl.put(
            "ddee".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res4);

        let mut opts = IteratorOptions::default();
        opts.reverse = true;
        let mut iter1 = skl.iterator(opts);

        while let Some((key, _)) = iter1.next() {
            assert!(!key.is_empty());
        }
    }
}
