use crate::data::log_record::LogRecordPos;
use crate::index::{IndexIterator, Indexer};
use crate::options::IteratorOptions;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;
use bytes::Bytes;
use crate::errors::Result;

// BTree is a wrapper around BTreeMap<Vec<u8>, LogRecordPos>
pub struct BTree {
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecordPos>>>,
}

impl BTree {
    // new creates a new BTree
    pub fn new() -> Self {
        Self {
            tree: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Indexer for BTree {
    // put inserts a key-value pair into the BTree
    // returns true if the key-value pair is inserted, false otherwise
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool {
        let mut with_write_guard = self.tree.write();
        with_write_guard.insert(key, pos);
        true
    }

    // get retrieves the value associated with a key from the BTree
    // returns None if the key is not found
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let with_read_guard = self.tree.read();
        with_read_guard.get(&key).cloned()
    }

    // delete removes a key-value pair from the BTree
    // returns true if the key-value pair is removed, false otherwise
    fn delete(&self, key: Vec<u8>) -> bool {
        let mut with_write_guard = self.tree.write();
        let remove_res = with_write_guard.remove(&key);
        remove_res.is_some()
    }

    // list_keys returns a list of all keys in the BTree
    fn list_keys(&self) -> Result<Vec<Bytes>> {
        let read_guard = self.tree.read();
        let mut keys = Vec::with_capacity(read_guard.len());
        for (k, _) in read_guard.iter() {
            keys.push(Bytes::copy_from_slice(&k));
        }
        Ok(keys)
    }

    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator> {
        let read_guard = self.tree.read();
        let mut items = Vec::with_capacity(read_guard.len());
        // load btree items into a vector
        for (key, value) in read_guard.iter() {
            items.push((key.clone(), value.clone()));
        }
        if options.reverse {
            items.reverse();
        }
        Box::new(BTreeIterator {
            items,
            curr_index: 0,
            options,
        })
    }
}

/// BTreeIterator is an iterator over the key-value pairs in the BTree
pub struct BTreeIterator {
    items: Vec<(Vec<u8>, LogRecordPos)>, // store the key-value pairs in a vector
    curr_index: usize,                   // the current index of the vector
    options: IteratorOptions,            // the options for the iterator
}

impl IndexIterator for BTreeIterator {
    /// rewind resets the iterator to the beginning of the key-value pairs
    fn rewind(&mut self) {
        self.curr_index = 0;
    }

    /// seek moves the iterator to the position of the first key-value pair whose key is greater than or equal to the given key
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

    /// next returns the next key-value pair in the iterator
    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)> {
        if self.curr_index >= self.items.len() {
            return None;
        }

        while let Some(item) = self.items.get(self.curr_index) {
            self.curr_index += 1;
            let prefix = &self.options.prefix;
            if prefix.is_empty() || item.0.starts_with(prefix) {
                return Some((&item.0, &item.1));
            }
        }

        None
    }
}

// tests
// cargo test --lib index
#[cfg(test)]
mod tests {
    use std::str::from_utf8;
    use super::*;

    #[test]
    fn test_btree_put_get_delete() {
        let btree = BTree::new();
        let key1 = vec![1, 2, 3];
        let key2 = vec![4, 5, 6];
        let key3 = "fufeng".as_bytes().to_vec();
        let pos1 = LogRecordPos {
            file_id: 1,
            offset: 2,
        };
        let pos2 = LogRecordPos {
            file_id: 3,
            offset: 4,
        };
        let pos3 = LogRecordPos {
            file_id: 5,
            offset: 6,
        };
        assert_eq!(btree.put(key1.clone(), pos1.clone()), true);
        assert_eq!(btree.put(key2.clone(), pos2.clone()), true);
        assert_eq!(btree.put(key3.clone(), pos3.clone()), true);
        assert_eq!(btree.get(key1.clone()), Some(pos1.clone()));
        assert_eq!(btree.get(key2.clone()), Some(pos2.clone()));
        assert_eq!(btree.get(key3.clone()), Some(pos3.clone()));
        assert_eq!(btree.delete(key1.clone()), true);
        assert_eq!(btree.get(key1.clone()), None);
        assert_eq!(btree.get(key2.clone()), Some(pos2.clone()));
        let pos4 = LogRecordPos {
            file_id: 7,
            offset: 8,
        };
        assert_eq!(btree.put(key3.clone(), pos4), true);
        assert_eq!(btree.get(key3.clone()), Some(pos4.clone()));
    }

    #[test]
    fn test_btree_put() {
        let btree = BTree::new();
        let res1 = btree.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 2,
            },
        );
        assert_eq!(res1, true);
        let res2 = btree.put(
            "a".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 3,
                offset: 4,
            },
        );
        assert_eq!(res2, true);
    }

    #[test]
    fn test_btree_get() {
        let btree = BTree::new();
        let res1 = btree.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 2,
            },
        );
        assert_eq!(res1, true);
        let res2 = btree.put(
            "a".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 3,
                offset: 4,
            },
        );
        assert_eq!(res2, true);

        let pos1 = btree.get("".as_bytes().to_vec());
        assert!(pos1.is_some());
        assert_eq!(pos1.unwrap().file_id, 1);
        assert_eq!(pos1.unwrap().offset, 2);

        let pos2 = btree.get("a".as_bytes().to_vec());
        assert!(pos2.is_some());
        assert_eq!(pos2.unwrap().file_id, 3);
        assert_eq!(pos2.unwrap().offset, 4);

        let pos3 = btree.get("b".as_bytes().to_vec());
        assert!(pos3.is_none());
    }

    #[test]
    fn test_btree_delete() {
        let btree = BTree::new();
        let res1 = btree.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 2,
            },
        );
        assert_eq!(res1, true);
        let res2 = btree.put(
            "a".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 3,
                offset: 4,
            },
        );
        assert_eq!(res2, true);

        let res3 = btree.delete("".as_bytes().to_vec());
        assert_eq!(res3, true);

        let pos1 = btree.get("".as_bytes().to_vec());
        assert!(pos1.is_none());

        let pos2 = btree.get("a".as_bytes().to_vec());
        assert!(pos2.is_some());
        assert_eq!(pos2.unwrap().file_id, 3);
        assert_eq!(pos2.unwrap().offset, 4);

        let res4 = btree.delete("b".as_bytes().to_vec());
        assert!(!res4);
        assert_eq!(res4, false);
    }

    #[test]
    fn test_btree_iterator_seek() {
        let btree = BTree::new();

        // no data
        let mut iter = btree.iterator(IteratorOptions::default());
        iter.seek("key1".as_bytes().to_vec());
        let result = iter.next();
        assert!(result.is_none());

        // one data
        btree.put("ccde".as_bytes().to_vec(), LogRecordPos { file_id: 1, offset: 10 });
        let mut iter = btree.iterator(IteratorOptions::default());
        iter.seek("aa".as_bytes().to_vec());
        let result = iter.next();
        assert!(result.is_some());

        let mut iter = btree.iterator(IteratorOptions::default());
        iter.seek("zz".as_bytes().to_vec());
        let result = iter.next();
        assert!(result.is_none());

        // multiple data
        btree.put("bbcd".as_bytes().to_vec(), LogRecordPos { file_id: 2, offset: 20 });
        btree.put("abce".as_bytes().to_vec(), LogRecordPos { file_id: 3, offset: 30 });
        btree.put("cbcf".as_bytes().to_vec(), LogRecordPos { file_id: 4, offset: 40 });
        let mut iter = btree.iterator(IteratorOptions::default());
        iter.seek("b".as_bytes().to_vec());
        while let Some (item) = iter.next() {
            println!("{:?}", String::from_utf8(item.0.to_vec()));
            assert!(item.0.len() > 0);
        }

        let mut iter = btree.iterator(IteratorOptions::default());
        iter.seek("cbcf".as_bytes().to_vec());
        while let Some (item) = iter.next() {
            println!("cbcf: {:?}", String::from_utf8(item.0.to_vec()));
            assert!(item.0.len() > 0);
        }

        let mut iter = btree.iterator(IteratorOptions::default());
        iter.seek("zz".as_bytes().to_vec());
        let item = iter.next();
        assert!(item.is_none());

        // reverse
        let mut iter = btree.iterator(IteratorOptions { reverse: true, ..IteratorOptions::default() });
        iter.seek("bb".as_bytes().to_vec());
        while let Some (item) = iter.next() {
            println!("bb: {:?}", String::from_utf8(item.0.to_vec()));
            assert!(item.0.len() > 0);
        }
    }

    #[test]
    fn test_btree_iterator_next() {
        let btree = BTree::new();

        // no data
        let mut iter = btree.iterator(IteratorOptions::default());
        let result = iter.next();
        assert!(result.is_none());

        // one data
        btree.put("ccde".as_bytes().to_vec(), LogRecordPos { file_id: 1, offset: 10 });
        let mut options = IteratorOptions::default();
        options.reverse = true;
        let mut iter = btree.iterator(options);
        let result = iter.next();
        assert!(result.is_some());

        // multiple data
        btree.put("bbcd".as_bytes().to_vec(), LogRecordPos { file_id: 2, offset: 20 });
        btree.put("abce".as_bytes().to_vec(), LogRecordPos { file_id: 3, offset: 30 });
        btree.put("cbcf".as_bytes().to_vec(), LogRecordPos { file_id: 4, offset: 40 });
        let mut options = IteratorOptions::default();
        options.reverse = true;
        let mut iter = btree.iterator(options);
        while let Some (item) = iter.next() {
            println!("multiple: {:?}", String::from_utf8(item.0.to_vec()));
            assert!(item.0.len() > 0);
        }

        // prefix
        let mut options = IteratorOptions::default();
        options.prefix = "c".as_bytes().to_vec();
        let mut iter = btree.iterator(options);
        while let Some (item) = iter.next() {
            println!("prefix: {:?}", String::from_utf8(item.0.to_vec()));
            assert!(item.0.len() > 0);
        }
    }
}
