use crate::data::log_record::LogRecordPos;
use crate::index::Indexer;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;

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
}

// tests
// cargo test --lib index
#[cfg(test)]
mod tests {
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
}
