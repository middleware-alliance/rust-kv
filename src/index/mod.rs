pub mod btree;

use crate::data::log_record::LogRecordPos;
use crate::options::IndexType;

// Indexer is a trait that defines the interface for an index.
// It provides methods for inserting, retrieving, and deleting keys and their corresponding positions in the log.
pub trait Indexer: Send + Sync {
    // Inserts a key-value pair into the index. Returns true if the insertion was successful, false otherwise.
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool;
    // Retrieves the position of a key from the index. Returns None if the key is not found.
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;
    // Deletes a key-value pair from the index. Returns true if the deletion was successful, false otherwise.
    fn delete(&self, key: Vec<u8>) -> bool;
}

pub fn new_indexer(index_type: IndexType) -> impl Indexer {
    match index_type {
        IndexType::BTree => btree::BTree::new(),
        IndexType::SkipList => todo!(),
        _ => panic!("Unsupported index type"),
    }
}
