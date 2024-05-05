pub mod btree;

use crate::errors::Result;
use bytes::Bytes;
use crate::data::log_record::LogRecordPos;
use crate::options::{IndexType, IteratorOptions};

// Indexer is a trait that defines the interface for an index.
// It provides methods for inserting, retrieving, and deleting keys and their corresponding positions in the log.
pub trait Indexer: Send + Sync {
    // Inserts a key-value pair into the index. Returns true if the insertion was successful, false otherwise.
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool;
    // Retrieves the position of a key from the index. Returns None if the key is not found.
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;
    // Deletes a key-value pair from the index. Returns true if the deletion was successful, false otherwise.
    fn delete(&self, key: Vec<u8>) -> bool;
    // Returns a list of all keys in the index.
    fn list_keys(&self) -> Result<Vec<Bytes>>;
    // Returns an iterator over the key-value pairs in the index.
    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator>;
}

pub fn new_indexer(index_type: IndexType) -> impl Indexer {
    match index_type {
        IndexType::BTree => btree::BTree::new(),
        IndexType::SkipList => todo!(),
        _ => panic!("Unsupported index type"),
    }
}

pub trait IndexIterator: Sync + Send {
    /// rewind the iterator to the beginning of the index
    fn rewind(&mut self);
    /// seek to the position of the key
    fn seek(&mut self, key: Vec<u8>);
    /// next returns the next key-value pair in the index
    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)>;
}