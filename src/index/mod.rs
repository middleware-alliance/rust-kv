pub mod btree;

use crate::data::log_record::LogRecordPos;

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
