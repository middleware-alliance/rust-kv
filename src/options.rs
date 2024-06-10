use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Options {
    // The directory path for db files.
    pub dir_path: PathBuf,
    // The size of each data file in bytes.
    pub data_file_size: u64,
    // Whether every operation requires persistent synchronization
    pub sync_writes: bool,
    // The type of index to use.
    pub index_type: IndexType,
}

#[derive(Debug, Clone, PartialEq)]
pub enum IndexType {
    // BTree index
    BTree,

    // skip  table index
    SkipList,

    // BPlusTree index
    BPlusTree,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            dir_path: std::env::temp_dir().join("bitcask-rs"),
            data_file_size: 256 * 1024 * 1024,
            sync_writes: false,
            index_type: IndexType::BTree,
        }
    }
}

/// Iterator options for scanning the database.
pub struct IteratorOptions {
    pub prefix: Vec<u8>,
    pub reverse: bool,
}

impl Default for IteratorOptions {
    fn default() -> Self {
        Self {
            prefix: Default::default(),
            reverse: false,
        }
    }
}

/// Write batch options for batch writing to the database.
pub struct WriteBatchOptions {
    pub max_batch_num: usize, // max batch number
    pub sync_writes: bool,  // whether every operation requires persistent synchronization
}

impl Default for WriteBatchOptions {
    fn default() -> Self {
        Self {
            max_batch_num: 1000,
            sync_writes: true,
        }
    }
}
