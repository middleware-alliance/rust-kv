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

#[derive(Debug, Clone)]
pub enum IndexType {
    // BTree index
    BTree,

    // skip  table index
    SkipList,
}


impl Default for Options {
    fn default() -> Self {
        Self {
            dir_path: std::env::temp_dir().join("bitcask-rs"),
            data_file_size: 256 *1024 * 1024,
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