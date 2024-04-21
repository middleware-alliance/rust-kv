use std::path::PathBuf;

pub struct Options {
    // The directory path for db files.
    pub dir_path: PathBuf,
    // The size of each data file in bytes.
    pub data_file_size: u64,
    // Whether every operation requires persistent synchronization
    pub sync_writes: bool,
}
