use crate::data::log_record::LogRecord;
use crate::errors::Result;
use crate::fio;
use parking_lot::RwLock;
use std::path::PathBuf;
use std::sync::Arc;

pub struct DataFile {
    file_id: Arc<RwLock<u32>>, // file_id is a shared variable that is accessed by multiple threads
    write_off: Arc<RwLock<u64>>, // write_off is a shared variable that is accessed by multiple threads, and it is used to keep track of the current write offset in the file
    io_manager: Box<dyn fio::IOManager>, // io_manager is a trait object that is used to perform I/O operations on the file
}

impl DataFile {
    pub fn new(dir_path: PathBuf, file_id: u32) -> Result<DataFile> {
        todo!()
    }

    pub fn get_write_off(&self) -> u64 {
        let read_guard = self.write_off.read();
        *read_guard
    }

    pub fn get_file_id(&self) -> u32 {
        let read_guard = self.file_id.read();
        *read_guard
    }

    pub fn read_log_record(&self, offset: u64) -> Result<LogRecord> {
        todo!()
    }

    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        todo!()
    }

    pub fn sync(&self) -> Result<()> {
        todo!()
    }
}
