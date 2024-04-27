use std::path::PathBuf;
use std::sync::Arc;
use bytes::{Buf, BytesMut};

use parking_lot::RwLock;
use prost::{decode_length_delimiter, length_delimiter_len};

use crate::data::log_record::{LogRecord, LogRecordType, max_log_record_header_size, ReadLogRecord};
use crate::errors::{Errors, Result};
use crate::fio;
use crate::fio::new_io_manager;

pub const DATA_FILE_NAME_SUFFIX: &str = ".data";

pub struct DataFile {
    file_id: Arc<RwLock<u32>>, // file_id is a shared variable that is accessed by multiple threads
    write_off: Arc<RwLock<u64>>, // write_off is a shared variable that is accessed by multiple threads, and it is used to keep track of the current write offset in the file
    io_manager: Box<dyn fio::IOManager>, // io_manager is a trait object that is used to perform I/O operations on the file
}

impl DataFile {
    /// Creates a new DataFile object with the given file_id and io_manager.
    pub fn new(dir_path: PathBuf, file_id: u32) -> Result<DataFile> {
        // Create the data file if it doesn't exist
        let file_name = get_data_file_path(dir_path, file_id);
        // init io manager
        let io_manager = new_io_manager(file_name)?;

        Ok(
            DataFile {
                file_id: Arc::new(RwLock::new(file_id)),
                write_off: Arc::new(RwLock::new(0)),
                io_manager: Box::new(io_manager),
            }
        )
    }

    pub fn get_write_off(&self) -> u64 {
        let read_guard = self.write_off.read();
        *read_guard
    }

    pub fn set_write_off(&self, off: u64) {
        let mut write_guard = self.write_off.write();
        *write_guard = off;
    }

    pub fn get_file_id(&self) -> u32 {
        let read_guard = self.file_id.read();
        *read_guard
    }

    /// Reads a log record from the data file at the given offset.
    pub fn read_log_record(&self, offset: u64) -> Result<ReadLogRecord> {
        // read header of log record
        let mut header_buf = BytesMut::zeroed(max_log_record_header_size());

        self.io_manager.read(&mut header_buf, offset)?;

        // parse header, get first bytes of payload (Type)
        let rec_type = header_buf.get_u8();

        // read payload of log record, get key and value
        let key_size = decode_length_delimiter(&mut header_buf).unwrap();
        let value_size = decode_length_delimiter(&mut header_buf).unwrap();

        // if key_size and value_size are 0, then read the end of the log record
        if key_size == 0 && value_size == 0 {
            return Err(Errors::ReadDataFileEOF);
        }

        // type key_size value_size
        // get actual header size
        let actual_header_size = length_delimiter_len(key_size) + length_delimiter_len(value_size) + 1;
        // get actual payload size
        let actual_payload_size = key_size + value_size;
        // body crc32
        let mut kv_buf = BytesMut::zeroed(actual_payload_size + 4);
        self.io_manager.read(&mut kv_buf, offset + actual_header_size as u64)?;

        // parse key and value
        let mut log_record = LogRecord {
            key: kv_buf.get(..key_size).unwrap().to_vec(),
            value: kv_buf.get(key_size..kv_buf.len() - 4).unwrap().to_vec(),
            rec_type: LogRecordType::from_u8(rec_type),
        };

        // read crc32
        kv_buf.advance(key_size + value_size);
        if kv_buf.get_u32() != log_record.get_crc() {
            return Err(Errors::InvalidLogRecordCrc);
        }

        // return log record and actual sizes
        Ok(ReadLogRecord {
            record: log_record,
            size: actual_header_size + actual_payload_size + 4,
        })
    }

    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        let n_bytes = self.io_manager.write(buf)?;
        // update write offset
        let mut write_guard = self.write_off.write();
        *write_guard += n_bytes as u64;
        Ok(n_bytes)
    }

    pub fn sync(&self) -> Result<()> {
        self.io_manager.sync()
    }
}

/// Returns the path of the data file with the given file_id in the given directory.
fn get_data_file_path(dir_path: PathBuf, file_id: u32) -> PathBuf {
    let name = std::format!("{:09}", file_id) + DATA_FILE_NAME_SUFFIX;
    dir_path.join(name)
    //let file_name = dir_path.to_path_buf().join(v);
    //String::from(file_name.to_str().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_data_file() {
        let dir_path= std::env::temp_dir();
        let data_file_res = DataFile::new(dir_path.clone(), 0);
        assert!(data_file_res.is_ok());

        let data_file = data_file_res.unwrap();
        assert_eq!(data_file.get_file_id(), 0);

        println!("temp dir: {:?}", dir_path.clone().as_os_str());

        // again with same file_id
        let data_file_res = DataFile::new(dir_path.clone(), 0);
        assert!(data_file_res.is_ok());


        // again with different file_id
        let data_file_res = DataFile::new(dir_path.clone(), 1);
        assert!(data_file_res.is_ok());

        let data_file = data_file_res.unwrap();
        assert_eq!(data_file.get_file_id(), 1);
    }

    #[test]
    fn test_data_file_write() {
        let dir_path= std::env::temp_dir();
        let data_file_res = DataFile::new(dir_path.clone(), 0);
        assert!(data_file_res.is_ok());


        let data_file = data_file_res.unwrap();
        let data = b"hello world";
        let n_bytes = data_file.write(data).unwrap();
        assert_eq!(n_bytes, 11);

        let data = b"hello fufeng";
        let n_bytes = data_file.write(data).unwrap();
        assert_eq!(n_bytes, 12);
    }

    #[test]
    fn test_data_file_sync() {
        let dir_path= std::env::temp_dir();
        let data_file_res = DataFile::new(dir_path.clone(), 0);
        assert!(data_file_res.is_ok());


        let data_file = data_file_res.unwrap();
        data_file.sync().unwrap();
    }

}