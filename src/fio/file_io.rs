use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::Arc;

use log::error;
use parking_lot::RwLock;

use crate::errors::Errors;
use crate::fio::IOManager;

/// A wrapper around a file descriptor that allows for concurrent access.
pub struct FileIO {
    /// The file descriptor wrapped by this object.
    fd: Arc<RwLock<File>>,
}

impl FileIO {
    /// Create a new `FileIO` object from a file path.
    pub fn new(path: PathBuf) -> crate::errors::Result<Self> {
        // Open the file in read-write mode.
        let file = match OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .append(true)
            .open(path)
        {
            Ok(file) => file,
            Err(e) => {
                error!("create data file err: {}", e);
                return Err(Errors::FailedOpenDataFile);
            }
        };

        // Create a new `FileIO` object from the file descriptor.
        Ok(FileIO {
            fd: Arc::new(RwLock::new(file)),
        })
    }
}

impl IOManager for FileIO {
    fn read(&self, buf: &mut [u8], offset: u64) -> crate::errors::Result<usize> {
        let lock_read_guard = self.fd.read();

        // Truncate the file to the current size if it is smaller than the offset.
        match lock_read_guard.read_at(buf, offset) {
            Ok(n) => Ok(n),
            Err(e) => {
                error!("read from data file err: {}", e);
                Err(Errors::FailedReadFromDataFile)
            }
        }
    }

    fn write(&self, buf: &[u8]) -> crate::errors::Result<usize> {
        let mut lock_write_guard = self.fd.write();

        // Write buffer to the file.
        match lock_write_guard.write(buf) {
            Ok(n) => Ok(n),
            Err(e) => {
                error!("write to data file err: {}", e);
                Err(Errors::FailedWriteToDataFile)
            }
        }
    }

    fn sync(&self) -> crate::errors::Result<()> {
        let lock_write_guard = self.fd.write();

        // Sync the memory buffer to disk.
        match lock_write_guard.sync_all() {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("sync data file err: {}", e);
                Err(Errors::FailedSyncDataFile)
            }
        }
    }

    fn size(&self) -> u64 {
        let read_guard = self.fd.read();
        let metadata = read_guard.metadata().unwrap();
        metadata.len()
    }
}

/// Test the `FileIO` implementation.
/// cargo test --lib file_io
#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn test_file_io_write() {
        let buf = PathBuf::from("/tmp/test_file_io_write.txt");
        //let file_io = FileIO::new("test_file_io_write.txt").unwrap();
        let file_io = FileIO::new(buf.clone());
        assert!(file_io.is_ok());
        let file_io = file_io.ok().unwrap();

        let data = b"hello";
        let n = file_io.write(data);
        assert!(n.is_ok());
        assert_eq!(n.ok().unwrap(), data.len());

        let x = " world".as_bytes();
        let n = file_io.write(x);
        assert!(n.is_ok());
        assert_eq!(n.ok().unwrap(), x.len());

        let result = fs::remove_file(buf);
        assert!(result.is_ok());
    }

    #[test]
    fn test_file_io_read() {
        let path = PathBuf::from("/tmp/test_file_io_read.txt");
        let file_io = FileIO::new(path.clone());
        assert!(file_io.is_ok());
        let file_io = file_io.ok().unwrap();

        let data = b"hello world";
        let n = file_io.write(data);
        assert!(n.is_ok());
        assert_eq!(n.ok().unwrap(), data.len());

        let mut buf = [0u8; 11];
        let n = file_io.read(&mut buf, 0);
        assert!(n.is_ok());
        let size = n.ok().unwrap();
        assert_eq!(size.clone(), data.len());
        assert_eq!(&buf[0..size], data);

        let mut buf = [0u8; 5];
        let n = file_io.read(&mut buf, 0);
        assert!(n.is_ok());
        let size = n.ok().unwrap();
        assert_eq!(size.clone(), 5);
        assert_eq!(&buf[0..5], &data[0..5]);

        let mut buf = [0u8; 5];
        let n = file_io.read(&mut buf, 6);
        assert!(n.is_ok());
        let size = n.ok().unwrap();
        assert_eq!(size.clone(), 5);
        assert_eq!(&buf[0..5], &data[6..11]);

        let result = fs::remove_file(path);
        assert!(result.is_ok());
    }

    #[test]
    fn test_file_io_sync() {
        let path = PathBuf::from("/tmp/test_file_io_sync.txt");
        let file_io = FileIO::new(path.clone());
        assert!(file_io.is_ok());
        let file_io = file_io.ok().unwrap();

        let data = b"hello world";
        let n = file_io.write(data);
        assert!(n.is_ok());
        assert_eq!(n.ok().unwrap(), data.len());

        let result = file_io.sync();
        assert!(result.is_ok());

        let result = fs::remove_file(path);
        assert!(result.is_ok());
    }

    #[test]
    fn test_file_io_size() {
        let path = PathBuf::from("/tmp/test_file_io_size.txt");
        let fio_res = FileIO::new(path.clone());
        assert!(fio_res.is_ok());
        let fio = fio_res.ok().unwrap();

        let size1 = fio.size();
        assert_eq!(size1, 0);

        let res2 = fio.write("key-b".as_bytes());
        assert!(res2.is_ok());

        let size2 = fio.size();
        assert_eq!(size2, 5);

        let res3 = fs::remove_file(path.clone());
        assert!(res3.is_ok());
    }
}
