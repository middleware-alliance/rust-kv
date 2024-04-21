mod file_io;

use crate::errors::Result;

/// IOManager is an interface for managing I/O operations.
pub trait IOManager: Send + Sync {
    /// Read data from the device into the given buffer at the given offset.
    /// Returns the number of bytes read.
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
    /// Write data from the given buffer to the device.
    /// Returns the number of bytes written.
    fn write(&self, buf: &[u8]) -> Result<usize>;
    /// Synchronize the device's internal cache with the underlying storage.
    /// This ensures that data written to the device is immediately available for reading.
    fn sync(&self) -> Result<()>;
}
