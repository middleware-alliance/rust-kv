#[derive(PartialEq)]
pub enum LogRecordType {
    // A normal log record.
    NORMAL,
    // A log record indicating that a key has been deleted.
    DELETED,
}

pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) rec_type: LogRecordType,
}

impl LogRecord {
    pub fn encode(&mut self) -> Vec<u8> {
        todo!()
    }
}

// LogRecordPos represents the position of a log record in a log file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}
