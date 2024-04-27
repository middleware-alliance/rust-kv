use prost::length_delimiter_len;

#[derive(PartialEq)]
pub enum LogRecordType {
    // A normal log record.
    NORMAL,
    // A log record indicating that a key has been deleted.
    DELETED,
}

impl LogRecordType {
    pub fn from_u8(value: u8) -> Self {
        match value {
            1 => LogRecordType::NORMAL,
            2 => LogRecordType::DELETED,
            _ => panic!("Invalid log record type: {}", value),
        }
    }
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

    pub fn get_crc(&mut self) -> u32 {
        todo!()
    }
}

// LogRecordPos represents the position of a log record in a log file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}

// ReadLogRecord represents a log record that has been read from a log file.
pub struct ReadLogRecord {
    pub(crate) record: LogRecord,
    pub(crate) size: usize,
}

// max_log_record_header_size returns the maximum size of a log record header.
pub fn max_log_record_header_size() -> usize {
    std::mem::size_of::<u8>() + length_delimiter_len(u32::MAX as usize) * 2
}