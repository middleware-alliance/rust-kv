// LogRecordPos represents the position of a log record in a log file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}