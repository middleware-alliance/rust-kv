use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum Errors {
    #[error("failed to read from data file")]
    FailedReadFromDataFile,

    #[error("failed to write to data file")]
    FailedWriteToDataFile,

    #[error("failed to sync data file")]
    FailedSyncDataFile,

    #[error("failed to open data file")]
    FailedOpenDataFile,

    #[error("key is empty")]
    KeyIsEmpty,

    #[error("memory index update failed")]
    IndexUpdateFailed,

    #[error("key not found")]
    KeyNotFound,

    #[error("data file not found")]
    DataFileNotFound,

    #[error("directory path can not be empty")]
    DirPathIsEmpty,

    #[error("data file size is too small")]
    DataFileSizeTooSmall,

    #[error("failed to create database directory")]
    FailedToCreateDatabaseDir,

    #[error("failed to read database directory")]
    FailedToReadDatabaseDir,

    #[error("failed to write to database directory")]
    DataDirectoryCorrupted,

    #[error("failed to read from data file: EOF")]
    ReadDataFileEOF,

    #[error("invalid crc value, log record may be corrupted")]
    InvalidLogRecordCrc,

    #[error("exceed max batch number")]
    ExceedMaxBatchNum,

    #[error("merge is in progress, try again later")]
    MergeInProgress,
}

pub type Result<T> = std::result::Result<T, Errors>;
