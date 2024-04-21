use thiserror::Error;

#[derive(Error, Debug)]
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
}

pub type Result<T> = std::result::Result<T, Errors>;
