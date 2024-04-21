use crate::data::log_record::LogRecordType::{DELETED, NORMAL};
use crate::data::log_record::{LogRecord, LogRecordPos};
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

use crate::data::data_file::DataFile;
use crate::errors::{Errors, Result};
use crate::index;
use crate::options::Options;

/// This is the engine struct, engine is responsible for managing the database.
pub struct Engine {
    options: Arc<Options>,
    // current active data file
    active_file: Arc<RwLock<DataFile>>,
    // older data files
    older_files: Arc<RwLock<HashMap<u32, DataFile>>>,
    // memory index
    index: Box<dyn index::Indexer>,
}

impl Engine {
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        // key is empty, return error
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // create a log record
        let mut log_record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            rec_type: NORMAL,
        };

        // append the log record to the active log file
        let log_record_pos = self.append_log_record(&mut log_record)?;

        // update the memory index
        let ok = self.index.put(key.to_vec(), log_record_pos);
        if !ok {
            return Err(Errors::IndexUpdateFailed);
        }

        Ok(())
    }

    /// append a log record to the active log file
    fn append_log_record(&self, log_record: &mut LogRecord) -> Result<LogRecordPos> {
        let dir_path = self.options.dir_path.clone();

        // encode the log record
        let enc_log_record = log_record.encode();
        let record_len = enc_log_record.len() as u64;

        // get the active data file
        let active_file = self.active_file.write();

        // check if the active data file is full
        if active_file.get_write_off() + record_len > active_file.get_file_size() {
            // Persistence is required, create a new data file and move the active data file to the older files
            active_file.sync()?;

            let current_fid = active_file.get_file_id();
            // get older data files
            let mut older_files = self.older_files.write();
            let old_file = DataFile::new(dir_path.clone(), active_file.get_file_id())?;
            // move the active data file to the older files
            older_files.insert(current_fid, old_file);

            // create a new active data file
            let new_active_file = DataFile::new(dir_path.clone(), current_fid + 1)?;
            *active_file = new_active_file;
        }

        // append the log record to the active data file
        let write_off = active_file.get_write_off();
        active_file.write(&enc_log_record)?;

        // if sync_writes is true, flush the data to disk
        if self.options.sync_writes {
            active_file.sync()?;
        }

        // construct the log record index
        Ok((LogRecordPos {
            file_id: active_file.get_file_id(),
            offset: write_off,
        }))
    }

    /*/// get the value of a key
    pub fn get(&self, key: Bytes) -> Result<Option<Bytes>> {
        // key is empty, return error
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // get the log record position from the memory index
        let log_record_pos = self.index.get(key.to_vec())?;

        // if the log record position is None, return None
        if log_record_pos.is_none() {
            return Ok(None);
        }

        // get the log record from the active data file
        let active_file = self.active_file.read();
        let log_record = active_file.read_log_record(log_record_pos.unwrap())?;

        // decode the log record
        let dec_log_record = LogRecord::decode(&log_record)?;

        // return the value
        Ok(Some(Bytes::from(dec_log_record.value)))
    }*/

    /// get the value of a key
    pub fn get(&self, key: Bytes) -> Result<Bytes> {
        // key is empty, return error
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // get the log record position from the memory index
        let log_record_pos = self.index.get(key.to_vec())?;

        // if the log record position is None, return None
        if log_record_pos.is_none() {
            return Err(Errors::KeyNotFound);
        }

        // from here, we have the log record position, we need to get the value from the data file
        let log_record_pos = log_record_pos.unwrap();
        // get the log record from the active data file
        let active_file = self.active_file.read();
        let older_files = self.older_files.read();

        let log_record = match active_file.get_file_id() == log_record_pos.file_id {
            true => active_file.read_log_record(log_record_pos.offset)?,
            false => {
                let older_file = older_files.get(&log_record_pos.file_id);
                // if the older file is not found, return error
                if older_file.is_none() {
                    return Err(Errors::DataFileNotFound);
                }
                older_file.unwrap().read_log_record(log_record_pos.offset)?
            }
        };

        // check if the log record is deleted
        if log_record.rec_type == DELETED {
            return Err(Errors::KeyNotFound);
        }

        // return the value
        Ok(log_record.value.into())
    }
}
