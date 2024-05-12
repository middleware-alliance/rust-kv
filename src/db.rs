use crate::batch::{
    log_record_key_with_seq_no, parse_log_record_key_to_seq_no, NON_TRANSACTION_SEQ_NO,
};
use crate::data::log_record::LogRecordType::{DELETED, NORMAL};
use crate::data::log_record::{LogRecord, LogRecordPos, LogRecordType, TransactionRecord};
use bytes::Bytes;
use log::warn;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::ptr::read;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::data::data_file::{DataFile, DATA_FILE_NAME_SUFFIX};
use crate::errors::{Errors, Result};
use crate::index;
use crate::options::Options;

const INITIAL_FILE_ID: u32 = 0;

/// This is the engine struct, engine is responsible for managing the database.
pub struct Engine {
    pub(crate) options: Arc<Options>,
    // current active data file
    pub(crate) active_file: Arc<RwLock<DataFile>>,
    // older data files
    pub(crate) older_files: Arc<RwLock<HashMap<u32, DataFile>>>,
    // memory index
    pub(crate) index: Box<dyn index::Indexer>,
    // created by loadDataFiles(), only used for loadIndexFromDataFiles(), not used in other methods
    pub(crate) load_data_file_ids: Vec<u32>,
    // batch commit lock for batch commit operations to avoid race conditions
    pub(crate) batch_commit_lock: Mutex<()>,
    // sequence number for batch commit operations
    pub(crate) seq_no: Arc<AtomicUsize>,
}

impl Engine {
    /// create a new bitcask engine instance
    pub fn open(options: Options) -> Result<Self> {
        // check options
        if let Some(e) = check_options(&options) {
            return Err(e);
        }

        // create the directory if it does not exist
        let opts = options.clone();

        let dir_path = opts.dir_path.clone();
        if !dir_path.is_dir() {
            if let Err(e) = fs::create_dir_all(dir_path.as_path()) {
                warn!("create directory failed: {}", e);
                return Err(Errors::FailedToCreateDatabaseDir);
            }
        }

        // load the data files from the directory
        let mut data_files = load_data_files(dir_path.clone())?;

        // set file ids for the data files
        let mut file_ids = Vec::new();
        for data_file in data_files.iter() {
            file_ids.push(data_file.get_file_id());
        }

        // reverse the data files to get the latest one as the active data file
        data_files.reverse();
        // set old file ids for the data files
        let mut older_files = HashMap::new();
        if data_files.len() > 1 {
            for _ in 0..=data_files.len() - 2 {
                let file = data_files.pop().unwrap();
                older_files.insert(file.get_file_id(), file);
            }
        }

        // get the active data file
        let active_file = match data_files.pop() {
            None => DataFile::new(dir_path.clone(), INITIAL_FILE_ID)?,
            Some(v) => v,
        };

        // construct the engine instance
        let engine = Self {
            options: Arc::new(opts),
            active_file: Arc::new(RwLock::new(active_file)),
            older_files: Arc::new(RwLock::new(older_files)),
            index: Box::new(index::new_indexer(options.index_type)),
            load_data_file_ids: file_ids,
            batch_commit_lock: Mutex::new(()),
            seq_no: Arc::new(AtomicUsize::new(1)),
        };

        // load the memory index from the data files
        let current_seq_no = engine.load_index_from_data_files()?;

        // update the sequence number for batch commit operations
        if current_seq_no > 0 {
            engine.seq_no.store(current_seq_no + 1, Ordering::SeqCst);
        }

        Ok(engine)
    }

    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        // key is empty, return error
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // create a log record
        let mut log_record = LogRecord {
            key: log_record_key_with_seq_no(key.to_vec(), NON_TRANSACTION_SEQ_NO),
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

    /// delete a key
    pub fn delete(&self, key: Bytes) -> Result<()> {
        // key is empty, return error
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // key existence check
        let pos = self.index.get(key.to_vec());
        if pos.is_none() {
            return Ok(());
        }

        // create a log record
        let mut log_record = LogRecord {
            key: log_record_key_with_seq_no(key.to_vec(), NON_TRANSACTION_SEQ_NO),
            value: Default::default(),
            rec_type: DELETED,
        };

        // append the log record to the active log file
        self.append_log_record(&mut log_record)?;

        // update the memory index
        let ok = self.index.delete(key.to_vec());
        if !ok {
            return Err(Errors::IndexUpdateFailed);
        }

        Ok(())
    }

    /// append a log record to the active log file
    pub(crate) fn append_log_record(&self, log_record: &mut LogRecord) -> Result<LogRecordPos> {
        let dir_path = self.options.dir_path.clone();

        // encode the log record
        let enc_log_record = log_record.encode();
        let record_len = enc_log_record.len() as u64;

        // get the active data file
        let mut active_file = self.active_file.write();

        // check if the active data file is full
        if active_file.get_write_off() + record_len > self.options.data_file_size {
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
        let log_record_pos = self.index.get(key.to_vec());

        // if the log record position is None, return None
        if log_record_pos.is_none() {
            return Err(Errors::KeyNotFound);
        }

        /*// get the log record position from the memory index
        let log_record_pos = self.index.get(key.to_vec());

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
            true => active_file.read_log_record(log_record_pos.offset)?.record,
            false => {
                let older_file = older_files.get(&log_record_pos.file_id);
                // if the older file is not found, return error
                if older_file.is_none() {
                    return Err(Errors::DataFileNotFound);
                }
                older_file
                    .unwrap()
                    .read_log_record(log_record_pos.offset)?
                    .record
            }
        };

        // check if the log record is deleted
        if log_record.rec_type == DELETED {
            return Err(Errors::KeyNotFound);
        }

        // return the value
        Ok(log_record.value.into())*/
        // from here, we have the log record position, we need to get the value from the data file
        let log_record_pos = log_record_pos.unwrap();
        // get the log record from the active data file
        self.get_value_by_position(&log_record_pos)
    }

    /// get the value of a key by log record position
    pub(crate) fn get_value_by_position(&self, log_record_pos: &LogRecordPos) -> Result<Bytes> {
        // get the log record from the active data file
        let active_file = self.active_file.read();
        let older_files = self.older_files.read();

        let log_record = match active_file.get_file_id() == log_record_pos.file_id {
            true => active_file.read_log_record(log_record_pos.offset)?.record,
            false => {
                let older_file = older_files.get(&log_record_pos.file_id);
                // if the older file is not found, return error
                if older_file.is_none() {
                    return Err(Errors::DataFileNotFound);
                }
                older_file
                    .unwrap()
                    .read_log_record(log_record_pos.offset)?
                    .record
            }
        };

        // check if the log record is deleted
        if log_record.rec_type == DELETED {
            return Err(Errors::KeyNotFound);
        }

        // return the value
        Ok(log_record.value.into())
    }

    // load the memory index from the data files
    fn load_index_from_data_files(&self) -> Result<usize> {
        let mut current_seq_no = NON_TRANSACTION_SEQ_NO;

        // data files is empty, return
        if self.load_data_file_ids.is_empty() {
            return Ok(current_seq_no);
        }

        // pending data files to load
        let mut transaction_records = HashMap::new();

        let active_file = self.active_file.read();
        let older_files = self.older_files.read();

        // iterate over the data files and load the index
        for (idx, file_id) in self.load_data_file_ids.iter().enumerate() {
            let mut offset = 0;

            loop {
                let log_record_res = match *file_id == active_file.get_file_id() {
                    true => active_file.read_log_record(offset),
                    false => {
                        let older_file = older_files.get(&file_id).unwrap();
                        // if the older file is not found, return error
                        /*if older_file.is_none() {
                            return Err(Errors::DataFileNotFound);
                        }*/
                        older_file.read_log_record(offset)
                    }
                };

                let (mut log_record, size) = match log_record_res {
                    Ok(result) => (result.record, result.size),
                    Err(e) => {
                        if e == Errors::ReadDataFileEOF {
                            break;
                        }

                        return Err(e);
                    }
                };

                // create memory index
                let log_record_pos = LogRecordPos {
                    file_id: *file_id,
                    offset,
                };

                // parse the log record key and sequence number
                let (real_key, seq_no) = parse_log_record_key_to_seq_no(log_record.key.clone());
                // no transaction commit, update the index
                if seq_no == NON_TRANSACTION_SEQ_NO {
                    self.update_index(real_key, log_record.rec_type, log_record_pos);
                } else {
                    if log_record.rec_type == LogRecordType::TXNFINISHED {
                        let records: &Vec<TransactionRecord> = transaction_records.get(&seq_no).unwrap();
                        for txn_record in records.iter() {
                            self.update_index(txn_record.record.key.clone(), txn_record.record.rec_type, txn_record.pos);
                        }
                        transaction_records.remove(&seq_no);
                    } else {
                        log_record.key = real_key;
                        transaction_records
                            .entry(seq_no)
                            .or_insert(Vec::new())
                            .push(TransactionRecord {
                                record: log_record,
                                pos: log_record_pos,
                            });
                    }
                }

                /*let ok = match log_record.rec_type {
                    NORMAL => self.index.put(log_record.key.to_vec(), log_record_pos),
                    DELETED => self.index.delete(log_record.key.to_vec()),
                };
                if !ok {
                    return Err(Errors::IndexUpdateFailed);
                }*/

                // update the current sequence number
                if seq_no > current_seq_no {
                    current_seq_no = seq_no;
                }

                // update the offset
                offset += size as u64;
            }

            // set the active file id
            if idx == self.load_data_file_ids.len() - 1 {
                active_file.set_write_off(offset);
            }
        }

        Ok(current_seq_no)
    }

    /// update the memory index for a log record
    fn update_index(&self, key: Vec<u8>, rec_type: LogRecordType, log_record_pos: LogRecordPos) {
        if rec_type == LogRecordType::NORMAL {
            self.index.put(key.clone(), log_record_pos);
        }
        if rec_type == LogRecordType::DELETED {
            self.index.delete(key);
        }
    }

    /// close the engine and sync the active data file
    pub fn close(&self) -> Result<()> {
        // sync the active data file
        let active_file = self.active_file.read();
        active_file.sync()
    }

    /// sync the active data file
    pub fn sync(&self) -> Result<()> {
        // sync the active data file
        let active_file = self.active_file.read();
        active_file.sync()
    }
}

/// check the options for errors
fn check_options(opts: &Options) -> Option<Errors> {
    let dir_path = opts.dir_path.to_str();
    if dir_path.is_none() || dir_path.unwrap().len() == 0 {
        return Some(Errors::DirPathIsEmpty);
    }

    if opts.data_file_size <= 0 {
        return Some(Errors::DataFileSizeTooSmall);
    }

    None
}

/// load the data files from the directory
fn load_data_files(dir_path: PathBuf) -> Result<Vec<DataFile>> {
    // read the directory
    let dir = fs::read_dir(dir_path.clone());
    if dir.is_err() {
        return Err(Errors::FailedToReadDatabaseDir);
    }

    let mut file_ids = Vec::new();
    let mut data_files = Vec::new();
    for file in dir.unwrap() {
        if let Ok(entry) = file {
            // get the file name
            let file_os_str = entry.file_name();
            let file_name = file_os_str.to_str().unwrap();

            // check if the file name ends with DATA_FILE_NAME_SUFFIX
            if file_name.ends_with(DATA_FILE_NAME_SUFFIX) {
                let split_names: Vec<&str> = file_name.split(".").collect();
                let file_id = match split_names[0].parse::<u32>() {
                    Ok(fid) => fid,
                    Err(_) => return Err(Errors::DataDirectoryCorrupted),
                };
                file_ids.push(file_id);
            }
        }
    }

    // is empty, return empty vector
    if file_ids.is_empty() {
        return Ok(data_files);
    }

    // sort the file ids
    file_ids.sort();

    // iterate over the file ids and create data files
    for file_id in file_ids.iter() {
        let data_file = DataFile::new(dir_path.clone(), *file_id)?;
        data_files.push(data_file);
    }

    Ok(data_files)
}
