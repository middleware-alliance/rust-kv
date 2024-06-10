use crate::batch::{
    log_record_key_with_seq_no, parse_log_record_key_to_seq_no, NON_TRANSACTION_SEQ_NO,
};
use crate::data::data_file::{
    get_data_file_path, DataFile, HINT_FILE_NAME, MERGE_FINISHED_FILE_NAME,
};
use crate::data::log_record::{decode_log_record_pos, LogRecord, LogRecordType};
use crate::options::Options;
use crate::{
    db::Engine,
    errors::{Errors, Result},
};
use log::error;
use std::fs;
use std::path::PathBuf;

const MERGE_DIR_NAME: &str = "merge";
const MERGE_FIN_KEY: &[u8] = "merge.finished".as_bytes();

impl Engine {
    /// Merges the data from the source database into the destination database.
    /// handle invalid data, and generate a hint index file.
    pub fn merge(&self) -> Result<()> {
        // acquire merging lock
        let lock = self.merging_lock.try_lock();
        if lock.is_none() {
            return Err(Errors::MergeInProgress);
        }

        // get merge data files
        let merge_files = self.rotate_merge_files()?;

        let merge_path = get_merge_path(self.options.dir_path.clone());
        // if merge path exists, remove it
        if merge_path.is_dir() {
            fs::remove_dir_all(merge_path.clone()).unwrap();
        }
        // create merge path
        if let Err(e) = fs::create_dir_all(merge_path.clone()) {
            return Err(Errors::FailedToCreateDatabaseDir);
        }

        // open tmp merge engine
        let mut merge_db_opts = Options::default();
        merge_db_opts.dir_path = merge_path.clone();
        merge_db_opts.data_file_size = self.options.data_file_size;
        let merge_db = Engine::open(merge_db_opts)?;

        // open hint index file
        let hint_file = DataFile::new_hint_file(merge_path.clone())?;
        // Process each data file in turn, rewriting valid data
        for data_file in merge_files.iter() {
            let mut offset = 0;
            loop {
                let (mut log_record, size) = match data_file.read_log_record(offset) {
                    Ok(result) => (result.record, result.size),
                    Err(e) => {
                        if e == Errors::ReadDataFileEOF {
                            break;
                        }

                        return Err(e);
                    }
                };

                // decode log record get real key
                let (original_key, _) = parse_log_record_key_to_seq_no(log_record.key.clone());
                if let Some(index_pos) = self.index.get(original_key.clone()) {
                    // if key in index, write to merge db
                    if index_pos.file_id == data_file.get_file_id() && index_pos.offset == offset {
                        // remove transaction id
                        log_record.key =
                            log_record_key_with_seq_no(original_key.clone(), NON_TRANSACTION_SEQ_NO);
                        let log_record_pos = merge_db.append_log_record(&mut log_record)?;
                        // write hint index
                        hint_file.write_hint_record(original_key.clone(), log_record_pos)?;
                    }
                }

                // move offset to next log record
                offset += size as u64;
            }
        }

        // sync merge db
        merge_db.sync();
        hint_file.sync();

        // Get file id who has not participated in the merge recently
        let non_merge_file_id = merge_files.last().unwrap().get_file_id() + 1;
        let merge_fin_file = DataFile::new_merge_fin_file(merge_path.clone())?;
        let merge_fin_record = LogRecord {
            key: MERGE_FIN_KEY.to_vec(),
            value: non_merge_file_id.to_string().into_bytes(),
            rec_type: LogRecordType::NORMAL,
        };
        let enc_record = merge_fin_record.encode();
        merge_fin_file.write(&enc_record)?;
        merge_fin_file.sync()?;

        Ok(())
    }

    fn rotate_merge_files(&self) -> Result<Vec<DataFile>> {
        // get older files id
        let mut merge_file_ids = Vec::new();
        let mut older_files = self.older_files.write();
        for fid in older_files.keys() {
            merge_file_ids.push(*fid);
        }

        // set active file to the oldest file
        let mut active_file = self.active_file.write();
        // sync active file
        active_file.sync();
        let active_file_id = active_file.get_file_id();
        let new_active_file = DataFile::new(self.options.dir_path.clone(), active_file_id + 1)?;
        *active_file = new_active_file;

        // add older files to merge list
        let old_file = DataFile::new(self.options.dir_path.clone(), active_file_id)?;
        older_files.insert(active_file_id, old_file);

        // add merge file id to merge list
        merge_file_ids.push(active_file_id);
        // sort merge file ids, from small to small
        merge_file_ids.sort();

        // open all need merge files
        let mut merge_files = Vec::new();
        for fid in merge_file_ids.iter() {
            let data_file = DataFile::new(self.options.dir_path.clone(), *fid)?;
            merge_files.push(data_file);
        }

        Ok(merge_files)
    }

    /// load index from hint file
    pub(crate) fn load_index_from_hint_file(&self) -> Result<()> {
        let hint_file_name = self.options.dir_path.join(HINT_FILE_NAME);
        // if hint file not exists, return
        if !hint_file_name.is_file() {
            return Ok(());
        }

        let hint_file = DataFile::new_hint_file(self.options.dir_path.clone())?;
        let mut offset = 0;
        loop {
            let (log_record, size) = match hint_file.read_log_record(offset) {
                Ok(result) => (result.record, result.size),
                Err(e) => {
                    if e == Errors::ReadDataFileEOF {
                        break;
                    }
                    return Err(e);
                }
            };

            // decode log record value to get log record pos
            let log_record_pos = decode_log_record_pos(log_record.value);
            // store memory index
            self.index.put(log_record.key, log_record_pos);
            offset += size as u64;
        }
        Ok(())
    }

    /// load merge files directly from disk
    pub(crate) fn load_merge_files(dir_path: PathBuf) -> Result<()> {
        let merge_path = get_merge_path(dir_path.clone());
        // if merge path not exists, return
        if !merge_path.is_dir() {
            return Ok(());
        }

        let dir = match fs::read_dir(merge_path.clone()) {
            Ok(dir) => dir,
            Err(e) => {
                error!("failed to read merge dir: {}", e);
                return Err(Errors::FailedToReadDatabaseDir);
            }
        };

        // find merge fin file
        let mut merge_finished = false;
        let mut merge_file_names = Vec::new();
        for file in dir {
            if let Ok(file) = file {
                let file_os_str = file.file_name();
                let file_name = file_os_str.to_str().unwrap();

                if file_name.ends_with(MERGE_FINISHED_FILE_NAME) {
                    merge_finished = true;
                }

                merge_file_names.push(file_name.to_string());
            }
        }

        // if merge not finished, return
        if !merge_finished {
            fs::remove_dir_all(merge_path.clone()).unwrap();
            return Ok(());
        }

        // Open the file that marks the merge completion and take out the file IDs that have not participated in the merge.
        let merge_fin_file = DataFile::new_merge_fin_file(merge_path.clone())?;
        let merge_fin_record = merge_fin_file.read_log_record(0)?;
        let v = String::from_utf8(merge_fin_record.record.value).unwrap();
        let non_merge_fid = v.parse::<u32>().unwrap();

        // find older files and remove them
        for filed_id in 0..non_merge_fid {
            let file = get_data_file_path(dir_path.clone(), filed_id);
            if file.is_file() {
                fs::remove_file(file).unwrap();
            }
        }

        // move new merge files to data dir
        for file_name in merge_file_names {
            let src_path = merge_path.join(file_name.clone());
            let dst_path = dir_path.join(file_name.clone());
            fs::rename(src_path, dst_path).unwrap();
        }

        // finally remove merge dir
        fs::remove_dir_all(merge_path.clone()).unwrap();

        Ok(())
    }
}

/// get merge file path
pub(crate) fn get_merge_path(dir_path: PathBuf) -> PathBuf {
    let file_name = dir_path.file_name().unwrap();
    let merge_name = std::format!("{}-{}", file_name.to_str().unwrap(), MERGE_DIR_NAME);
    let parent = dir_path.parent().unwrap();
    parent.to_path_buf().join(merge_name)
}
