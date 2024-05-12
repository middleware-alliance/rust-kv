use crate::data::log_record::{LogRecord, LogRecordType};
use crate::db::Engine;
use crate::errors::Result;
use crate::options::WriteBatchOptions;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use bytes::{BufMut, Bytes, BytesMut};
use prost::{decode_length_delimiter, encode_length_delimiter};
use crate::data::log_record::LogRecordType::NORMAL;

const TXN_FIN_KEY:&[u8] = "txn-fin".as_bytes();
pub(crate) const NON_TRANSACTION_SEQ_NO: usize = 0;

/// A batch of writes to be applied atomically to the database.
pub struct WriteBatch<'a> {
    pending_writes: Arc<Mutex<HashMap<Vec<u8>, LogRecord>>>, // A map of keys to their corresponding LogRecord
    engine: &'a Engine,         // The engine to which the writes should be applied
    options: WriteBatchOptions, // The options for the write batch
}

impl Engine {
    /// Creates a new write batch with the given options.
    pub fn new_write_batch(&self, options: WriteBatchOptions) -> Result<WriteBatch> {
        Ok(WriteBatch {
            pending_writes: Arc::new(Mutex::new(HashMap::new())),
            engine: self,
            options,
        })
    }
}

impl  WriteBatch<'_> {

    /// Adds a put operation to the write batch.
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(crate::errors::Errors::KeyIsEmpty);
        }

        // temporary storage for the LogRecord
        let record =LogRecord{
            key: key.to_vec(),
            value: value.to_vec(),
            rec_type: NORMAL,
        };

        let mut pending_writes = self.pending_writes.lock();
        pending_writes.insert(key.to_vec(), record);
        Ok(())
    }

    /// Adds a delete operation to the write batch.
    pub fn delete(&self, key: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(crate::errors::Errors::KeyIsEmpty);
        }

        let mut pending_writes = self.pending_writes.lock();
        // if the key is not present in the map, it means it is already deleted
        let index_pos = self.engine.index.get(key.to_vec());
        if index_pos.is_none() {
            if pending_writes.contains_key(&key.to_vec()) {
                pending_writes.remove(&key.to_vec());
            }
            return Ok(());
        }

        // temporary storage for the LogRecord
        let record = LogRecord{
            key: key.to_vec(),
            value: Default::default(),
            rec_type: crate::data::log_record::LogRecordType::DELETED,
        };

        pending_writes.insert(key.to_vec(), record);
        Ok(())
    }

    /// Commits the write batch to the database.
    pub fn commit(&self) -> Result<()> {
        let mut pending_writes = self.pending_writes.lock();
        if pending_writes.len() == 0 {
            return Ok(());
        }

        if pending_writes.len() > self.options.max_batch_num {
            return Err(crate::errors::Errors::ExceedMaxBatchNum);
        }

        // lock the engine for batch commit
        let mutex_guard = self.engine.batch_commit_lock.lock();

        // fetch the current sequence number
        let seq_no = self.engine.seq_no.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let mut positions = HashMap::new();

        // write the log records to the log file
        for (_, record) in pending_writes.iter() {
            let mut log_record = LogRecord{
                key: log_record_key_with_seq_no(record.key.clone(), seq_no),
                value: record.value.clone(),
                rec_type: record.rec_type,
            };

            let pos = self.engine.append_log_record(&mut log_record);
            positions.insert(record.key.clone(), pos.unwrap());
        }

        // write fin record to the log file
        let mut fin_record = LogRecord{
            key: log_record_key_with_seq_no(TXN_FIN_KEY.to_vec(), seq_no),
            value: Default::default(),
            rec_type: LogRecordType::TXNFINISHED,
        };
        self.engine.append_log_record(&mut fin_record)?;

        // if the config requires it, flush the log file to disk
        if self.options.sync_writes {
            self.engine.sync()?;
        }

        // update the index
        for (_, item) in pending_writes.iter() {
           if item.rec_type == LogRecordType::NORMAL {
               let record_pos = positions.get(&item.key).unwrap();
               self.engine.index.put(item.key.clone(), *record_pos);
           }
            else if item.rec_type == LogRecordType::DELETED {
               self.engine.index.delete(item.key.clone());
            }
        }

        // clear the pending writes
        pending_writes.clear();

        Ok(())
    }

}

// helper function to generate the key for a log record with a sequence number
pub(crate) fn log_record_key_with_seq_no(key: Vec<u8>, seq_no: usize) -> Vec<u8> {
    let mut enc_key = BytesMut::new();
    encode_length_delimiter(seq_no, &mut enc_key).unwrap();
    enc_key.extend_from_slice(&key.to_vec());
    enc_key.to_vec()
}

// helper function to extract the sequence number from a log record key
pub(crate) fn parse_log_record_key_to_seq_no(key: Vec<u8>) -> (Vec<u8>, usize) {
    let mut dec_key = BytesMut::new();
    dec_key.put_slice(&key);
    let seq_no = decode_length_delimiter(&mut dec_key).unwrap();
    (dec_key.to_vec(), seq_no)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use crate::errors::Errors;
    use crate::options::Options;
    use crate::util;
    use super::*;

    #[test]
    fn test_write_batch() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-put");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let batch = engine.new_write_batch(WriteBatchOptions::default()).expect("failed to create write batch");
        // write some data, not commit yet
        let result = batch.put(util::rand_kv::get_test_key(1), util::rand_kv::get_test_value(10));
        assert!(result.is_ok());
        let result = batch.put(util::rand_kv::get_test_key(2), util::rand_kv::get_test_value(10));
        assert!(result.is_ok());

        let result = engine.get(util::rand_kv::get_test_key(1));
        assert_eq!(Errors::KeyNotFound, result.err().unwrap());

        // after commit, read the data back
        let result = batch.commit();
        assert!(result.is_ok());

        let result = engine.get(util::rand_kv::get_test_key(1));
        assert!(result.is_ok());
        println!("{:?}", result);

        // check transactional sequence number
        let seq_no = batch.engine.seq_no.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(2, seq_no);

        // delete some data, not commit yet
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove dir");
    }

    #[test]
    fn test_write_batch_after_restart() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-put-batch");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let batch = engine.new_write_batch(WriteBatchOptions::default()).expect("failed to create write batch");
        // write some data, not commit yet
        let result = batch.put(util::rand_kv::get_test_key(1), util::rand_kv::get_test_value(10));
        assert!(result.is_ok());
        let result = batch.put(util::rand_kv::get_test_key(2), util::rand_kv::get_test_value(20));
        assert!(result.is_ok());
        let result = batch.commit();
        assert!(result.is_ok());

        let result = batch.put(util::rand_kv::get_test_key(1), util::rand_kv::get_test_value(10));
        assert!(result.is_ok());
        let result = batch.commit();
        assert!(result.is_ok());

        // restart engine, check
        engine.close().expect("failed to close engine");
        let engine = Engine::open(opts.clone()).expect("failed to open engine");
        let keys = engine.list_keys();
        println!("{:#?}", keys);

        // check transactional sequence number
        let seq_no = batch.engine.seq_no.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(3, seq_no);
        println!("seq_no: {:}", seq_no);

        // delete some data, not commit yet
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove dir");
    }

    #[test]
    fn test_write_batch_exceed_max_batch_num() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-put-batch");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let keys = engine.list_keys().unwrap();
        println!("{:#?}", keys);
        assert_eq!(0, keys.len());

        let mut wb_opts = WriteBatchOptions::default();
        wb_opts.max_batch_num = 1000000000;
        let batch = engine.new_write_batch(wb_opts).expect("failed to create write batch");

        for i in 0..10 {
            let result = batch.put(util::rand_kv::get_test_key(i), util::rand_kv::get_test_value(10));
            assert!(result.is_ok());
        }

        // not commit yet
        //let result = batch.commit();

        let keys = engine.list_keys().unwrap();
        println!("{:#?}", keys);
        assert_eq!(0, keys.len());

        let result = engine.get(util::rand_kv::get_test_key(1));
        assert_eq!(Errors::KeyNotFound, result.err().unwrap());

        // delete some data, not commit yet
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove dir");
    }
}