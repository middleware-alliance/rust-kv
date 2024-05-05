use crate::db::Engine;
use crate::errors::Result;
use crate::index::IndexIterator;
use crate::options::IteratorOptions;
use bytes::Bytes;
use parking_lot::RwLock;
use std::sync::Arc;

pub struct Iterator<'a> {
    index_iter: Arc<RwLock<Box<dyn IndexIterator>>>, // index iterator
    engine: &'a Engine,                              // engine reference
}

impl Engine {
    // create a new iterator
    pub fn iter(&self, options: IteratorOptions) -> Iterator {
        Iterator {
            index_iter: Arc::new(RwLock::new(self.index.iterator(options))),
            engine: self,
        }
    }

    // list all keys in the engine
    pub fn list_keys(&self) -> Result<Vec<Bytes>> {
        self.index.list_keys()
    }

    pub fn fold<F>(&self, mut f: F) -> Result<()>
    where
        Self: Sized,
        F: FnMut(Bytes, Bytes) -> bool,
    {
        let mut iterator = self.iter(IteratorOptions::default());
        while let Some((key, value)) = iterator.next() {
            if !f(key, value) {
                break;
            }
        }

        Ok(())
    }
}

impl Iterator<'_> {
    /// rewind the iterator to the beginning of the index
    pub fn rewind(&self) {
        self.index_iter.write().rewind();
    }

    /// seek to the specified key
    pub fn seek(&self, key: Vec<u8>) {
        self.index_iter.write().seek(key);
    }

    /// move to the next entry
    pub fn next(&mut self) -> Option<(Bytes, Bytes)> {
        let mut write_guard = self.index_iter.write();
        if let Some(item) = write_guard.next() {
            let value = self
                .engine
                .get_value_by_position(item.1)
                .expect("failed to get value from data file");
            return Some((Bytes::from(item.0.to_vec()), value));
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::db::Engine;
    use crate::options::{IteratorOptions, Options};
    use crate::util;
    use bytes::Bytes;
    use std::path::PathBuf;

    #[test]
    fn test_iterator_list_keys() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-iterator-list-keys");
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        // no data in engine
        let keys = engine.list_keys().expect("failed to list keys");
        assert_eq!(keys.len(), 0);

        // add multiple data
        let result = engine.put(Bytes::from("bbcc"), util::rand_kv::get_test_value(100));
        assert!(result.is_ok());
        let result = engine.put(Bytes::from("ccc"), util::rand_kv::get_test_value(100));
        assert!(result.is_ok());
        let result = engine.put(Bytes::from("dd"), util::rand_kv::get_test_value(100));
        assert!(result.is_ok());
        let result = engine.put(Bytes::from("ee"), util::rand_kv::get_test_value(100));
        assert!(result.is_ok());
        let result = engine.put(Bytes::from("ff"), util::rand_kv::get_test_value(100));
        assert!(result.is_ok());

        let keys = engine.list_keys().expect("failed to list keys");
        assert_eq!(keys.len(), 5);
        assert!(keys.contains(&Bytes::from("bbcc")));
        assert!(keys.contains(&Bytes::from("ccc")));
        assert!(keys.contains(&Bytes::from("dd")));
        assert!(keys.contains(&Bytes::from("ee")));
        assert!(keys.contains(&Bytes::from("ff")));

        // delete file folder
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove dir");
    }

    #[test]
    fn test_iterator_fold() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-iterator-fold");
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        // add multiple data
        let result = engine.put(Bytes::from("bbcc"), util::rand_kv::get_test_value(100));
        assert!(result.is_ok());
        let result = engine.put(Bytes::from("ccc"), util::rand_kv::get_test_value(100));
        assert!(result.is_ok());
        let result = engine.put(Bytes::from("dd"), util::rand_kv::get_test_value(100));
        assert!(result.is_ok());
        let result = engine.put(Bytes::from("ee"), util::rand_kv::get_test_value(100));
        assert!(result.is_ok());
        let result = engine.put(Bytes::from("ff"), util::rand_kv::get_test_value(100));
        assert!(result.is_ok());

        let mut count = 0;
        engine
            .fold(|key, value| {
                count += 1;
                assert!(key.len() > 0);
                assert!(value.len() > 0);
                true
            })
            .expect("failed to fold");
        assert_eq!(count, 5);

        engine
            .fold(|key, value| {
                println!("key: {:?}, value: {:?}", key, value);
                if key.ge(&"bb") {
                    return false;
                }
                return true;
            })
            .expect("failed to fold");

        // delete file folder
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove dir");
    }

    #[test]
    fn test_iterator_seek() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-iterator-seek");
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        // no data in engine
        let mut iter = engine.iter(IteratorOptions::default());
        assert!(iter.next().is_none());

        // add one data
        let result = engine.put(Bytes::from("aacc"), util::rand_kv::get_test_value(100));
        assert!(result.is_ok());
        let mut iterator = engine.iter(IteratorOptions::default());
        iterator.seek("a".as_bytes().to_vec());
        assert!(iterator.next().is_some());

        // add multiple data
        let result = engine.put(Bytes::from("bbcc"), util::rand_kv::get_test_value(100));
        assert!(result.is_ok());
        let result = engine.put(Bytes::from("ccc"), util::rand_kv::get_test_value(100));
        assert!(result.is_ok());
        let result = engine.put(Bytes::from("dd"), util::rand_kv::get_test_value(100));
        assert!(result.is_ok());
        let result = engine.put(Bytes::from("ee"), util::rand_kv::get_test_value(100));
        assert!(result.is_ok());
        let result = engine.put(Bytes::from("ff"), util::rand_kv::get_test_value(100));
        assert!(result.is_ok());

        let mut iterator = engine.iter(IteratorOptions::default());
        iterator.seek("bbcc".as_bytes().to_vec());
        assert!(iterator.next().is_some());

        // delete file folder
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove dir");
    }

    #[test]
    fn test_iterator_next() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-iterator-next");
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        // add multiple data
        let result = engine.put(Bytes::from("bbcc"), util::rand_kv::get_test_value(100));
        assert!(result.is_ok());
        let result = engine.put(Bytes::from("ccc"), util::rand_kv::get_test_value(100));
        assert!(result.is_ok());
        let result = engine.put(Bytes::from("dd"), util::rand_kv::get_test_value(100));
        assert!(result.is_ok());
        let result = engine.put(Bytes::from("ee"), util::rand_kv::get_test_value(100));
        assert!(result.is_ok());
        let result = engine.put(Bytes::from("ff"), util::rand_kv::get_test_value(100));
        assert!(result.is_ok());

        let mut iterator = engine.iter(IteratorOptions::default());
        iterator.seek("bbcc".as_bytes().to_vec());
        assert_eq!(iterator.next().unwrap().0, "bbcc".as_bytes());
        assert_eq!(iterator.next().unwrap().0, "ccc".as_bytes());
        assert_eq!(iterator.next().unwrap().0, "dd".as_bytes());
        assert_eq!(iterator.next().unwrap().0, "ee".as_bytes());
        assert_eq!(iterator.next().unwrap().0, "ff".as_bytes());
        assert!(iterator.next().is_none());

        // delete file folder
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove dir");
    }

    #[test]
    fn test_iterator_prefix() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-iterator-prefix");
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        // add multiple data
        let result = engine.put(Bytes::from("bbcc"), util::rand_kv::get_test_value(100));
        assert!(result.is_ok());
        let result = engine.put(Bytes::from("ccc"), util::rand_kv::get_test_value(100));
        assert!(result.is_ok());
        let result = engine.put(Bytes::from("dd"), util::rand_kv::get_test_value(100));
        assert!(result.is_ok());
        let result = engine.put(Bytes::from("ee"), util::rand_kv::get_test_value(100));
        assert!(result.is_ok());
        let result = engine.put(Bytes::from("ff"), util::rand_kv::get_test_value(100));
        assert!(result.is_ok());

        let mut options = IteratorOptions::default();
        options.prefix = "bb".as_bytes().to_vec();
        let mut iterator = engine.iter(options);
        iterator.seek("bb".as_bytes().to_vec());
        assert_eq!(iterator.next().unwrap().0, "bbcc".as_bytes());
        assert!(iterator.next().is_none());

        // delete file folder
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove dir");
    }
}
