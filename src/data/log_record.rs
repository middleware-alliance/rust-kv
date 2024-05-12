use bytes::{BufMut, BytesMut};
use prost::{encode_length_delimiter, length_delimiter_len};

#[derive(PartialEq, Clone, Copy, Debug)]
pub enum LogRecordType {
    // A normal log record.
    NORMAL = 1,

    // A log record indicating that a key has been deleted.
    DELETED = 2,

    // A log record indicating that a transaction has been finished.
    TXNFINISHED = 3,
}

impl LogRecordType {
    pub fn from_u8(value: u8) -> Self {
        match value {
            1 => LogRecordType::NORMAL,
            2 => LogRecordType::DELETED,
            3 => LogRecordType::TXNFINISHED,
            _ => panic!("Invalid log record type: {}", value),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) rec_type: LogRecordType,
}

impl LogRecord {
    /// encode encodes a log record into a byte slice.
    /// +---------------------------------------------------------------------------------------+
    /// | record type | key size 			   | value size     		 | key | value | crc32 |
    /// +---------------------------------------------------------------------------------------+
    /// | 1           | Variable length (max 5) | Variable length (max 5) | n   | n     | 4     |
    pub fn encode(&self) -> Vec<u8> {
        let (enc_buf, _) = self.encode_and_get_crc();
        enc_buf
    }

    /// DecodeLogRecord decodes a log record from a byte slice.
    pub fn get_crc(&self) -> u32 {
        let (_, crc_value) = self.encode_and_get_crc();
        crc_value
    }

    fn encode_and_get_crc(&self) -> (Vec<u8>, u32) {
        // init buffer bytes, store record data
        let mut buf = BytesMut::new();
        buf.reserve(self.encode_length());

        // first encode record type
        buf.put_u8(self.rec_type as u8);

        // then encode key length
        encode_length_delimiter(self.key.len(), &mut buf).unwrap();
        // then encode value length
        encode_length_delimiter(self.value.len(), &mut buf).unwrap();

        // store key and value
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);

        // calculate crc32
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf);
        let crc = hasher.finalize();
        buf.put_u32(crc);

        //println!("crc: {:}", crc);

        // return encoded bytes and crc
        (buf.to_vec(), crc)
    }

    /// decode_length decodes the length of a log record from a byte slice.
    fn encode_length(&self) -> usize {
        std::mem::size_of::<u8>() + // record type
            length_delimiter_len(self.key.len()) + // key length
            length_delimiter_len(self.value.len()) + // key length
            self.key.len() +  // key size
            self.value.len() +  // key size
            std::mem::size_of::<u32>()  // crc32
    }
}

/// LogRecordPos represents the position of a log record in a log file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}

/// ReadLogRecord represents a log record that has been read from a log file.
#[derive(Debug, Clone, PartialEq)]
pub struct ReadLogRecord {
    pub(crate) record: LogRecord,
    pub(crate) size: usize,
}

/// TransactionRecord represents a log record that is part of a transaction.
#[derive(Debug, Clone, PartialEq)]
pub struct TransactionRecord {
    pub(crate) record: LogRecord,
    pub(crate) pos: LogRecordPos,
}

/// max_log_record_header_size returns the maximum size of a log record header.
pub fn max_log_record_header_size() -> usize {
    std::mem::size_of::<u8>() + length_delimiter_len(u32::MAX as usize) * 2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_record_encode_decode() {
        // normal log record
        let log_record = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: "bitcask-rs".as_bytes().to_vec(),
            rec_type: LogRecordType::NORMAL,
        };
        let enc = log_record.encode();
        println!("{:?}", enc);
        assert!(enc.len() > 5);
        assert_eq!(1020360578, log_record.get_crc());

        // null value log record
        let log_record = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: Default::default(),
            rec_type: LogRecordType::NORMAL,
        };
        let enc = log_record.encode();
        println!("{:?}", enc);
        assert!(enc.len() > 5);
        assert_eq!(3756865478, log_record.get_crc());

        // deleted type log record
        let log_record = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: "bitcask-rs".as_bytes().to_vec(),
            rec_type: LogRecordType::DELETED,
        };

        let enc = log_record.encode();
        println!("{:?}", enc);
        assert!(enc.len() > 5);
        assert_eq!(1867197446, log_record.get_crc());
    }

}
