use bytes::Bytes;

pub fn get_test_key(i: usize) -> Bytes {
    Bytes::from(format!("bitcask-rust-key-{:09}", i))
}

pub fn get_test_value(i: usize) -> Bytes {
    Bytes::from(format!("bitcask-rust-value-{:09}", i))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_test_key() {
        assert_eq!(get_test_key(123456789), Bytes::from("bitcask-rust-key-123456789"));

        for i in 0..10 {
            assert_eq!(get_test_key(i), Bytes::from(format!("bitcask-rust-key-{:09}", i)));
        }
    }

    #[test]
    fn test_get_test_value() {
        assert_eq!(get_test_value(123456789), Bytes::from("bitcask-rust-value-123456789"));

        for i in 0..10 {
            assert_eq!(get_test_value(i), Bytes::from(format!("bitcask-rust-value-{:09}", i)));
        }
    }
}