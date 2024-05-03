use rust_kv::{db, options::Options};
use bytes::Bytes;

fn main() {
    let opts = Options::default();
    let engine = db::Engine::open(opts).expect("failed to open bitcask engine");

    let result = engine.put(Bytes::from("name"), Bytes::from("bitcask-rs"));
    assert!(result.is_ok());

    let result = engine.get(Bytes::from("name"));
    assert!(result.is_ok());
    let value = result.unwrap();
    assert_eq!(value.clone(), Bytes::from("bitcask-rs"));
    println!("result: {:?}", value);

    let result = engine.delete(Bytes::from("name"));
    assert!(result.is_ok());
}