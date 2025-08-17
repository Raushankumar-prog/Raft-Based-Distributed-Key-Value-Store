use raft::store::KvStore;

#[test]
fn test_set_and_get() {
    let store = KvStore::new("test_kv.log");
    store.set("foo".to_string(), "bar".to_string());
    assert_eq!(store.get("foo"), Some("bar".to_string()));
}

#[test]
fn test_overwrite() {
    let store = KvStore::new("test_kv.log");
    store.set("foo".to_string(), "bar".to_string());
    store.set("foo".to_string(), "baz".to_string());
    assert_eq!(store.get("foo"), Some("baz".to_string()));
}
