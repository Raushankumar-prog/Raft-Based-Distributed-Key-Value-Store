use kv_store::KvStore;
use std::fs;

#[test]
fn test_kv_set_and_get() {
    let log_path = "test_kv_1.log";
    let snap_path = "test_kv_1.snap";
    let _ = fs::remove_file(log_path);
    let _ = fs::remove_file(snap_path);

    let store = KvStore::new(log_path, snap_path).unwrap();
    store.set("foo".to_string(), "bar".to_string()).unwrap();
    assert_eq!(store.get("foo").unwrap(), Some("bar".to_string()));

    let _ = fs::remove_file(log_path);
    let _ = fs::remove_file(snap_path);
}

#[test]
fn test_kv_overwrite() {
    let log_path = "test_kv_2.log";
    let snap_path = "test_kv_2.snap";
    let _ = fs::remove_file(log_path);
    let _ = fs::remove_file(snap_path);

    let store = KvStore::new(log_path, snap_path).unwrap();
    store.set("foo".to_string(), "bar".to_string()).unwrap();
    store.set("foo".to_string(), "baz".to_string()).unwrap();
    assert_eq!(store.get("foo").unwrap(), Some("baz".to_string()));

    let _ = fs::remove_file(log_path);
    let _ = fs::remove_file(snap_path);
}
