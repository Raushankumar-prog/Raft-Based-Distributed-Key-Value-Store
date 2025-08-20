use raft::store::KvStore;
use std::thread;
use std::time::Duration;
use actix_web::{web, App, HttpServer};

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

// Initialize Raft node (single node for demo, use vec![2,3] for cluster)
let mut raft = RaftNode::new(1, vec![]);

// Spawn Raft background thread for ticking (election, heartbeat, etc.)
thread::spawn(move || {
    loop {
        raft.tick();
        // For demo: periodically propose a command as leader
        if raft.role == raft::RaftRole::Leader {
            raft.propose(format!("set foo {}", chrono::Utc::now().timestamp()));
        }
        thread::sleep(Duration::from_millis(100));
    }
});

// Start HTTP API server
let sys = actix_web::rt::System::new();
sys.block_on(async move {
    HttpServer::new(move || app_factory(store.clone()))
        .bind("127.0.0.1:8080")
        .unwrap()
        .run()
        .await
        .unwrap();
});
