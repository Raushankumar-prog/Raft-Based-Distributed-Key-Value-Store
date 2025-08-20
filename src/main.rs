mod api;
mod raft;
mod store;

use crate::api::app_factory;
use crate::store::KvStore;
use actix_web::HttpServer;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let store = Arc::new(KvStore::new("kv.log"));

    // Initialize Raft node (single node for demo, use vec![2,3] for cluster)
    let mut raft = raft::RaftNode::new(1, vec![]);

    // Spawn Raft background thread for ticking (election, heartbeat, etc.)
    let raft_store = store.clone();
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

    Ok(())
}
