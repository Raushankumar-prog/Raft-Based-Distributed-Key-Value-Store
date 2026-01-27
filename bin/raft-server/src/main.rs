mod network;

use actix_web::HttpServer;
use api::configure_app;
use kv_store::KvStore;
use network::HttpRaftNetwork;
use raft_core::RaftNode;
use std::env;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // CLI Args: raft-server <id> <peer1> <peer2> ...
    // E.g., raft-server 1 2 3 corresponds to ports 8081, 8082, 8083
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: raft-server <id> [peer_id ...]");
        return Ok(()); // Should error
    }

    let id: u64 = args[1].parse().expect("Invalid ID");
    let peers: Vec<u64> = args[2..]
        .iter()
        .map(|s| s.parse().expect("Invalid Peer"))
        .collect();
    let port = 8080 + id;

    println!(
        "Starting Raft Node {} on port {} with peers {:?}",
        id, port, peers
    );

    let store = Arc::new(KvStore::new(
        &format!("kv_{}.log", id),
        &format!("kv_{}.snap", id),
    ));
    let network = HttpRaftNetwork::new();

    // RaftNode needs to be shared:
    // 1. Ticker thread needs mutable access to run tick().
    // 2. API handlers need mutable access to handle RPCs.
    // --> Arc<Mutex<RaftNode>>
    let raft_node = RaftNode::new(id, peers, network);
    let raft = Arc::new(Mutex::new(raft_node));

    // Spawn Ticker Thread
    let raft_clone = raft.clone();
    thread::spawn(move || {
        loop {
            // Scope the lock to release it quickly
            {
                let mut node = raft_clone.lock().unwrap();
                node.tick();
            }
            thread::sleep(Duration::from_millis(50)); // Tick frequency
        }
    });

    // Start HTTP API server
    let sys = actix_web::rt::System::new();
    sys.block_on(async move {
        HttpServer::new(move || {
            let store = store.clone();
            let raft = raft.clone();
            actix_web::App::new().configure(move |cfg| configure_app(cfg, store, raft))
        })
        .bind(format!("127.0.0.1:{}", port))?
        .run()
        .await
    })
}
