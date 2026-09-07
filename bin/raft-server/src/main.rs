mod network;

use actix_web::HttpServer;
use api::configure_app;
use kv_store::KvStore;
use network::HttpRaftNetwork;
use raft_core::{MemStorage, RaftNode};
use std::env;
use std::sync::Arc;
use tokio::sync::mpsc;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: raft-server <id> [peer_id ...]");
        std::process::exit(1);
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

    let store = Arc::new(
        KvStore::new(&format!("kv_{}.log", id), &format!("kv_{}.snap", id))
            .expect("Failed to initialize KvStore"),
    );
    let network = HttpRaftNetwork::new();
    let storage = MemStorage::new();

    let (apply_tx, mut apply_rx) = mpsc::channel(100);

    // Spawn non-blocking Tokio RaftActor
    let raft_handle = RaftNode::spawn(id, peers, network, storage, Some(apply_tx));

    // Spawn State Machine Application Task
    let store_applier = store.clone();
    tokio::spawn(async move {
        while let Some((key, value)) = apply_rx.recv().await {
            let _ = store_applier.set(key, value);
        }
    });

    // Start HTTP API server
    HttpServer::new(move || {
        let store = store.clone();
        let raft = raft_handle.clone();
        actix_web::App::new().configure(move |cfg| configure_app(cfg, store, raft))
    })
    .bind(format!("127.0.0.1:{}", port))?
    .run()
    .await
}
