use crate::store::KvStore;
use actix_web::{App, HttpResponse, HttpServer, Responder, web};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use raft::RaftNode;

#[derive(Deserialize)]
pub struct SetRequest {
    pub key: String,
    pub value: String,
}

#[derive(Serialize)]
pub struct GetResponse {
    pub value: Option<String>,
}

pub fn app_factory(store: Arc<KvStore>) -> App<impl actix_web::dev::ServiceFactory> {
    App::new()
        .app_data(web::Data::new(store))
        .route("/set", web::post().to(set_kv))
        .route("/get/{key}", web::get().to(get_kv))
}

async fn set_kv(store: web::Data<Arc<KvStore>>, req: web::Json<SetRequest>) -> impl Responder {
    store.set(req.key.clone(), req.value.clone());
    HttpResponse::Ok().body("OK")
}

async fn get_kv(store: web::Data<Arc<KvStore>>, key: web::Path<String>) -> impl Responder {
    let value = store.get(&key.into_inner());
    HttpResponse::Ok().json(GetResponse { value })
}

// Initialize Raft node (single node for demo, use vec![2,3] for cluster)
let mut raft = RaftNode::new(1, vec![]);

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
});
