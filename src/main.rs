
mod api;
mod store;
mod raft;

use crate::api::app_factory;
use crate::store::KvStore;
use actix_web::HttpServer;
use std::sync::Arc;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let store = Arc::new(KvStore::new("kv.log"));
    HttpServer::new(move || app_factory(store.clone()))
        .bind(("127.0.0.1", 8080))?
        .run()
        .await
}
