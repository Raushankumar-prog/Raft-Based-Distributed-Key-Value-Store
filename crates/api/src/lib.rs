mod dto;
mod handlers;

use actix_web::web;
use kv_store::KvStore;
use raft_core::RaftHandle;
use std::sync::Arc;

pub fn configure_app(
    cfg: &mut web::ServiceConfig,
    store: Arc<KvStore>,
    raft: RaftHandle,
) {
    cfg.app_data(web::Data::new(store))
        .app_data(web::Data::new(raft))
        .route("/set", web::post().to(handlers::kv::set_kv))
        .route("/get/{key}", web::get().to(handlers::kv::get_kv))
        .route("/raft/vote", web::post().to(handlers::raft::handle_vote))
        .route(
            "/raft/append",
            web::post().to(handlers::raft::handle_append),
        );
}
