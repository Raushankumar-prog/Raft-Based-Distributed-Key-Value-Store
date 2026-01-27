use actix_web::{web, HttpResponse, Responder};
use raft_core::{AppendEntriesArgs, RaftApi, RequestVoteArgs};
use std::sync::{Arc, Mutex};

pub async fn handle_vote(
    raft: web::Data<Arc<Mutex<dyn RaftApi>>>,
    req: web::Json<RequestVoteArgs>,
) -> impl Responder {
    let mut node = raft.lock().unwrap();
    let reply = node.on_request_vote(req.into_inner());
    HttpResponse::Ok().json(reply)
}

pub async fn handle_append(
    raft: web::Data<Arc<Mutex<dyn RaftApi>>>,
    req: web::Json<AppendEntriesArgs>,
) -> impl Responder {
    let mut node = raft.lock().unwrap();
    let reply = node.on_append_entries(req.into_inner());
    HttpResponse::Ok().json(reply)
}
