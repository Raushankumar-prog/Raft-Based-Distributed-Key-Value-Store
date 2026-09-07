use actix_web::{web, HttpResponse, Responder};
use raft_core::{AppendEntriesArgs, RaftHandle, RequestVoteArgs};

pub async fn handle_vote(
    raft: web::Data<RaftHandle>,
    req: web::Json<RequestVoteArgs>,
) -> impl Responder {
    match raft.request_vote(req.into_inner()).await {
        Ok(reply) => HttpResponse::Ok().json(reply),
        Err(err) => HttpResponse::InternalServerError().body(err),
    }
}

pub async fn handle_append(
    raft: web::Data<RaftHandle>,
    req: web::Json<AppendEntriesArgs>,
) -> impl Responder {
    match raft.append_entries(req.into_inner()).await {
        Ok(reply) => HttpResponse::Ok().json(reply),
        Err(err) => HttpResponse::InternalServerError().body(err),
    }
}
