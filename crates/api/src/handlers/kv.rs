use crate::dto::{GetResponse, SetRequest};
use actix_web::{error, web, HttpResponse, Responder};
use kv_store::KvStore;
use raft_core::{ClientCommand, RaftHandle};
use std::sync::Arc;

pub async fn set_kv(
    raft: web::Data<RaftHandle>,
    req: web::Json<SetRequest>,
) -> actix_web::Result<impl Responder> {
    let command = ClientCommand::Set {
        key: req.key.clone(),
        value: req.value.clone(),
    };

    match raft.propose(command).await {
        Ok(_) => Ok(HttpResponse::Ok().body("OK")),
        Err(err_msg) => Ok(HttpResponse::ServiceUnavailable().body(format!("Raft error: {}", err_msg))),
    }
}

pub async fn get_kv(
    store: web::Data<Arc<KvStore>>,
    key: web::Path<String>,
) -> actix_web::Result<impl Responder> {
    let key = key.into_inner();
    let store = store.clone();

    let value = web::block(move || store.get(&key))
        .await?
        .map_err(|e| error::ErrorInternalServerError(format!("Store error: {}", e)))?;

    Ok(HttpResponse::Ok().json(GetResponse { value }))
}
