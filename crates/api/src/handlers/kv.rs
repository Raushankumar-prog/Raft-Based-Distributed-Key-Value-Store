use crate::dto::{GetResponse, SetRequest};
use actix_web::{error, web, HttpResponse, Responder};
use kv_store::KvStore;
use std::sync::Arc;

pub async fn set_kv(
    store: web::Data<Arc<KvStore>>,
    req: web::Json<SetRequest>,
) -> actix_web::Result<impl Responder> {
    let key = req.key.clone();
    let value = req.value.clone();
    let store = store.clone();

    web::block(move || store.set(key, value))
        .await?
        .map_err(|e| error::ErrorInternalServerError(format!("Store error: {}", e)))?;

    Ok(HttpResponse::Ok().body("OK"))
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
