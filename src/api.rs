use crate::store::KvStore;
use actix_web::{App, HttpResponse, HttpServer, Responder, web};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

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
