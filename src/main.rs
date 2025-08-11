use actix_web::{App, HttpResponse, HttpServer, Responder, web};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct AppState {
    store: Arc<Mutex<HashMap<String, String>>>,
}

#[derive(Deserialize)]
struct SetRequest {
    key: String,
    value: String,
}

#[derive(Serialize)]
struct GetResponse {
    value: Option<String>,
}

async fn set_kv(data: web::Data<AppState>, req: web::Json<SetRequest>) -> impl Responder {
    let mut store = data.store.lock().unwrap();
    store.insert(req.key.clone(), req.value.clone());
    HttpResponse::Ok().body("OK")
}

async fn get_kv(data: web::Data<AppState>, key: web::Path<String>) -> impl Responder {
    let store = data.store.lock().unwrap();
    let value = store.get(&key.into_inner()).cloned();
    HttpResponse::Ok().json(GetResponse { value })
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let state = AppState {
        store: Arc::new(Mutex::new(HashMap::new())),
    };

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(state.clone()))
            .route("/set", web::post().to(set_kv))
            .route("/get/{key}", web::get().to(get_kv))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
