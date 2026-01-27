use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct SetRequest {
    pub key: String,
    pub value: String,
}

#[derive(Serialize)]
pub struct GetResponse {
    pub value: Option<String>,
}
