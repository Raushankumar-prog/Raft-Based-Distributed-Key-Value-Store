use std::env;
use reqwest::blocking::Client;
use serde_json::json;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: client <get/set> <key> [value]");
        return;
    }
    let client = Client::new();
    let cmd = &args[1];
    let key = &args[2];
    match cmd.as_str() {
        "get" => {
            let url = format!("http://127.0.0.1:8080/get/{}", key);
            let resp = client.get(&url).send().unwrap();
            println!("{}", resp.text().unwrap());
        }
        "set" => {
            if args.len() < 4 {
                eprintln!("Usage: client set <key> <value>");
                return;
            }
            let value = &args[3];
            let url = "http://127.0.0.1:8080/set";
            let body = json!({"key": key, "value": value});
            let resp = client.post(url).json(&body).send().unwrap();
            println!("{}", resp.text().unwrap());
        }
        _ => eprintln!("Unknown command: {}", cmd),
    }
}
