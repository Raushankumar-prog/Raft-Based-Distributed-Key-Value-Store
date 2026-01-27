use reqwest::blocking::Client;
use serde_json::json;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: raft-client <get/set> <key> [value]");
        eprintln!("Example: raft-client set mykey myvalue");
        eprintln!("Example: raft-client get mykey");
        return;
    }

    let client = Client::new();
    let cmd = &args[1];
    let key = &args[2];

    match cmd.as_str() {
        "get" => {
            let url = format!("http://127.0.0.1:8080/get/{}", key);
            match client.get(&url).send() {
                Ok(resp) => {
                    if resp.status().is_success() {
                        println!("{}", resp.text().unwrap_or_default());
                    } else {
                        eprintln!("Error: {}", resp.status());
                    }
                }
                Err(e) => eprintln!("Failed to connect: {}", e),
            }
        }
        "set" => {
            if args.len() < 4 {
                eprintln!("Usage: raft-client set <key> <value>");
                return;
            }
            let value = &args[3];
            let url = "http://127.0.0.1:8080/set";
            let body = json!({"key": key, "value": value});
            match client.post(url).json(&body).send() {
                Ok(resp) => {
                    println!("{}", resp.text().unwrap_or_default());
                }
                Err(e) => eprintln!("Failed to connect: {}", e),
            }
        }
        _ => eprintln!("Unknown command: {}", cmd),
    }
}
