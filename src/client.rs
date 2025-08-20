use reqwest::blocking::Client;
use serde_json::json;
use std::env;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Write};

struct KeyValueStore {
    map: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, String>>>,
    log_path: String,
    snapshot_path: String,
}

impl KeyValueStore {
    pub fn new(log_path: &str, snapshot_path: &str) -> Self {
        let mut store = KeyValueStore {
            map: std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            log_path: log_path.to_string(),
            snapshot_path: snapshot_path.to_string(),
        };
        store.load_from_log();
        store.load_from_snapshot();
        store
    }

    pub fn set(&self, key: String, value: String) {
        let mut map = self.map.lock().unwrap();
        map.insert(key.clone(), value.clone());
        self.append_log(&key, &value);
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let map = self.map.lock().unwrap();
        map.get(key).cloned()
    }

    fn append_log(&self, key: &str, value: &str) {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)
            .unwrap();
        writeln!(file, "{}:{}", key, value).unwrap();
    }

    fn load_from_log(&mut self) {
        if let Ok(file) = File::open(&self.log_path) {
            let reader = BufReader::new(file);
            let mut map = self.map.lock().unwrap();
            for line in reader.lines() {
                if let Ok(entry) = line {
                    if let Some((k, v)) = entry.split_once(":") {
                        map.insert(k.to_string(), v.to_string());
                    }
                }
            }
        }
    }

    // --- Snapshotting ---
    pub fn create_snapshot(&self) {
        let map = self.map.lock().unwrap();
        let mut file = File::create(&self.snapshot_path).unwrap();
        for (k, v) in map.iter() {
            writeln!(file, "{}:{}", k, v).unwrap();
        }
        // After snapshot, truncate the log
        File::create(&self.log_path).unwrap();
    }

    fn load_from_snapshot(&mut self) {
        if let Ok(file) = File::open(&self.snapshot_path) {
            let reader = BufReader::new(file);
            let mut map = self.map.lock().unwrap();
            for line in reader.lines() {
                if let Ok(entry) = line {
                    if let Some((k, v)) = entry.split_once(":") {
                        map.insert(k.to_string(), v.to_string());
                    }
                }
            }
        }
    }
}

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
