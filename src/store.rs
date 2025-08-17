use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::sync::{Arc, Mutex};

pub struct KvStore {
    pub map: Arc<Mutex<HashMap<String, String>>>,
    log_path: String,
}

impl KvStore {
    pub fn new(log_path: &str) -> Self {
        let map = Arc::new(Mutex::new(HashMap::new()));
        let mut store = KvStore {
            map: map.clone(),
            log_path: log_path.to_string(),
        };
        store.load_from_log();
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
}
