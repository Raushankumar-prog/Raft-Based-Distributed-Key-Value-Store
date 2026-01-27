use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::sync::{Arc, Mutex};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum KvError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Lock poisoned")]
    LockPoisoned,
}

pub type Result<T> = std::result::Result<T, KvError>;

#[derive(Serialize, Deserialize, Debug)]
struct LogEntry {
    key: String,
    value: String,
}

pub struct KvStore {
    pub map: Arc<Mutex<HashMap<String, String>>>,
    log_path: String,
    #[allow(dead_code)]
    snapshot_path: String,
}

impl KvStore {
    pub fn new(log_path: &str, snapshot_path: &str) -> Result<Self> {
        let map = Arc::new(Mutex::new(HashMap::new()));
        let mut store = KvStore {
            map: map.clone(),
            log_path: log_path.to_string(),
            snapshot_path: snapshot_path.to_string(),
        };

        store.load_from_log()?;

        Ok(store)
    }

    pub fn set(&self, key: String, value: String) -> Result<()> {
        self.append_log(&key, &value)?;

        let mut map = self.map.lock().map_err(|_| KvError::LockPoisoned)?;
        map.insert(key, value);

        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<String>> {
        let map = self.map.lock().map_err(|_| KvError::LockPoisoned)?;
        Ok(map.get(key).cloned())
    }

    fn append_log(&self, key: &str, value: &str) -> Result<()> {
        let entry = LogEntry {
            key: key.to_string(),
            value: value.to_string(),
        };

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)?;

        serde_json::to_writer(&file, &entry)?;
        writeln!(file)?;

        Ok(())
    }

    fn load_from_log(&mut self) -> Result<()> {
        let file = match File::open(&self.log_path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(KvError::Io(e)),
        };

        let reader = BufReader::new(file);
        let mut map = self.map.lock().map_err(|_| KvError::LockPoisoned)?;

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }

            match serde_json::from_str::<LogEntry>(&line) {
                Ok(entry) => {
                    map.insert(entry.key, entry.value);
                }
                Err(e) => {
                    eprintln!("Skipping corrupted log line: {} | Error: {}", line, e);
                }
            }
        }

        Ok(())
    }

    pub fn create_snapshot(&self) -> Result<()> {
        let map = self.map.lock().map_err(|_| KvError::LockPoisoned)?;
        let mut file = File::create(&self.snapshot_path)?;

        for (k, v) in map.iter() {
            let entry = LogEntry {
                key: k.clone(),
                value: v.clone(),
            };
            serde_json::to_writer(&file, &entry)?;
            writeln!(file)?;
        }

        File::create(&self.log_path)?;
        Ok(())
    }
}
