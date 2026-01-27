use crate::storage::RaftStorage;
use crate::types::LogEntry;
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MemError {
    #[error("Storage poison error")]
    Poison,
}

#[derive(Clone)]
pub struct MemStorage {
    term: Arc<Mutex<u64>>,
    voted_for: Arc<Mutex<Option<u64>>>,
    log: Arc<Mutex<Vec<LogEntry>>>,
}

impl MemStorage {
    pub fn new() -> Self {
        let dummy = LogEntry {
            term: 0,
            command: crate::types::ClientCommand::NoOp,
        };
        MemStorage {
            term: Arc::new(Mutex::new(0)),
            voted_for: Arc::new(Mutex::new(None)),
            log: Arc::new(Mutex::new(vec![dummy])),
        }
    }
}

#[async_trait]
impl RaftStorage for MemStorage {
    type Error = MemError;

    async fn save_term_and_vote(
        &self,
        term: u64,
        voted_for: Option<u64>,
    ) -> Result<(), Self::Error> {
        let mut t = self.term.lock().map_err(|_| MemError::Poison)?;
        let mut v = self.voted_for.lock().map_err(|_| MemError::Poison)?;
        *t = term;
        *v = voted_for;
        Ok(())
    }

    async fn load_term_and_vote(&self) -> Result<(u64, Option<u64>), Self::Error> {
        let t = self.term.lock().map_err(|_| MemError::Poison)?;
        let v = self.voted_for.lock().map_err(|_| MemError::Poison)?;
        Ok((*t, *v))
    }

    async fn append_entry(&self, entry: &LogEntry) -> Result<(), Self::Error> {
        let mut l = self.log.lock().map_err(|_| MemError::Poison)?;
        l.push(entry.clone());
        Ok(())
    }

    async fn get_log_entries(&self, start_index: usize) -> Result<Vec<LogEntry>, Self::Error> {
        let l = self.log.lock().map_err(|_| MemError::Poison)?;
        if start_index >= l.len() {
            return Ok(vec![]);
        }
        Ok(l[start_index..].to_vec())
    }

    async fn log_len(&self) -> Result<usize, Self::Error> {
        let l = self.log.lock().map_err(|_| MemError::Poison)?;
        Ok(l.len())
    }
}
