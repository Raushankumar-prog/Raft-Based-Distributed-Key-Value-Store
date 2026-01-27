use crate::types::LogEntry;
use async_trait::async_trait;
use std::error::Error;

#[async_trait]
pub trait RaftStorage: Send + Sync + 'static {
    type Error: Error + Send + Sync + 'static;

    async fn save_term_and_vote(
        &self,
        term: u64,
        voted_for: Option<u64>,
    ) -> Result<(), Self::Error>;
    async fn load_term_and_vote(&self) -> Result<(u64, Option<u64>), Self::Error>;

    async fn append_entry(&self, entry: &LogEntry) -> Result<(), Self::Error>;
    async fn get_log_entries(&self, start_index: usize) -> Result<Vec<LogEntry>, Self::Error>;

    async fn log_len(&self) -> Result<usize, Self::Error>;
}
