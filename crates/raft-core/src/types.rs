use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ClientCommand {
    Set { key: String, value: String },
    NoOp,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogEntry {
    pub term: u64,
    pub command: ClientCommand,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}
