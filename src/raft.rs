use std::collections::HashMap;
use std::time::{Duration, Instant};
use rand::Rng;

#[derive(Debug, Clone, PartialEq)]
pub enum RaftRole {
    Leader,
    Follower,
    Candidate,
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub term: u64,
    pub command: String, // For simplicity, store as string
}

pub struct RaftNode {
    pub id: u64,
    pub role: RaftRole,
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub log: Vec<LogEntry>,
    pub commit_index: usize,
    pub last_applied: usize,
    pub peers: Vec<u64>,
    pub election_deadline: Instant,
}

impl RaftNode {
    pub fn new(id: u64, peers: Vec<u64>) -> Self {
        let mut rng = rand::thread_rng();
        let timeout = rng.gen_range(150..300);
        RaftNode {
            id,
            role: RaftRole::Follower,
            current_term: 0,
            voted_for: None,
            log: vec![],
            commit_index: 0,
            last_applied: 0,
            peers,
            election_deadline: Instant::now() + Duration::from_millis(timeout),
        }
    }
    // TODO: Add methods for election, heartbeat, log replication, etc.
}
