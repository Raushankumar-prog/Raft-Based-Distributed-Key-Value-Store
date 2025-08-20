use rand::Rng;
use std::collections::HashMap;
use std::time::{Duration, Instant};

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
    last_heartbeat: Instant,                // Track last heartbeat time
    state_machine: HashMap<String, String>, // Simple key-value store for applied commands
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
            last_heartbeat: Instant::now(),
            state_machine: HashMap::new(),
        }
    }
    // TODO: Add methods for election, heartbeat, log replication, etc.

    pub fn tick(&mut self) {
        if self.role == RaftRole::Leader {
            self.send_heartbeats();
        } else if Instant::now() > self.election_deadline {
            self.start_election();
        }
    }

    fn reset_election_timeout(&mut self) {
        let mut rng = rand::thread_rng();
        let timeout = rng.gen_range(150..300);
        self.election_deadline = Instant::now() + Duration::from_millis(timeout);
    }

    pub fn start_election(&mut self) {
        self.role = RaftRole::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        let mut votes = 1; // vote for self

        // Simulate votes from peers (randomized for demo)
        for _ in &self.peers {
            if rand::random::<bool>() {
                votes += 1;
            }
        }

        if votes > (self.peers.len() + 1) / 2 {
            self.role = RaftRole::Leader;
            self.send_heartbeats();
        } else {
            self.role = RaftRole::Follower;
        }
        self.reset_election_timeout();
    }

    pub fn send_heartbeats(&mut self) {
        self.last_heartbeat = Instant::now();
        self.reset_election_timeout();
        // In a real implementation, send AppendEntries RPCs to peers
    }

    pub fn propose(&mut self, command: String) {
        if self.role == RaftRole::Leader {
            self.log.push(LogEntry {
                term: self.current_term,
                command: command.clone(),
            });
            self.commit_index = self.log.len();
            self.apply_log();
        }
    }

    fn apply_log(&mut self) {
        while self.last_applied < self.commit_index {
            let entry = &self.log[self.last_applied];
            // For demo: parse "set key value"
            let parts: Vec<&str> = entry.command.split_whitespace().collect();
            if parts.len() == 3 && parts[0] == "set" {
                self.state_machine
                    .insert(parts[1].to_string(), parts[2].to_string());
            }
            self.last_applied += 1;
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.state_machine.get(key).cloned()
    }
}
