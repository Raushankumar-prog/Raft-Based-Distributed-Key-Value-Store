use rand::Rng;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::network::RaftNetwork;
use crate::rpc::{AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply};
use crate::types::{LogEntry, RaftRole};

pub struct RaftNode<N: RaftNetwork> {
    pub id: u64,
    pub peers: Vec<u64>,
    network: N,

    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub log: Vec<LogEntry>,

    pub commit_index: usize,
    pub last_applied: usize,
    pub role: RaftRole,

    pub next_index: HashMap<u64, usize>,
    pub match_index: HashMap<u64, usize>,

    election_deadline: Instant,
    last_heartbeat: Instant,
}

impl<N: RaftNetwork> RaftNode<N> {
    pub fn new(id: u64, peers: Vec<u64>, network: N) -> Self {
        let dummy_entry = LogEntry {
            term: 0,
            command: crate::types::ClientCommand::NoOp,
        };

        RaftNode {
            id,
            peers,
            network,
            current_term: 0,
            voted_for: None,
            log: vec![dummy_entry],
            commit_index: 0,
            last_applied: 0,
            role: RaftRole::Follower,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            election_deadline: Self::random_election_deadline(),
            last_heartbeat: Instant::now(),
        }
    }

    fn random_election_deadline() -> Instant {
        let millis = rand::thread_rng().gen_range(300..600);
        Instant::now() + Duration::from_millis(millis)
    }

    pub async fn tick(&mut self) {
        match self.role {
            RaftRole::Follower | RaftRole::Candidate => {
                if Instant::now() > self.election_deadline {
                    self.start_election().await;
                }
            }
            RaftRole::Leader => {
                let heartbeat_interval = Duration::from_millis(100);
                if Instant::now().duration_since(self.last_heartbeat) >= heartbeat_interval {
                    self.send_heartbeats().await;
                }
            }
        }
    }

    pub async fn start_election(&mut self) {
        self.role = RaftRole::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.election_deadline = Self::random_election_deadline();

        let args = RequestVoteArgs {
            term: self.current_term,
            candidate_id: self.id,
            last_log_index: self.log.len() - 1,
            last_log_term: self.log.last().unwrap().term,
        };

        let mut votes = 1;

        for peer in &self.peers {
            if let Some(reply) = self.network.send_request_vote(*peer, args.clone()).await {
                if reply.term > self.current_term {
                    self.become_follower(reply.term);
                    return;
                }
                if reply.term == self.current_term && reply.vote_granted {
                    votes += 1;
                }
            }
        }

        if votes > (self.peers.len() + 1) / 2 {
            self.become_leader();
        }
    }

    pub fn handle_request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        if args.term > self.current_term {
            self.become_follower(args.term);
        }

        let mut granted = false;
        if args.term == self.current_term {
            if self.voted_for.is_none() || self.voted_for == Some(args.candidate_id) {
                let last_index = self.log.len() - 1;
                let last_term = self.log.last().unwrap().term;

                if args.last_log_term > last_term
                    || (args.last_log_term == last_term && args.last_log_index >= last_index)
                {
                    granted = true;
                    self.voted_for = Some(args.candidate_id);
                    self.election_deadline = Self::random_election_deadline();
                }
            }
        }

        RequestVoteReply {
            term: self.current_term,
            vote_granted: granted,
        }
    }

    pub fn handle_append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        if args.term > self.current_term {
            self.become_follower(args.term);
        }

        let success = if args.term < self.current_term {
            false
        } else {
            self.election_deadline = Self::random_election_deadline();
            self.role = RaftRole::Follower;

            if args.prev_log_index >= self.log.len()
                || self.log[args.prev_log_index].term != args.prev_log_term
            {
                false
            } else {
                let mut index = args.prev_log_index + 1;
                for entry in args.entries {
                    if index < self.log.len() {
                        if self.log[index].term != entry.term {
                            self.log.truncate(index);
                            self.log.push(entry);
                        }
                    } else {
                        self.log.push(entry);
                    }
                    index += 1;
                }
                if args.leader_commit > self.commit_index {
                    self.commit_index = std::cmp::min(args.leader_commit, self.log.len() - 1);
                }
                true
            }
        };

        AppendEntriesReply {
            term: self.current_term,
            success,
            conflict_index: 0,
            conflict_term: 0,
        }
    }

    fn become_follower(&mut self, term: u64) {
        self.current_term = term;
        self.role = RaftRole::Follower;
        self.voted_for = None;
        self.election_deadline = Self::random_election_deadline();
    }

    fn become_leader(&mut self) {
        self.role = RaftRole::Leader;
        for peer in &self.peers {
            self.next_index.insert(*peer, self.log.len());
            self.match_index.insert(*peer, 0);
        }
    }

    async fn send_heartbeats(&mut self) {
        self.last_heartbeat = Instant::now();
        for peer in self.peers.clone() {
            let prev_index = self.log.len() - 1;
            let prev_term = self.log[prev_index].term;

            let args = AppendEntriesArgs {
                term: self.current_term,
                leader_id: self.id,
                prev_log_index: prev_index,
                prev_log_term: prev_term,
                entries: vec![],
                leader_commit: self.commit_index,
            };

            self.network.send_append_entries(peer, args).await;
        }
    }

    pub fn propose(&mut self, command: crate::types::ClientCommand) {
        if self.role == RaftRole::Leader {
            self.log.push(LogEntry {
                term: self.current_term,
                command,
            });
        }
    }
}

impl<N: RaftNetwork> crate::api::RaftApi for RaftNode<N> {
    fn on_request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        self.handle_request_vote(args)
    }
    fn on_append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        self.handle_append_entries(args)
    }
}
