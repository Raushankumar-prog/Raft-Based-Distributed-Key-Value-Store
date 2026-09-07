use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{interval, Instant};

use crate::api::RaftHandle;
use crate::network::RaftNetwork;
use crate::rpc::{AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply};
use crate::storage::RaftStorage;
use crate::types::{ClientCommand, LogEntry, RaftMessage, RaftRole};

pub struct RaftNode<N: RaftNetwork, S: RaftStorage> {
    pub id: u64,
    pub peers: Vec<u64>,
    network: Arc<N>,
    storage: Arc<S>,

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

    pending_proposals: HashMap<usize, oneshot::Sender<Result<(), String>>>,
    apply_tx: Option<mpsc::Sender<(String, String)>>,
}

impl<N: RaftNetwork, S: RaftStorage> RaftNode<N, S> {
    pub fn spawn(
        id: u64,
        peers: Vec<u64>,
        network: N,
        storage: S,
        apply_tx: Option<mpsc::Sender<(String, String)>>,
    ) -> RaftHandle {
        let (tx, rx) = mpsc::channel(100);
        let dummy = LogEntry {
            term: 0,
            command: ClientCommand::NoOp,
        };

        let node = RaftNode {
            id,
            peers,
            network: Arc::new(network),
            storage: Arc::new(storage),
            current_term: 0,
            voted_for: None,
            log: vec![dummy],
            commit_index: 0,
            last_applied: 0,
            role: RaftRole::Follower,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            election_deadline: Self::random_election_deadline(),
            last_heartbeat: Instant::now(),
            pending_proposals: HashMap::new(),
            apply_tx,
        };

        tokio::spawn(node.run(rx));

        RaftHandle::new(tx)
    }

    fn random_election_deadline() -> Instant {
        let millis = rand::thread_rng().gen_range(300..600);
        Instant::now() + Duration::from_millis(millis)
    }

    async fn run(mut self, mut rx: mpsc::Receiver<RaftMessage>) {
        let mut ticker = interval(Duration::from_millis(50));

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    self.on_tick().await;
                }
                msg = rx.recv() => {
                    match msg {
                        Some(RaftMessage::RequestVote { args, tx }) => {
                            let reply = self.handle_request_vote(args).await;
                            let _ = tx.send(reply);
                        }
                        Some(RaftMessage::AppendEntries { args, tx }) => {
                            let reply = self.handle_append_entries(args).await;
                            let _ = tx.send(reply);
                        }
                        Some(RaftMessage::Propose { command, tx }) => {
                            let _ = self.handle_propose(command, tx).await;
                        }
                        Some(RaftMessage::GetState { tx }) => {
                            let _ = tx.send((self.current_term, self.role.clone()));
                        }
                        None => break,
                    }
                }
            }
        }
    }

    async fn on_tick(&mut self) {
        match self.role {
            RaftRole::Follower | RaftRole::Candidate => {
                if Instant::now() > self.election_deadline {
                    self.start_election().await;
                }
            }
            RaftRole::Leader => {
                let heartbeat_interval = Duration::from_millis(100);
                if Instant::now().duration_since(self.last_heartbeat) >= heartbeat_interval {
                    self.replicate_and_heartbeat().await;
                }
            }
        }
        self.apply_committed().await;
    }

    pub async fn start_election(&mut self) {
        self.role = RaftRole::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.election_deadline = Self::random_election_deadline();
        let _ = self.storage.save_term_and_vote(self.current_term, self.voted_for).await;

        let args = RequestVoteArgs {
            term: self.current_term,
            candidate_id: self.id,
            last_log_index: self.log.len() - 1,
            last_log_term: self.log.last().unwrap().term,
        };

        let total_nodes = self.peers.len() + 1;
        if 1 > total_nodes / 2 {
            self.become_leader();
            return;
        }

        // Concurrent parallel RequestVote broadcasts
        let (tx, mut rx) = mpsc::channel(self.peers.len() + 1);
        for peer in self.peers.clone() {
            let network = self.network.clone();
            let args_clone = args.clone();
            let tx_clone = tx.clone();
            tokio::spawn(async move {
                if let Some(reply) = network.send_request_vote(peer, args_clone).await {
                    let _ = tx_clone.send((peer, reply)).await;
                }
            });
        }
        drop(tx);

        let mut votes = 1;
        while let Some((_peer, reply)) = rx.recv().await {
            if reply.term > self.current_term {
                self.become_follower(reply.term).await;
                return;
            }
            if self.role == RaftRole::Candidate
                && reply.term == self.current_term
                && reply.vote_granted
            {
                votes += 1;
                if votes > total_nodes / 2 {
                    self.become_leader();
                    return;
                }
            }
        }
    }

    pub async fn handle_request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        if args.term > self.current_term {
            self.become_follower(args.term).await;
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
                    let _ = self.storage.save_term_and_vote(self.current_term, self.voted_for).await;
                }
            }
        }

        RequestVoteReply {
            term: self.current_term,
            vote_granted: granted,
        }
    }

    pub async fn handle_append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        if args.term > self.current_term {
            self.become_follower(args.term).await;
        }

        if args.term < self.current_term {
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
                conflict_index: self.log.len(),
                conflict_term: 0,
            };
        }

        self.election_deadline = Self::random_election_deadline();
        self.role = RaftRole::Follower;

        if args.prev_log_index >= self.log.len() {
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
                conflict_index: self.log.len(),
                conflict_term: 0,
            };
        }

        if self.log[args.prev_log_index].term != args.prev_log_term {
            let conflict_term = self.log[args.prev_log_index].term;
            let mut conflict_index = args.prev_log_index;
            while conflict_index > 0 && self.log[conflict_index - 1].term == conflict_term {
                conflict_index -= 1;
            }
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
                conflict_index,
                conflict_term,
            };
        }

        let mut index = args.prev_log_index + 1;
        for entry in args.entries {
            if index < self.log.len() {
                if self.log[index].term != entry.term {
                    self.log.truncate(index);
                    self.log.push(entry.clone());
                    let _ = self.storage.append_entry(&entry).await;
                }
            } else {
                self.log.push(entry.clone());
                let _ = self.storage.append_entry(&entry).await;
            }
            index += 1;
        }

        if args.leader_commit > self.commit_index {
            self.commit_index = std::cmp::min(args.leader_commit, self.log.len() - 1);
            self.apply_committed().await;
        }

        AppendEntriesReply {
            term: self.current_term,
            success: true,
            conflict_index: 0,
            conflict_term: 0,
        }
    }

    async fn become_follower(&mut self, term: u64) {
        self.current_term = term;
        self.role = RaftRole::Follower;
        self.voted_for = None;
        self.election_deadline = Self::random_election_deadline();
        let _ = self.storage.save_term_and_vote(self.current_term, self.voted_for).await;
    }

    fn become_leader(&mut self) {
        self.role = RaftRole::Leader;
        let last_log_idx = self.log.len();
        for peer in &self.peers {
            self.next_index.insert(*peer, last_log_idx);
            self.match_index.insert(*peer, 0);
        }
    }

    pub async fn replicate_and_heartbeat(&mut self) {
        if self.role != RaftRole::Leader {
            return;
        }
        self.last_heartbeat = Instant::now();

        // Concurrent parallel AppendEntries broadcasts
        let (tx, mut rx) = mpsc::channel(self.peers.len() + 1);

        for peer in self.peers.clone() {
            let next = self.next_index.get(&peer).copied().unwrap_or(self.log.len());
            let prev_index = if next > 0 { next - 1 } else { 0 };
            let prev_term = self.log[prev_index].term;
            let entries = if next < self.log.len() {
                self.log[next..].to_vec()
            } else {
                vec![]
            };

            let args = AppendEntriesArgs {
                term: self.current_term,
                leader_id: self.id,
                prev_log_index: prev_index,
                prev_log_term: prev_term,
                entries: entries.clone(),
                leader_commit: self.commit_index,
            };

            let network = self.network.clone();
            let tx_clone = tx.clone();
            let entries_len = entries.len();

            tokio::spawn(async move {
                if let Some(reply) = network.send_append_entries(peer, args).await {
                    let _ = tx_clone.send((peer, prev_index, entries_len, reply)).await;
                }
            });
        }
        drop(tx);

        while let Some((peer, prev_index, entries_len, reply)) = rx.recv().await {
            if reply.term > self.current_term {
                self.become_follower(reply.term).await;
                return;
            }
            if self.role == RaftRole::Leader && reply.term == self.current_term {
                if reply.success {
                    let new_match = prev_index + entries_len;
                    self.match_index.insert(peer, new_match);
                    self.next_index.insert(peer, new_match + 1);
                } else if reply.conflict_index > 0 {
                    self.next_index.insert(peer, reply.conflict_index);
                } else {
                    let current_next = self.next_index.get(&peer).copied().unwrap_or(1);
                    if current_next > 1 {
                        self.next_index.insert(peer, current_next - 1);
                    }
                }
            }
        }

        self.update_commit_index().await;
    }

    async fn update_commit_index(&mut self) {
        if self.role != RaftRole::Leader {
            return;
        }

        let total_nodes = self.peers.len() + 1;
        let mut new_commit = self.commit_index;
        for n in (self.commit_index + 1)..self.log.len() {
            if self.log[n].term == self.current_term {
                let mut match_count = 1; // Self
                for peer in &self.peers {
                    if self.match_index.get(peer).copied().unwrap_or(0) >= n {
                        match_count += 1;
                    }
                }
                if match_count > total_nodes / 2 {
                    new_commit = n;
                }
            }
        }

        if new_commit > self.commit_index {
            self.commit_index = new_commit;
            self.notify_pending_proposals(new_commit);
            self.apply_committed().await;
        }
    }

    fn notify_pending_proposals(&mut self, commit_idx: usize) {
        let keys_to_notify: Vec<usize> = self
            .pending_proposals
            .keys()
            .copied()
            .filter(|&idx| idx <= commit_idx)
            .collect();

        for idx in keys_to_notify {
            if let Some(tx) = self.pending_proposals.remove(&idx) {
                let _ = tx.send(Ok(()));
            }
        }
    }

    pub async fn handle_propose(
        &mut self,
        command: ClientCommand,
        tx: oneshot::Sender<Result<(), String>>,
    ) -> Result<(), String> {
        if self.role != RaftRole::Leader {
            let err = "Not leader".to_string();
            let _ = tx.send(Err(err.clone()));
            return Err(err);
        }

        let entry = LogEntry {
            term: self.current_term,
            command,
        };
        self.log.push(entry.clone());
        let _ = self.storage.append_entry(&entry).await;
        let index = self.log.len() - 1;

        if self.peers.is_empty() {
            self.commit_index = index;
            let _ = tx.send(Ok(()));
            self.apply_committed().await;
        } else {
            self.pending_proposals.insert(index, tx);
            self.replicate_and_heartbeat().await;
        }

        Ok(())
    }

    async fn apply_committed(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let entry = &self.log[self.last_applied];
            if let ClientCommand::Set { key, value } = &entry.command {
                if let Some(tx) = &self.apply_tx {
                    let _ = tx.send((key.clone(), value.clone())).await;
                }
            }
        }
    }
}
