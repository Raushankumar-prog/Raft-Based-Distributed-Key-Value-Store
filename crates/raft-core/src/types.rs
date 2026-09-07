use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use crate::rpc::{AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply};

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

pub enum RaftMessage {
    RequestVote {
        args: RequestVoteArgs,
        tx: oneshot::Sender<RequestVoteReply>,
    },
    AppendEntries {
        args: AppendEntriesArgs,
        tx: oneshot::Sender<AppendEntriesReply>,
    },
    Propose {
        command: ClientCommand,
        tx: oneshot::Sender<Result<(), String>>,
    },
    GetState {
        tx: oneshot::Sender<(u64, RaftRole)>,
    },
}
