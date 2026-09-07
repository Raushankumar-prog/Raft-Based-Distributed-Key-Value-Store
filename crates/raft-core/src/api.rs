use crate::rpc::{AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply};
use crate::types::{ClientCommand, RaftMessage, RaftRole};
use tokio::sync::{mpsc, oneshot};

#[derive(Clone)]
pub struct RaftHandle {
    sender: mpsc::Sender<RaftMessage>,
}

impl RaftHandle {
    pub fn new(sender: mpsc::Sender<RaftMessage>) -> Self {
        RaftHandle { sender }
    }

    pub async fn request_vote(&self, args: RequestVoteArgs) -> Result<RequestVoteReply, String> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(RaftMessage::RequestVote { args, tx })
            .await
            .map_err(|e| e.to_string())?;
        rx.await.map_err(|e| e.to_string())
    }

    pub async fn append_entries(&self, args: AppendEntriesArgs) -> Result<AppendEntriesReply, String> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(RaftMessage::AppendEntries { args, tx })
            .await
            .map_err(|e| e.to_string())?;
        rx.await.map_err(|e| e.to_string())
    }

    pub async fn propose(&self, command: ClientCommand) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(RaftMessage::Propose { command, tx })
            .await
            .map_err(|e| format!("Actor channel send error: {}", e))?;
        rx.await.map_err(|e| format!("Channel receiver error: {}", e))?
    }

    pub async fn get_state(&self) -> Result<(u64, RaftRole), String> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(RaftMessage::GetState { tx })
            .await
            .map_err(|e| e.to_string())?;
        rx.await.map_err(|e| e.to_string())
    }
}
