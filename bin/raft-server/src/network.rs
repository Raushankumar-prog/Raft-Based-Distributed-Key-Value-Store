use async_trait::async_trait;
use raft_core::{
    AppendEntriesArgs, AppendEntriesReply, RaftNetwork, RequestVoteArgs, RequestVoteReply,
};
use reqwest::Client;

#[derive(Clone)]
pub struct HttpRaftNetwork {
    client: Client,
}

impl HttpRaftNetwork {
    pub fn new() -> Self {
        HttpRaftNetwork {
            client: Client::builder()
                .timeout(std::time::Duration::from_millis(500))
                .build()
                .unwrap_or_else(|_| Client::new()),
        }
    }
}

#[async_trait]
impl RaftNetwork for HttpRaftNetwork {
    async fn send_request_vote(
        &self,
        target: u64,
        args: RequestVoteArgs,
    ) -> Option<RequestVoteReply> {
        let port = 8080 + target;
        let url = format!("http://127.0.0.1:{}/raft/vote", port);

        match self.client.post(&url).json(&args).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    resp.json().await.ok()
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    }

    async fn send_append_entries(
        &self,
        target: u64,
        args: AppendEntriesArgs,
    ) -> Option<AppendEntriesReply> {
        let port = 8080 + target;
        let url = format!("http://127.0.0.1:{}/raft/append", port);

        match self.client.post(&url).json(&args).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    resp.json().await.ok()
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    }
}

