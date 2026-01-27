use raft_core::{
    AppendEntriesArgs, AppendEntriesReply, RaftNetwork, RequestVoteArgs, RequestVoteReply,
};
use reqwest::blocking::Client;

pub struct HttpRaftNetwork {
    client: Client,
}

impl HttpRaftNetwork {
    pub fn new() -> Self {
        HttpRaftNetwork {
            client: Client::new(),
        }
    }
}

impl RaftNetwork for HttpRaftNetwork {
    fn send_request_vote(&self, target: u64, args: RequestVoteArgs) -> Option<RequestVoteReply> {
        // Assume target ID maps to port 8080 + ID
        let port = 8080 + target;
        let url = format!("http://127.0.0.1:{}/raft/vote", port);

        match self.client.post(&url).json(&args).send() {
            Ok(resp) => {
                if resp.status().is_success() {
                    resp.json().ok()
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    }

    fn send_append_entries(
        &self,
        target: u64,
        args: AppendEntriesArgs,
    ) -> Option<AppendEntriesReply> {
        let port = 8080 + target;
        let url = format!("http://127.0.0.1:{}/raft/append", port);

        match self.client.post(&url).json(&args).send() {
            Ok(resp) => {
                if resp.status().is_success() {
                    resp.json().ok()
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    }
}
