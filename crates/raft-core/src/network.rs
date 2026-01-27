use crate::rpc::{AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply};
use async_trait::async_trait;

#[async_trait]
pub trait RaftNetwork: Send + Sync + 'static {
    async fn send_request_vote(
        &self,
        target: u64,
        args: RequestVoteArgs,
    ) -> Option<RequestVoteReply>;
    async fn send_append_entries(
        &self,
        target: u64,
        args: AppendEntriesArgs,
    ) -> Option<AppendEntriesReply>;
}
