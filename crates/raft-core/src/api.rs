use crate::rpc::{AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply};

pub trait RaftApi: Send + Sync {
    fn on_request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply;
    fn on_append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply;
}
