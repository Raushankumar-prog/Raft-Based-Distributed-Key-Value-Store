use async_trait::async_trait;
use raft_core::{
    AppendEntriesArgs, AppendEntriesReply, ClientCommand, MemStorage, RaftHandle, RaftNetwork,
    RaftNode, RaftRole, RequestVoteArgs, RequestVoteReply,
};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

struct MockNetwork;

#[async_trait]
impl RaftNetwork for MockNetwork {
    async fn send_request_vote(
        &self,
        _target: u64,
        _args: RequestVoteArgs,
    ) -> Option<RequestVoteReply> {
        None
    }
    async fn send_append_entries(
        &self,
        _target: u64,
        _args: AppendEntriesArgs,
    ) -> Option<AppendEntriesReply> {
        None
    }
}

#[derive(Clone)]
struct ChannelClusterNetwork {
    nodes: Arc<RwLock<HashMap<u64, RaftHandle>>>,
    partitioned: Arc<RwLock<HashSet<(u64, u64)>>>,
}

impl ChannelClusterNetwork {
    fn new() -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            partitioned: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    fn register_node(&self, id: u64, handle: RaftHandle) {
        self.nodes.write().unwrap().insert(id, handle);
    }

    fn partition(&self, a: u64, b: u64) {
        let mut p = self.partitioned.write().unwrap();
        p.insert((a, b));
        p.insert((b, a));
    }
}

#[async_trait]
impl RaftNetwork for ChannelClusterNetwork {
    async fn send_request_vote(
        &self,
        target: u64,
        args: RequestVoteArgs,
    ) -> Option<RequestVoteReply> {
        let from = args.candidate_id;
        if self.partitioned.read().unwrap().contains(&(from, target)) {
            return None;
        }
        let handle = self.nodes.read().unwrap().get(&target).cloned()?;
        handle.request_vote(args).await.ok()
    }

    async fn send_append_entries(
        &self,
        target: u64,
        args: AppendEntriesArgs,
    ) -> Option<AppendEntriesReply> {
        let from = args.leader_id;
        if self.partitioned.read().unwrap().contains(&(from, target)) {
            return None;
        }
        let handle = self.nodes.read().unwrap().get(&target).cloned()?;
        handle.append_entries(args).await.ok()
    }
}

#[tokio::test]
async fn test_single_node_raft_actor_and_proposal() {
    let (apply_tx, mut apply_rx) = mpsc::channel(10);
    let handle = RaftNode::spawn(1, vec![], MockNetwork, MemStorage::new(), Some(apply_tx));

    // Sleep 650ms to ensure election deadline threshold (300-600ms) passes
    sleep(Duration::from_millis(650)).await;

    let (_term, role) = handle.get_state().await.unwrap();
    assert_eq!(role, RaftRole::Leader);

    // Propose a command as leader over oneshot channel
    let res = handle
        .propose(ClientCommand::Set {
            key: "name".to_string(),
            value: "raft".to_string(),
        })
        .await;

    assert!(res.is_ok());

    // Verify applied committed entry was received via mpsc channel
    let applied = apply_rx.recv().await;
    assert_eq!(applied, Some(("name".to_string(), "raft".to_string())));
}

#[tokio::test]
async fn test_three_node_cluster_network_partition() {
    let network = ChannelClusterNetwork::new();
    let (tx1, _rx1) = mpsc::channel(10);
    let (tx2, _rx2) = mpsc::channel(10);
    let (tx3, _rx3) = mpsc::channel(10);

    let h1 = RaftNode::spawn(1, vec![2, 3], network.clone(), MemStorage::new(), Some(tx1));
    let h2 = RaftNode::spawn(2, vec![1, 3], network.clone(), MemStorage::new(), Some(tx2));
    let h3 = RaftNode::spawn(3, vec![1, 2], network.clone(), MemStorage::new(), Some(tx3));

    network.register_node(1, h1.clone());
    network.register_node(2, h2.clone());
    network.register_node(3, h3.clone());

    // Allow time for leader election across 3 nodes
    sleep(Duration::from_millis(800)).await;

    // Find the elected leader
    let handles = vec![(1u64, &h1), (2u64, &h2), (3u64, &h3)];
    let mut leader_id = 0u64;
    let mut leader_handle = None;

    for (id, handle) in &handles {
        if let Ok((_term, RaftRole::Leader)) = handle.get_state().await {
            leader_id = *id;
            leader_handle = Some((*handle).clone());
            break;
        }
    }

    assert!(leader_handle.is_some(), "A leader should be elected in a 3-node cluster");
    let leader_handle = leader_handle.unwrap();

    // Propose a command to the leader
    let res = leader_handle
        .propose(ClientCommand::Set {
            key: "k1".to_string(),
            value: "v1".to_string(),
        })
        .await;

    assert!(res.is_ok(), "Leader proposal should succeed with majority quorum");

    // Partition the leader from all peers (isolating the leader into a minority partition of 1)
    let non_leaders: Vec<u64> = vec![1, 2, 3].into_iter().filter(|&id| id != leader_id).collect();
    for peer in &non_leaders {
        network.partition(leader_id, *peer);
    }

    // Now proposals on the isolated old leader must fail or time out because it cannot reach a majority quorum
    let fail_res = tokio::time::timeout(
        Duration::from_millis(400),
        leader_handle.propose(ClientCommand::Set {
            key: "k2".to_string(),
            value: "v2".to_string(),
        }),
    )
    .await;

    assert!(
        fail_res.is_err() || fail_res.unwrap().is_err(),
        "Isolated leader must not commit without majority quorum"
    );
}

