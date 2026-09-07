# RaftKV - Asynchronous Distributed Key-Value Store

**RaftKV** is a distributed Key-Value store built in Rust, implementing the **Raft Consensus Algorithm**. It handles leader election, log replication, and consistent data storage across a multi-node cluster.

This project focuses on asynchronous Rust system design, leveraging Tokio actors, lock-free message passing, and modular Cargo workspace architecture.

## 🚀 Key Technical Highlights

*   **Actor-Based Concurrency**: Built on the **Tokio** runtime. The `RaftNode` runs as an asynchronous Actor inside a `tokio::select!` event loop, processing commands and RPCs via `mpsc` channels to avoid mutex lock-contention across `.await` points.
*   **Quorum-Based Write Guarantees**: Proposals block non-blockingly using `oneshot` response channels and only resolve HTTP responses (`200 OK`) once log entries are committed by a majority quorum.
*   **Parallel RPC Broadcasts**: Candidate elections and leader log replication fan out concurrently to all peer nodes using spawned Tokio tasks.
*   **Pluggable Architecture**:
    *   **Storage Abstraction**: Uses `#[async_trait]` to decouple consensus state management (`RaftStorage`) from the physical storage engine, supporting in-memory (`MemStorage`) and persistent disk backends.
    *   **Network Abstraction**: Decoupled transport layer (`RaftNetwork`) allowing seamless mocking in unit tests and HTTP/TCP transports in production.
*   **Workspace Organization**: Split into isolated Cargo workspace crates (`raft-core`, `kv-store`, `api`, `raft-server`, `raft-client`) enforcing strict domain boundaries.

## 🏗️ Architecture

The project is structured into three main library crates and binary targets:

-   `crates/raft-core`: Pure consensus state machine (`RaftNode`), Actor handle (`RaftHandle`), `RaftStorage` trait, and RPC payload definitions.
-   `crates/kv-store`: Durable Write-Ahead Log (WAL) storage engine backed by JSON log persistence and state restoration on startup.
-   `crates/api`: Edge HTTP API layer built on **Actix-Web**, translating REST endpoints into Raft actor proposals.
-   `bin/raft-server`: Server binary instantiating consensus actors, storage applier, and HTTP service.

## 🛠️ Usage

### Prerequisites
-   Rust (1.70+)

### Running a Cluster Node
```bash
# Start Node 1 (id: 1, peers: 2, 3)
cargo run --bin raft-server 1 2 3

# Start Node 2 (id: 2, peers: 1, 3)
cargo run --bin raft-server 2 1 3

# Start Node 3 (id: 3, peers: 1, 2)
cargo run --bin raft-server 3 1 2
```

### API Examples
**Set a Key-Value Pair (Proposes to Raft Leader)**
```bash
curl -X POST http://localhost:8081/set -d '{"key": "foo", "value": "bar"}' -H "Content-Type: application/json"
```

**Get a Value**
```bash
curl http://localhost:8081/get/foo
```

## 🧪 Testing

The test suite validates consensus state transitions, actor message handling, and fault tolerance under network partitions:

```bash
cargo test
```

### Key Tests Included
- `test_single_node_raft_actor_and_proposal`: Tests actor initialization, election timeout, and proposal commit flow.
- `test_three_node_cluster_network_partition`: Simulates a 3-node cluster, elects a leader, partitions the leader from its peers, and verifies that proposal commits fail when majority quorum is lost.

