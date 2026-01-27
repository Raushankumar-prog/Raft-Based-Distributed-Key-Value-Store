# RaftKV - Asynchronous Distributed Key-Value Store

**RaftKV** is a distributed Key-Value store built in Rust, implementing the **Raft Consensus Algorithm**. It handles leader election, log replication, and consistent data storage across a cluster of nodes.

This project demonstrates **Intermediate-to-Advanced Rust** patterns, focusing on asynchronous concurrency, type safety, and modular architecture.

## 🚀 Key Technical Highlights

*   **Asynchronous Core**: Built on the **Tokio** runtime. The consensus engine uses non-blocking I/O for high-performance networking and timer management, avoiding costly thread spawning.
*   **Pluggable Architecture**:
    *   **Storage Abstraction**: Uses `#[async_trait]` to decouple the Raft logic from the underlying storage engine. This allows hot-swapping between `MemStorage` (for testing) and persistent `FileStorage`.
    *   **Network Abstraction**: Decoupled network layer allowing for easy mocking and different transport implementations (gRPC, TCP, Channels).
*   **Type-Safe Design**: Leveraging Rust's strong type system (Enums & Pattern Matching) to enforce valid state transitions and prevent "stringly typed" errors in command parsing.
*   **Workspace Structure**: organized as a Cargo Workspace with isolated crates (`raft-core`, `kv-store`, `api`) to enforce separation of concerns.

## 🏗️ Architecture

The project is split into three main crates:

-   `crates/raft-core`: The pure consensus logic. It defines the `RaftNode`, `RaftStorage` trait, and RPC types. It is completely agnostic of the web layer.
-   `crates/kv-store`: A persistent Log Structured Merge (LSM)-lite storage engine. It provides durability by appending to a write-ahead log (WAL).
-   `crates/api`: The HTTP Edge layer using **Actix-Web**. It translates REST requests into Raft commands.

## 🛠️ Usage

### Prerequisites
-   Rust (1.70+)

### Running a Node
```bash
# Start the server (defaults to port 8080)
cargo run --bin raft-server
```

### API Examples
**Set a Value (Proposes to Raft Leader)**
```bash
curl -X POST http://localhost:8080/set -d '{"key": "foo", "value": "bar"}' -H "Content-Type: application/json"
```

**Get a Value (Strongly Consistent Read)**
```bash
curl http://localhost:8080/get/foo
```

## 🧪 Testing

The project includes an InMemory Storage adapter (`MemStorage`) designed for high-speed integration testing of the consensus logic without hitting the disk.

```bash
cargo test
```
