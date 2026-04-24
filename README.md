# miniRaft 
This is a class project from Wellesley's CS343: Distributed Computing. This project implements [Raft Distributed Consensus Algorithm](https://raft.github.io/) in Go, built to demonstrate core distributed systems concepts including leader election, log replication, and fault tolerance. This implementation covers the core Raft protocol:
- **Leader election** with randomized election timeouts
- **Log replication** from leader to followers
- **Commit index advancement** once a majority acknowledges an entry
- **Heartbeat mechanism** to suppress spurious elections
- **Simulated node failure and recovery** 

## Key Components
 
| Function | Purpose |
|---|---|
| `RequestVote` | RPC handler; processes vote requests, enforces log up-to-date check |
| `AppendEntry` | RPC handler; handles heartbeats and log replication |
| `LeaderElection` | Drives the candidate vote-gathering process |
| `Heartbeat` | Sends periodic AppendEntry RPCs to maintain leadership |
| `replicateToFollowers` | Replicates new log entries with retry/backoff on failure |
| `applyEntries` | Background goroutine applying committed entries to the state machine |
| `ClientAddToLog` | Simulates client requests by appending entries on the leader |
| `simulateFailure` | Randomly takes a node offline and brings it back |
 
---

## To Run miniRaft
1. This repository includes a port configuration file cluster.txt that contains five random local host port addresses. You can run miniRaft with five processes with cluster.txt, or configure your own clusters.

2. To simulate the cluster, start each node in a separate terminal, passing its zero-based ID and the config file:

  ```
   go run raftNode.go 0 cluster.txt
   go run raftNode.go 1 cluster.txt
   go run raftNode.go 2 cluster.txt
   ```

