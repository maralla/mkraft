
## Paper and Learning Web

<img src="img/logo2.jpg" alt="My Image" align="right" width="350">

https://raft.github.io/

### Basic Raft Properties and Mechanisms that guarantee them
Raft guarantees that each of these properties is true at all the time
#### Leader Election ($5.2)
- Election Safety: at most one leader can be elected in a given term (not time but term) -> ($5.2 Leader Election)
#### Log Replication ($5.3)
- Leader Append-only: a leader never overwrites or deletes entries in its logs; it only appends new entries; -> ($5.3 Log Replication)
(but followers may overwrite or truncate their logs)
- Log Matching: if two logs contain an entry with the same index and term, then logs are identical in all entries up through the given index; -> ($5.3 Log Replication)
#### Safety ($5.4)
- Leader Completeness: if a log entry is commited in a given term, then that entry will be present in the logs of the leaders for all higher numbered terms; ($5.4)
- State Machine Safety: if a server has applied a log entry at a given index to its state machine, no other server will ever apply a different log entry for the same index ($5.4.3)

## In-Progress
AppendEntries of Paper

## Backlog

### Raft Paper
- [ ] AppendEntries (WIP)
- [ ] Log Replication
- [ ] Persistence
- [ ] Membership Changes
- [ ] Log Compaction
- [ ] Example of Pluggable State Machine

### Infra Framework
- [ ] rate limiter
- [ ] pressure testing
- [ ] otel: metrics
grpc part
https://grpc.io/docs/guides/performance/
- [ ] try to change to streaming RPCs (with keep alive) or keep both and do a benchmarking
- [ ] channel optimization
- [ ] error handling
- [ ] authentication
- [ ] flow control
      
| Number | Feature Topic                                                        |
| ------ | -------------------------------------------------------------------- |
| 1      | deadline/timeout                                                     |
| 2      | graceful shutdown (https://grpc.io/docs/guides/server-graceful-stop) |
| 3      | server/client interceptors                                           |
| 4      | compression                                                          |
| 5      | retry (partially used)                                               |



<img src="img/logo1.jpg" alt="My Image" align="left" width="300">

## Patterns for GoGymnastics
- [ ] async all with future pattern
- [ ] retry call with timeout
- [ ] fan-in/fan-out in RAFT
- [ ] short stopping in RAFT
- [ ] ticker, timer
- [ ] context examples summary
- [ ] plummer and channels

## Implementation Decisions and Extreme Engineering
- [ ] add batching to handling ClientCommands, and send AppendLogEntries
- [ ] check prof Yang's improvements of paxos for ideas
- [ ] check architecture classes of caching and memory hierarchy for ideas


## History Logs

### v0.1.0 leader eleaction, server framework -- April/May 2025

## Infra Framework
- [x] configuration: unified conf file, env
- [x] docker: docker file and docker compose

#### Module2: Consensus
- [x] framework: sending framework, based on term
- [x] framework: receiving framework, based on term
- [x] framework: sending framework, based on log and commit index
- [x] framework: receiving framework, based on log and commmit index

#### Module3: Membership
- [x] Fixed Membership thru config

#### Module1: Leade Election
- [x] leader election
- [x] testing: basic features
- [x] testing: simulate adversial cases in async network env (clock drifting, delay, partition)

#### Module0: Engineering Basics
- [x] rpc-client framework
- [X] rpc-server framework
- [x] logging
- [x] configuration management

#### Bug List 
- [x] when 2 nodes run together, no one becomes the leader
- [x] when 3 nodes run together, the first leader will panic because appendEntries return the incorrect term
- [x] when running server-1, the rpc send doesn't get connection refused error -> not logged?
- [x] cancel func deffered before assigned, probably the reason to panic;
- [x] server get the request, no one handles it;

#### gRPC refinement
- [x] reading the following grpc docs to solidify current implementation
    - https://grpc.io/docs/guides/health-checking/ 
    - https://grpc.io/docs/guides/server-graceful-stop/
    - https://grpc.io/docs/guides/cancellation/
    - https://grpc.io/docs/guides/error/
    - https://grpc.io/docs/guides/deadlines/ (DONE)

### Week April 13-19 2025
- [x] fix all todos and summarize into go-gymnastics (30%)
- [x] make the first version of leaderselection runnable and playable (30%)

### Week April 7-12 2025 (All Done)
- [x] current rpc layers are unncessarily complicated, design what are really needed for appendEntries/heartbeat in consensus, the two are different (record designs)
- [x] add network of RPC (server and client)
- [x] design the simplest membership that can work, put it in gogynastics
