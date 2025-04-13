
## Paper and Learning Web

<img src="img/logo2.jpg" alt="My Image" align="right" width="350">

https://raft.github.io/

## External Frameworks/Dependencies
- gRPC

## TODO List

### Week April 13-19 2025
- [ ] fix all todos and summarize into go-gymnastics
- [ ] make the first version of leaderselection runnable and playable

### Week April 7-12 2025 (All Done)
- [x] current rpc layers are unncessarily complicated, design what are really needed for appendEntries/heartbeat in consensus, the two are different (record designs)
- [x] add network of RPC (server and client)
- [x] design the simplest membership that can work, put it in gogynastics


## Module Checks
### Module0: Engineering Basics
- [x] rpc-client framework
- [X] rpc-server framework
- [x] logging
- [x] configuration management

### Module1: Leade Election
- [x] leader election
- [ ] testing: basic features
- [ ] testing: simulate adversial cases in async network env (clock drifting, delay, partition)

### Module2: Consensus
- [ ] framework: sending framework, based on term
- [ ] framework: receiving framework, based on term
- [ ] framework: sending framework, based on log and commit index
- [ ] framework: receiving framework, based on log and commmit index

### Module3: Log
- [ ] replication
- [ ] persistence
- [ ] compaction

### Module4: StateMachine
- [ ] interface for pluggable
- [ ] one pluggable example

### Module5: Membership Management
- [ ] Service Discovery
- [ ] Membership Change

### Module6: Learning of Golang for gymnastics
- [ ] async all with future pattern
- [ ] retry call with timeout
- [ ] fan-in/fan-out in RAFT
- [ ] short stopping in RAFT
- [ ] ticker, timer
- [ ] context examples summary
- [ ] plummer and channels

### Module7: Extreme Engineering for performance
- [ ] add batching to handling ClientCommands, and send AppendLogEntries
- [ ] check prof Yang's improvements of paxos for ideas
- [ ] check architecture classes of caching and memory hierarchy for ideas

## Module Explnations

## Implementation Decisions and Extreme Engineering

<img src="img/logo1.jpg" alt="My Image" align="left" width="300">