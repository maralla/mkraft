
## Paper and Learning Web

<img src="pic/logo2.jpg" alt="My Image" align="right" width="350">

https://raft.github.io/

## TODO List
### Week April 7-12 2025
- [x] current rpc layers are unncessarily complicated, design what are really needed for appendEntries/heartbeat in consensus, the two are different (record designs)
- [ ] add network of RPC (server and client)
- [ ] static membership
- [ ] read go blogs and concurrency pattern -> refine and summarize current patterns to gogymnastics

## Modular Progress Checks

### Module0: Engineering Basics
- [ ] rpc-client framework
- [ ] rpc-real protocol grpc?
- [ ] rpc-server framework
- [ ] configuration management
- [x] logging
- [ ] metrics

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