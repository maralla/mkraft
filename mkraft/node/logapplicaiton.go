package node

// Implementation gap: Raft log application behavior across different node roles and transitions
// Implementation gap: is complicated and not well documented by the paper; thus, I dedicate a file to this.

// AS A Leader, newly elected
// Case(1) When a follower becomes the leader,
// it needs to apply previous committed logs (which doesn't require replies to the clients)
// to the state machine before it can reply to new client commands

// AS A Leader, while serving
// Case(2) When a leader stays as the leader,
// it needs to apply new committed logs to the state machine
// and reply to the clients

// AS A Leader, but stale
// Case(3) When a leader finds itself stale
// it needs to finish the committed yet not applied logs and reply to the clients
// meanwhile not accepting new client

// AS A Follower/Candidate
// Case(4) When a follower/candidate commits a new log
// it needs to apply the log to the state machine but doesn't need to reply to the clients
