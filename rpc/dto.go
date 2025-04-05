package rpc

type AppendEntriesRequest struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesResponse struct {
	Term    int
	Success bool
}

type RequestVoteRequest struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteResponse struct {
	Term        int
	VoteGranted bool
}

type LogEntry struct {
	Term  int
	Index int
	Data  string
}

type RPCResWrapper[T RPCResponse] struct {
	Err  error
	Resp T
}

type RPCResponse interface {
	AppendEntriesResponse | RequestVoteResponse
}
