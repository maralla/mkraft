package main

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

type MajorityAppendEntriesResp struct {
	Term            int
	Success         bool
	SingleResponses []AppendEntriesResponse
	OriginalRequest AppendEntriesRequest
}

type RequestVoteRequest struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteInternal struct {
	request RequestVoteRequest
	resChan chan RequestVoteResponse // which shall be a buffer of 1
}

type RequestVoteResponse struct {
	Term        int
	VoteGranted bool
}

type MajorityRequestVoteResp struct {
	Term            int
	VoteGranted     bool
	SingleResponses []RequestVoteResponse
	OriginalRequest RequestVoteRequest
	Error           error
}

type LogEntry struct {
	Term  int
	Index int
	Data  string
}

type ResWrapper struct {
	Err  error
	Resp interface{}
}
