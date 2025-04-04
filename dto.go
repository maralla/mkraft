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

type MajorityRequestVoteResp struct {
	Term            int
	VoteGranted     bool
	SingleResponses []RequestVoteResponse
}

type LogEntry struct {
	Term  int
	Index int
	Data  string
}
