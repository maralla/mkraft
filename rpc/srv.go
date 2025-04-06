package rpc

// server interface
// shall be implemented by a grpc server and try to make an abstract interface
type RPCServerIface interface {
	SendRequestVote(request RequestVoteRequest) RequestVoteResponse
	SendAppendEntries(request AppendEntriesRequest) AppendEntriesResponse
}
