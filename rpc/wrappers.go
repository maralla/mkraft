package rpc

type RPCResWrapper[T RPCResponse] struct {
	Err  error
	Resp T
}

type RPCResponse interface {
	AppendEntriesResponse | RequestVoteResponse
}
