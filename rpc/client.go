package rpc

import (
	"context"
	"fmt"

	util "github.com/maki3cat/mkraft/util"
)

var logger = util.GetSugarLogger()

type RPCRespWrapper[T RPCResponse] struct {
	Err  error
	Resp T
}

type RPCResponse interface {
	*AppendEntriesResponse | *RequestVoteResponse
}

// the real RPC wrapper used directly for the server
type InternalClientIface interface {
	SendRequestVote(ctx context.Context, req *RequestVoteRequest) chan RPCRespWrapper[*RequestVoteResponse]
	SendAppendEntries(ctx context.Context, req *AppendEntriesRequest) RPCRespWrapper[*AppendEntriesResponse]
	SayHello(ctx context.Context, req *HelloRequest) (*HelloReply, error)
	String() string
}

type InternalClientImpl struct {
	nodeId    string
	nodeAddr  string
	rawClient RaftServiceClient
}

func NewInternalClient(raftServiceClient RaftServiceClient, nodeID, nodeAddr string) InternalClientIface {
	return &InternalClientImpl{
		nodeId:    nodeID,
		nodeAddr:  nodeAddr,
		rawClient: raftServiceClient,
	}
}

func (rc *InternalClientImpl) String() string {
	return fmt.Sprintf("InternalClientImpl: %s, %s", rc.nodeId, rc.nodeAddr)
}

func (rc *InternalClientImpl) SayHello(ctx context.Context, req *HelloRequest) (*HelloReply, error) {
	return rc.rawClient.SayHello(ctx, req)
}

// the generator pattern
func (rc *InternalClientImpl) SendAppendEntries(
	ctx context.Context, req *AppendEntriesRequest) RPCRespWrapper[*AppendEntriesResponse] {
	response, err := rc.rawClient.AppendEntries(ctx, req)
	logger.Debugw("InternalClinet SendAppendEntries", "request", req, "response", response, "error", err)
	wrapper := RPCRespWrapper[*AppendEntriesResponse]{
		Resp: response,
		Err:  err,
	}
	return wrapper
}
