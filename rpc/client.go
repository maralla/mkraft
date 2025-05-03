package rpc

import (
	"context"
	"fmt"
	"time"

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

type InternalClientIface interface {

	// send request vote is keep retrying until the context is done or the response is received
	SendRequestVoteWithRetries(ctx context.Context, req *RequestVoteRequest) chan RPCRespWrapper[*RequestVoteResponse]

	// send append entries is a simple sync rpc call
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

func (rc *InternalClientImpl) SendAppendEntries(
	ctx context.Context, req *AppendEntriesRequest) RPCRespWrapper[*AppendEntriesResponse] {
	singleCallCtx, singleCallCancel := context.WithTimeout(ctx, time.Millisecond*(util.RPC_REUQEST_TIMEOUT_IN_MS-10))
	defer singleCallCancel()

	resp, err := rc.rawClient.AppendEntries(singleCallCtx, req)
	if err != nil {
		logger.Errorw("single RPC error in SendAppendEntries:", "to", rc.rawClient, "error", err)
	} else {
		logger.Debugw("single RPC SendAppendEntries response:", "member", rc, "response", resp)
	}

	wrapper := RPCRespWrapper[*AppendEntriesResponse]{
		Resp: resp,
		Err:  err,
	}
	return wrapper
}

// the context shall be timed out in election timeout period
func (rc *InternalClientImpl) SendRequestVoteWithRetries(ctx context.Context, req *RequestVoteRequest) chan RPCRespWrapper[*RequestVoteResponse] {
	logger.Debugw("send SendRequestVote", "req", req)
	out := make(chan RPCRespWrapper[*RequestVoteResponse], 1)

	retriedRPC := func() {
		singleResChan := AsyncCallRequestVote(ctx, rc.rawClient, req)
		for {
			select {
			case <-ctx.Done():
				out <- RPCRespWrapper[*RequestVoteResponse]{
					Err: fmt.Errorf("%s", "election timeout for request votes")}
				return
			case resp := <-singleResChan:
				if resp.Err != nil {
					deadline, ok := ctx.Deadline()
					if ok && time.Until(deadline) < util.GetConfig().GetRPCRequestTimeout() {
						out <- resp
						return
					} else {
						logger.Errorw("need retry, RPC error:", "to", rc, "error", resp.Err)
						singleResChan = AsyncCallRequestVote(ctx, rc.rawClient, req)
						continue
					}
				} else {
					out <- resp
					return
				}
			}
		}
	}
	go retriedRPC()
	return out
}

// https://grpc.io/docs/guides/deadlines/
// https://github.com/grpc/grpc-go/blob/master/examples/features/deadline/client/main.go
func SyncCallRequestVote(ctx context.Context, rc RaftServiceClient, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	singleCallCtx, singleCallCancel := context.WithTimeout(ctx, time.Millisecond*(util.RPC_REUQEST_TIMEOUT_IN_MS-10))
	defer singleCallCancel()
	resp, err := rc.RequestVote(singleCallCtx, req)
	if err != nil {
		logger.Errorw("single RPC error:", "to", rc, "error", err)
	} else {
		logger.Debugw("single RPC response:", "member", rc, "response", resp)
	}
	return resp, err
}

func AsyncCallRequestVote(ctx context.Context, rc RaftServiceClient, req *RequestVoteRequest) chan RPCRespWrapper[*RequestVoteResponse] {
	singleResChan := make(chan RPCRespWrapper[*RequestVoteResponse], 1) // must be buffered
	go func() {
		resp, err := SyncCallRequestVote(ctx, rc, req)
		wrapper := RPCRespWrapper[*RequestVoteResponse]{
			Resp: resp,
			Err:  err,
		}
		singleResChan <- wrapper
	}()
	return singleResChan
}
