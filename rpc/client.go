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

	// send append entries is one simple sync rpc call with rpc timeout
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

func (rc *InternalClientImpl) SendAppendEntries(ctx context.Context, req *AppendEntriesRequest) RPCRespWrapper[*AppendEntriesResponse] {
	resp, err := rc.syncCallAppendEntries(ctx, req)
	wrapper := RPCRespWrapper[*AppendEntriesResponse]{
		Resp: resp,
		Err:  err,
	}
	return wrapper
}

func (rc *InternalClientImpl) syncCallAppendEntries(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	rpcTimeout := util.GetConfig().GetRPCRequestTimeout()
	singleCallCtx, singleCallCancel := context.WithTimeout(ctx, rpcTimeout)
	defer singleCallCancel()

	resp, err := rc.rawClient.AppendEntries(singleCallCtx, req)
	if err != nil {
		logger.Errorw("single RPC error in SendAppendEntries:", "to", rc.rawClient, "error", err)
	} else {
		logger.Debugw("single RPC SendAppendEntries response:", "member", rc, "response", resp)
	}
	return resp, err
}

// the context shall be timed out in election timeout period
func (rc *InternalClientImpl) SendRequestVoteWithRetries(ctx context.Context, req *RequestVoteRequest) chan RPCRespWrapper[*RequestVoteResponse] {
	logger.Debugw("send SendRequestVote", "req", req)
	out := make(chan RPCRespWrapper[*RequestVoteResponse], 1)

	retriedRPC := func() {
		singleResChan := rc.asyncCallRequestVote(ctx, req)
		for {
			select {
			case <-ctx.Done():
				out <- RPCRespWrapper[*RequestVoteResponse]{
					Err: fmt.Errorf("%s", "election timeout to receive any non-error response")}
				return
			case resp := <-singleResChan:
				if resp.Err != nil {
					deadline, ok := ctx.Deadline()
					// todo: move to config
					logger.Debugw("retrying RPC, deadline:", "deadline", deadline)
					if ok && time.Until(deadline) < util.GetConfig().GetMinRemainingTimeForRPC() {
						out <- RPCRespWrapper[*RequestVoteResponse]{
							Err: fmt.Errorf("%s", "election timeout to receive any non-error response")}
						return
					} else {
						logger.Errorw("need retry, RPC error:", "to", rc, "error", resp.Err)
						singleResChan = rc.asyncCallRequestVote(ctx, req)
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
func (rc *InternalClientImpl) syncCallRequestVote(ctx context.Context, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	rpcTimeout := util.GetConfig().GetRPCRequestTimeout()
	singleCallCtx, singleCallCancel := context.WithTimeout(ctx, rpcTimeout)
	defer singleCallCancel()

	resp, err := rc.rawClient.RequestVote(singleCallCtx, req)
	if err != nil {
		logger.Errorw("single RPC error:", "to", rc, "error", err)
	} else {
		logger.Debugw("single RPC response:", "member", rc, "response", resp)
	}
	return resp, err
}

func (rc *InternalClientImpl) asyncCallRequestVote(ctx context.Context, req *RequestVoteRequest) chan RPCRespWrapper[*RequestVoteResponse] {
	singleResChan := make(chan RPCRespWrapper[*RequestVoteResponse], 1) // must be buffered
	go func() {
		resp, err := rc.syncCallRequestVote(ctx, req)
		wrapper := RPCRespWrapper[*RequestVoteResponse]{
			Resp: resp,
			Err:  err,
		}
		singleResChan <- wrapper
	}()
	return singleResChan
}
