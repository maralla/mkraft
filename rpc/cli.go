// This file defines the RPC client interface and its implementation
// the RPC client is responsible for sending the RPC requests to only one other node and try to get a response
// it doesn't manage the consensus between different nodes
// TODO: maki: need to learn and revist the future and retry pattern here
// golang gymnastics
// summarize the golang patterns here:
// 1) async call with future; 2) retry pattern; 3) context with timeout/cancel; 4)template and generics

// paper: the 2 RPCs defined by the Raft paper
// membership module shall be responsible for the client to each member

package rpc

import (
	"context"
	"fmt"
	"time"

	util "github.com/maki3cat/mkraft/util"
)

// the real RPC iface witch will handle cancel/timeout with network solutions
// will plugin in gRPC
type RPCClientIface interface {
	SyncSendRequestVote(ctx context.Context, req RequestVoteRequest) (RequestVoteResponse, error)
	SyncSendAppendEntries(ctx context.Context, req AppendEntriesRequest) (AppendEntriesResponse, error)
}

func NewRPCClient() RPCClientIface {
	return &MockSimpleRPCClientImpl{}
}

type MockSimpleRPCClientImpl struct {
	// dummy, mock
}

func (mock *MockSimpleRPCClientImpl) SyncSendRequestVote(ctx context.Context, req RequestVoteRequest) (RequestVoteResponse, error) {
	fmt.Println("send request vote to one other nodes")
	return RequestVoteResponse{
		Term:        req.Term,
		VoteGranted: true,
	}, nil
}

func (mock *MockSimpleRPCClientImpl) SyncSendAppendEntries(ctx context.Context, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	fmt.Println("send append entries to one other nodes")
	return AppendEntriesResponse{
		Term:    req.Term,
		Success: true,
	}, nil
}

// the real RPC wrapper used directly for the server
type InternalClientIface interface {
	RetriedSendRequestVote(ctx context.Context, req RequestVoteRequest, wrappedResChan chan RPCResWrapper[RequestVoteResponse])
	AsyncSendRequestVote(ctx context.Context, req RequestVoteRequest) chan RPCResWrapper[RequestVoteResponse]
	SendAppendEntries(ctx context.Context, req AppendEntriesRequest, resChan chan RPCResWrapper[AppendEntriesResponse])
}

func NewInternalClient(simpleClient RPCClientIface) InternalClientIface {
	return &RetriedClientImpl{
		simpleClient: simpleClient,
	}
}

type RetriedClientImpl struct {
	simpleClient RPCClientIface
}

type RetriedRPCFunc[TReq any, TRes RPCResponse] func(ctx context.Context, req TReq) chan RPCResWrapper[TRes]
type AsyncRPCFunc[TReq any, TRes RPCResponse] func(ctx context.Context, req TReq) chan RPCResWrapper[TRes]

func (rc *RetriedClientImpl) RetriedSendRequestVote(ctx context.Context, req RequestVoteRequest, wrappedResChan chan RPCResWrapper[RequestVoteResponse]) {
	retriedRPCFunc(rc.AsyncSendRequestVote, ctx, req, wrappedResChan)
}

// CreateRetriedRPCFunc creates a retry function for AppendEntries or RequestVote RPCs
func retriedRPCFunc[TReq any, TRes RPCResponse](
	asyncFunc AsyncRPCFunc[TReq, TRes], ctx context.Context, req TReq, wrappedResChan chan RPCResWrapper[TRes]) {
	defer close(wrappedResChan)

	retryTicker := time.NewTicker(time.Millisecond * util.RPC_REUQEST_TIMEOUT_IN_MS)
	defer retryTicker.Stop()

	var singleResChan chan RPCResWrapper[TRes]
	var singleCallCtx context.Context
	var singleCallCancel context.CancelFunc

	singleCallCtx, singleCallCancel = context.WithTimeout(ctx, time.Millisecond*(util.RPC_REUQEST_TIMEOUT_IN_MS-10))
	singleResChan = asyncFunc(singleCallCtx, req)

	for {
		select {
		case <-ctx.Done():
			// Parent context canceled or timed out
			singleCallCancel()
			wrappedResChan <- RPCResWrapper[TRes]{Err: fmt.Errorf("context done")}
			return
		case <-retryTicker.C:
			// No response within timeout, retry
			singleCallCancel()
			singleCallCtx, singleCallCancel = context.WithTimeout(ctx, time.Millisecond*util.RPC_REUQEST_TIMEOUT_IN_MS)
			singleResChan = asyncFunc(singleCallCtx, req)
		case response := <-singleResChan:
			// Got a response
			singleCallCancel()
			wrappedResChan <- response
			return
		}
	}
}

func (rc *RetriedClientImpl) AsyncSendRequestVote(ctx context.Context, req RequestVoteRequest) chan RPCResWrapper[RequestVoteResponse] {
	res := make(chan RPCResWrapper[RequestVoteResponse], 1) // buffered with 1 to prevent goroutine leak
	go func() {
		// todo: this should be the real rpc
		response, err := rc.simpleClient.SyncSendRequestVote(ctx, req)
		wrapper := RPCResWrapper[RequestVoteResponse]{
			Resp: response,
			Err:  err,
		}
		res <- wrapper
	}()
	return res
}

func (rc *RetriedClientImpl) SendAppendEntries(
	ctx context.Context, req AppendEntriesRequest, resChan chan RPCResWrapper[AppendEntriesResponse]) {
	// currently no retries for AppendEntries, if timeout, the err shall be timeout
	ctx, cancel := context.WithTimeout(ctx, util.Config.RPCRequestTimeout)
	defer cancel()
	// the ctx with timeout should be handled by the network in the sync call
	response, err := rc.simpleClient.SyncSendAppendEntries(ctx, req)
	wrapper := RPCResWrapper[AppendEntriesResponse]{
		Resp: response,
		Err:  err,
	}
	resChan <- wrapper
}
