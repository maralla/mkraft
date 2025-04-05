// This file defines the RPC client interface and its implementation
// the RPC client is responsible for sending the RPC requests to only one other node and try to get a response
// it doesn't manage the consensus between different nodes

package rpc

import (
	"context"
	"fmt"
	"time"

	"github.com/maki3cat/mkraft/conf"
)

// TODO: maki: need to learn and revist the future and retry pattern here
// golang gymnastics
// summarize the golang patterns here:
// 1) async call with future; 2) retry pattern; 3) context with timeout/cancel; 4)template and generics

// paper: the 2 RPCs defined by the Raft paper
// membership module shall be responsible for the client to each member
type RPCClientIface interface {
	SyncSendRequestVote(ctx context.Context, req RequestVoteRequest) (RequestVoteResponse, error)
	SyncSendAppendEntries(ctx context.Context, req AppendEntriesRequest) (AppendEntriesResponse, error)
	AsyncSendRequestVote(ctx context.Context, req RequestVoteRequest) chan RPCResWrapper[RequestVoteResponse]
	AsyncSendAppendEntries(ctx context.Context, req AppendEntriesRequest) chan RPCResWrapper[AppendEntriesResponse]
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

// MAKI: NOT SURE IF THIS IS A PATTERN
// maki: how to combine the ctx timeout/cancel select and this simiple waiting
// maki: is this the simple future pattern in golang
// maki: todo: how to stop the synchronous call when timeout? -> check how RPC implements this like grpc?
func (mock *MockSimpleRPCClientImpl) AsyncSendRequestVote(ctx context.Context, req RequestVoteRequest) chan RPCResWrapper[RequestVoteResponse] {
	res := make(chan RPCResWrapper[RequestVoteResponse], 1) // buffered with 1 to prevent goroutine leak
	go func() {
		response, err := mock.SyncSendRequestVote(ctx, req)
		wrapper := RPCResWrapper[RequestVoteResponse]{
			Resp: response,
			Err:  err,
		}
		res <- wrapper
	}()
	return res
}

func (mock *MockSimpleRPCClientImpl) AsyncSendAppendEntries(ctx context.Context, req AppendEntriesRequest) chan RPCResWrapper[AppendEntriesResponse] {
	res := make(chan RPCResWrapper[AppendEntriesResponse], 1)
	go func() {
		response, err := mock.SyncSendAppendEntries(ctx, req)
		wrapper := RPCResWrapper[AppendEntriesResponse]{
			Resp: response,
			Err:  err,
		}
		res <- wrapper
	}()
	return res
}

// the complicated RPC client used directly for the server
type RetriedClientIface interface {
	RetriedSendAppendEntries(ctx context.Context, req AppendEntriesRequest) chan RPCResWrapper[AppendEntriesResponse]
	RetriedSendRequestVote(ctx context.Context, req RequestVoteRequest) chan RPCResWrapper[RequestVoteResponse]
}

type RetriedClientImpl struct {
	simpleClient RPCClientIface
}

type RetriedRPCFunc[TReq any, TRes RPCResponse] func(ctx context.Context, req TReq) chan RPCResWrapper[TRes]
type AsyncRPCFunc[TReq any, TRes RPCResponse] func(ctx context.Context, req TReq) chan RPCResWrapper[TRes]

// Using the template to create the specific RPC functions
func (rc *RetriedClientImpl) RetriedSendAppendEntries(ctx context.Context, req AppendEntriesRequest) chan RPCResWrapper[AppendEntriesResponse] {
	retrySendFunc := createRetriedRPCFunc[AppendEntriesRequest, AppendEntriesResponse](
		rc.simpleClient.AsyncSendAppendEntries,
	)
	return retrySendFunc(ctx, req)
}

func (rc *RetriedClientImpl) RetriedSendRequestVote(ctx context.Context, req RequestVoteRequest) chan RPCResWrapper[RequestVoteResponse] {
	retrySendFunc := createRetriedRPCFunc[RequestVoteRequest, RequestVoteResponse](
		rc.simpleClient.AsyncSendRequestVote,
	)
	return retrySendFunc(ctx, req)
}

// CreateRetriedRPCFunc creates a retry function for AppendEntries or RequestVote RPCs
func createRetriedRPCFunc[TReq any, TRes RPCResponse](
	asyncFunc AsyncRPCFunc[TReq, TRes],
) RetriedRPCFunc[TReq, TRes] {

	return func(ctx context.Context, req TReq) chan RPCResWrapper[TRes] {
		wrappedResChan := make(chan RPCResWrapper[TRes])
		go func() {
			defer close(wrappedResChan)

			retryTicker := time.NewTicker(time.Millisecond * conf.RPC_REUQEST_TIMEOUT_IN_MS)
			defer retryTicker.Stop()

			var singleResChan chan RPCResWrapper[TRes]
			var singleCallCtx context.Context
			var singleCallCancel context.CancelFunc
			singleCallCtx, singleCallCancel = context.WithTimeout(ctx, time.Millisecond*(conf.RPC_REUQEST_TIMEOUT_IN_MS-10))
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
					singleCallCtx, singleCallCancel = context.WithTimeout(ctx, time.Millisecond*conf.RPC_REUQEST_TIMEOUT_IN_MS)
					singleResChan = asyncFunc(singleCallCtx, req)
				case response := <-singleResChan:
					// Got a response
					singleCallCancel()
					wrappedResChan <- response
					return
				}
			}
		}()
		return wrappedResChan
	}
}
