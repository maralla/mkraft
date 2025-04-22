// This file aims to keep requesting vote in the election period until
// the election timeout is reached OR a response is received.
// The RPC timeout is way shorter than the election timeout.
package rpc

import (
	"context"
	"fmt"
	"time"

	util "github.com/maki3cat/mkraft/util"
)

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

// the context shall be timed out in election timeout period
func (rc *InternalClientImpl) SendRequestVote(ctx context.Context, req *RequestVoteRequest) chan RPCRespWrapper[*RequestVoteResponse] {
	logger.Debugw("send request vote", "req", req)
	out := make(chan RPCRespWrapper[*RequestVoteResponse], 1)

	retriedRPC := func() {
		retryTicker := time.NewTicker(time.Millisecond * util.RPC_REUQEST_TIMEOUT_IN_MS)
		defer retryTicker.Stop()
		singleResChan := AsyncCallRequestVote(ctx, rc.rawClient, req)
		for {
			select {
			case <-ctx.Done():
				out <- RPCRespWrapper[*RequestVoteResponse]{
					Err: fmt.Errorf("%s", "election timeout for request votes")}
				return
			// to avoid race of singleResChan, and ticker
			// the ticker shall be a bit longer than the RPC timeout
			case resp := <-singleResChan:
				out <- resp
				return
			case <-retryTicker.C:
				logger.Debugw("retrying RPC", "to", rc, "req", req)
				singleResChan = AsyncCallRequestVote(ctx, rc.rawClient, req)
			}
		}
	}
	go retriedRPC()
	return out
}
