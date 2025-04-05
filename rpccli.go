package main

import (
	"context"
	"fmt"
)

func RPCClientSendRequestVote(ctx context.Context, memberID int, req RequestVoteRequest) (RequestVoteResponse, error) {
	// dummy, mock
	fmt.Println("send request vote to one other nodes")
	return RequestVoteResponse{
		Term:        req.Term,
		VoteGranted: true,
	}, nil
}
