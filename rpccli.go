package main

import (
	"context"
	"fmt"
)

func RPCSendRequestVote(ctx context.Context, req RequestVoteRequest, resChan chan RequestVoteResponse, errChan chan error) {
	// dummy, mock
	fmt.Println("send request vote to one other nodes")
	resChan <- RequestVoteResponse{
		Term:        req.Term,
		VoteGranted: true,
	}
}
