package main

import (
	"context"
	"fmt"
	"sync"
)

func RPCSendRequestVote(ctx context.Context, req RequestVoteRequest, resChan chan RequestVoteResponse, errChan chan error, wg *sync.WaitGroup) {
	// dummy, mock
	fmt.Println("send request vote to one other nodes")
	resChan <- RequestVoteResponse{
		Term:        req.Term,
		VoteGranted: true,
	}
	wg.Done()
}
