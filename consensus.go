package main

import (
	"context"
	"fmt"
	"sync"
)

// todo: this is a mock, we need to find the membership
func getMembership() []int {
	membership := []int{1, 2, 3, 4, 5}
	return membership
}

// here the pattern is there are K nodes in the membership found by the client
// and the client sends the request to all other nodes and waits for the majority with a response

// todo: the naming with client and server right now is to distinguish different functions
// should be both a rpc server and a rpc client
// send and receive shall be async so that we don't need to wait for all resposes
func ClientSendRequestVoteToAll(ctx context.Context, request RequestVoteRequest, resultChannel chan MajorityRequestVoteResp) error {
	members := getMembership()
	var wg sync.WaitGroup
	var resChan = make(chan RequestVoteResponse, len(members))
	var errChan = make(chan error, len(members))
	ctxTimeout, cancel := context.WithTimeout(ctx, REUQEST_TIMEOUT_IN_MS)
	defer cancel()

	majority := len(members)/2 + 1
	for i := 0; i < majority; i++ {
		wg.Add(1)
	}

	// the fan-in fan-out pattern
	for _, member := range members {
		go RPCSendRequestVote(ctxTimeout, request, resChan, errChan, &wg)
		if member == request.CandidateID {
			continue
		}
		fmt.Println("send request vote to node", member)
	}

	// send to all other nodes
	fmt.Println("need to find all other nodes")
	fmt.Println("in parallel, send request vote to all other nodes of term, nodeID", request.Term, request.CandidateID)

	// if we use synchronous ?
	fmt.Println("future wait for majority of votes with a timeout")

	// todo: to be implemented, so we plan to add the majority here?
	// assumes majority comes back
	// assumes timeout happens -> the ctx requires a timeout
	// what happens if the ctx timeouts and the reponse came back? who handles it
	return nil
}

func ClientSendAppendEntriesToAll(
	ctx context.Context, request AppendEntriesRequest, respChan chan MajorityAppendEntriesResp, errChan chan error) {
	// maki: some implementation level design details to be documented
	// SEND TO ALL OTHER NODES
	// todo: implementation problem
	// synchronous OR asynchronous call
	fmt.Println("send heartbeat to all other nodes")
}
