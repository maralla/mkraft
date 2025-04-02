package main

import (
	"context"
	"fmt"
)

// todo: the naming with client and server right now is to distinguish different functions

// should be both a rpc server and a rpc client
// send and receive shall be async so that we don't need to wait for all resposes
func ClientSendRequestVoteToAll(ctx context.Context, request RequestVoteRequest, resultChannel chan RequestVoteResponse) error {
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

func ClientSendAppendEntriesToAll(ctx context.Context, request AppendEntriesRequest) error {
	// send to all other nodes
	fmt.Println("need to find all other nodes")
	return nil
}
