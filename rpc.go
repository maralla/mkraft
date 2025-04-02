package main

import (
	"context"
	"fmt"
)

// todo: the naming with client and server right now is to distinguish different functions

// should be both a rpc server and a rpc client
// send and receive shall be async so that we don't need to wait for all resposes
func ClientSendRequestVoteToAll(ctx context.Context, request RequestVoteRequest) error {
	// send to all other nodes
	fmt.Println("need to find all other nodes")
	fmt.Println("send request vote to all other nodes of term, nodeID", request.Term, request.CandidateID)
	return nil
}

func ClientSendAppendEntriesToAll(ctx context.Context, request AppendEntriesRequest) error {
	// send to all other nodes
	fmt.Println("need to find all other nodes")
	return nil
}

// server side
func ServerRequestVoteHandler() {

}

func ServerAppendEntriesHandler() {

}

func ServerCallbackRequestVoteResponse(response RequestVoteResponse) {
}

func ServerCallbackAppendEntriesResponse(response AppendEntriesResponse) {
}
