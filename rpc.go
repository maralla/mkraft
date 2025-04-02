package main

import (
	"context"
	"fmt"
)

// should be both a rpc server and a rpc client

func SendRequestVoteToAll(ctx context.Context, nodeID int, term int) error {
	// send to all other nodes
	fmt.Println("send request vote to all other nodes of term, nodeID", term, nodeID)
	return nil
}

func SendAppendEntriesToAll() {
	// send to all other nodes
}

// server side
func RequestVoteHandler() {

}

func AppendEntriesHandler() {

}
