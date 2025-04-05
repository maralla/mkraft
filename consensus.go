package main

import (
	"context"
	"fmt"
)

// todo: this is a mock, we need to find the membership
func getMembership() []int {
	membership := []int{1, 2, 3, 4, 5}
	return membership
}

// todo: should use next layer RPC keep calling the unreturned nodes until the context is canceled
// only send to the channel when majority is reached OR current candidate shall end
func RequestVoteSend(ctx context.Context, request RequestVoteRequest, resultChannel chan MajorityRequestVoteResp) {
	// todo: should add a wrapper to retry until cancel
	members := getMembership()

	// maki: important for testing
	// todo: if the ctx is canceled by the fan-out not returned, will
	// the goroutine write to the channel?
	var resChan = make(chan ResWrapper, len(members))
	rpcCall := func(membershipID int) {
		resp, err := RPCClientSendRequestVote(ctx, membershipID, request)
		wrapper := ResWrapper{
			Err:  err,
			Resp: resp,
		}
		resChan <- wrapper
	}

	// FAN-OUT
	for _, member := range members {
		go rpcCall(member)
		fmt.Println("send request vote to node", member)
	}

	// FAN-IN WITH STOPPING SHORT
	total := len(members)
	majority := len(members)/2 + 1
	voteAccumulated := 0
	voteFailed := 0
	for i := 0; i < len(members); i++ {
		select {
		case res := <-resChan:
			if err := res.Err; err != nil {
				// todo: further analysis what happens in error -> lost one vote/heartbeat
				sugarLogger.Info("error in sending request vote", err)
				continue
			} else {
				term := res.Resp.(RequestVoteResponse).Term
				// if someone responds with a term greater than the current term
				if term > request.Term {
					sugarLogger.Info("term is greater than current term")
					resultChannel <- MajorityRequestVoteResp{
						Term:        term,
						VoteGranted: false,
					}
					return
				}
				if term == request.Term {
					if res.Resp.(RequestVoteResponse).VoteGranted {
						// won the election
						voteAccumulated++
						if voteAccumulated >= majority {
							resultChannel <- MajorityRequestVoteResp{
								Term:        request.Term,
								VoteGranted: true,
							}
							return
						}
					} else {
						// a fail or draw in the election
						// no need to return anything
						voteFailed++
						if voteFailed > total-majority {
							return
						}
					}
				}
				if term < request.Term {
					// todo: how to handle somecase that should never happen, should it panic?
					sugarLogger.Error("term is less than current term, this should not happen")
				}
			}
		case <-ctx.Done():
			sugarLogger.Info("context canceled")
			// right now we don't send to the result channel when timeout
			return
		}
	}
}

func AppendEntriesSend(
	ctx context.Context, request AppendEntriesRequest, respChan chan MajorityAppendEntriesResp, errChan chan error) {
	// maki: some implementation level design details to be documented
	// SEND TO ALL OTHER NODES
	// todo: implementation problem
	// synchronous OR asynchronous call
	fmt.Println("send heartbeat to all other nodes")
}
