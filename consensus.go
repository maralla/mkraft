package main

import (
	"context"

	"github.com/maki3cat/mkraft/rpc"
)

type MajorityAppendEntriesResp struct {
	Term            int
	Success         bool
	SingleResponses []rpc.AppendEntriesResponse
	OriginalRequest rpc.AppendEntriesRequest
}

type MajorityRequestVoteResp struct {
	Term            int
	VoteGranted     bool
	SingleResponses []rpc.RequestVoteResponse
	OriginalRequest rpc.RequestVoteRequest
	Error           error
}

// CONSENSUS MODULE
// todo: currently the result channel only retruns when there is win/fail for sure
func RequestVoteSend(ctx context.Context, request rpc.RequestVoteRequest, resultChannel chan MajorityRequestVoteResp) {

	members := getClientOfAllMembers()
	resChan := make(chan rpc.RPCResWrapper[rpc.RequestVoteResponse], len(members)) // buffered with len(members) to prevent goroutine leak

	// FAN-OUT
	for _, member := range members {
		go member.RetriedSendRequestVote(ctx, request, resChan)
	}

	// FAN-IN WITH STOPPING SHORT
	total := len(members)
	majority := len(members)/2 + 1
	voteAccumulated := 0
	voteFailed := 0
	// todo: or should use for true range?
	for i := 0; i < len(members); i++ {
		select {
		case res := <-resChan:
			if err := res.Err; err != nil {
				sugarLogger.Error("error in sending request vote to one node", err)
				continue
			} else {
				resp := res.Resp
				// if someone responds with a term greater than the current term
				if resp.Term > request.Term {
					sugarLogger.Info("term is greater than current term")
					resultChannel <- MajorityRequestVoteResp{
						Term:        resp.Term,
						VoteGranted: false,
					}
					return
				}
				if resp.Term == request.Term {
					if resp.VoteGranted {
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
						// todo: not sure if this the right logic to do this
						// a fail or draw in the election, unsure
						// no need to return anything
						voteFailed++
						if voteFailed > total-majority {
							return
						}
					}
				}
				if resp.Term < request.Term {
					sugarLogger.Error("invairant failed, smaller term is not overwritten by larger term")
					panic("this should not happen, the consensus algorithm is not implmented correctly")
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
	ctx context.Context, request rpc.AppendEntriesRequest, respChan chan MajorityAppendEntriesResp, errChan chan error) {
	members := getClientOfAllMembers()
	resChan := make(chan rpc.RPCResWrapper[rpc.AppendEntriesResponse], len(members)) // buffered with len(members) to prevent goroutine leak
	// FAN-OUT
	for _, member := range members {
		go member.RetriedSendAppendEntries(ctx, request, resChan)
	}
	// FAN-IN WITH STOPPING SHORT
	total := len(members)
	majority := len(members)/2 + 1
	successAccumulated := 0
	successFailed := 0
	for i := 0; i < len(members); i++ {
		select {
		case res := <-resChan:
			if err := res.Err; err != nil {
				sugarLogger.Error("error in sending append entries to one node", err)
				continue
			} else {
				resp := res.Resp
				if resp.Term > request.Term {
					sugarLogger.Info("term is greater than current term")
					respChan <- MajorityAppendEntriesResp{
						Term:    resp.Term,
						Success: false,
					}
					return
				}
				if resp.Term == request.Term {
					if resp.Success {
						successAccumulated++
						if successAccumulated >= majority {
							respChan <- MajorityAppendEntriesResp{
								Term:    request.Term,
								Success: true,
							}
							return
						}
					} else {
						successFailed++
						if successFailed > total-majority {
							sugarLogger.Error("invairant failed, one same term has different leader?")
							panic("this should not happen, the consensus algorithm is not implmented correctly")
						}
					}
				}
				if resp.Term < request.Term {
					sugarLogger.Error("invairant failed, smaller term is not overwritten by larger term")
					panic("this should not happen, the consensus algorithm is not implmented correctly")
				}
			}
		case <-ctx.Done():
			sugarLogger.Info("context canceled")
			return
		}
	}
}
