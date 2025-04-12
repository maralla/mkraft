package main

import (
	"context"

	"github.com/maki3cat/mkraft/rpc"
	"github.com/maki3cat/mkraft/util"
)

var sugarLogger = util.GetSugarLogger()

type MajorityAppendEntriesResp struct {
	Term            int32
	Success         bool
	SingleResponses []rpc.AppendEntriesResponse
	OriginalRequest rpc.AppendEntriesRequest
	Error           error
}

type MajorityRequestVoteResp struct {
	Term            int32
	VoteGranted     bool
	SingleResponses []rpc.RequestVoteResponse
	OriginalRequest rpc.RequestVoteRequest
	Error           error
}

// CONSENSUS MODULE
// todo: currently the result channel only retruns when there is win/fail for sure
func RequestVoteSendForConsensus(ctx context.Context, request *rpc.RequestVoteRequest, resultChannel chan *MajorityRequestVoteResp) {

	members := GetPeersInMembership()
	resChan := make(chan rpc.RPCResWrapper[*rpc.RequestVoteResponse], len(members)) // buffered with len(members) to prevent goroutine leak
	for _, member := range members {
		// FAN-OUT
		// maki: todo topic for go gynastics
		go func() {
			memberHandle := member
			ctxWithTimeout, cancel := context.WithTimeout(ctx, util.GetConfig().GetElectionTimeout())
			defer cancel()
			// FAN-IN
			resChan <- <-memberHandle.SendRequestVote(ctxWithTimeout, request)
		}()
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
					resultChannel <- &MajorityRequestVoteResp{
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
							resultChannel <- &MajorityRequestVoteResp{
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

func AppendEntriesSendForConsensus(
	ctx context.Context, request *rpc.AppendEntriesRequest, respChan chan *MajorityAppendEntriesResp) {

	members := GetPeersInMembership()
	allRespChan := make(chan rpc.RPCResWrapper[*rpc.AppendEntriesResponse], len(members))
	for _, member := range members {
		memberHandle := member
		// FAN-OUT
		go func() {
			ctxWithTimeout, cancel := context.WithTimeout(ctx, util.GetConfig().GetElectionTimeout())
			defer cancel()
			// FAN-IN
			allRespChan <- memberHandle.SendAppendEntries(ctxWithTimeout, request)
		}()
	}

	// STOPPING SHORT
	total := len(members)
	responseNeeded := len(members)/2 + 1 - 1 // -1 because the leader doesn't need to send to itself
	successAccumulated := 0
	failAccumulated := 0

	for range members {
		select {
		case res := <-allRespChan:
			if err := res.Err; err != nil {
				sugarLogger.Error("error in sending append entries to one node", err)
				continue
			} else {
				resp := res.Resp
				if resp.Term > request.Term {
					sugarLogger.Info("term is greater than current term")
					respChan <- &MajorityAppendEntriesResp{
						Term:    resp.Term,
						Success: false,
					}
					return
				}
				if resp.Term == request.Term {
					if resp.Success {
						successAccumulated++
						if successAccumulated >= responseNeeded {
							respChan <- &MajorityAppendEntriesResp{
								Term:    request.Term,
								Success: true,
							}
							return
						}
					} else {
						failAccumulated++
						if failAccumulated > total-responseNeeded {
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

// TODO: THE WHOLE MODULE SHALL BE REFACTORED TO BE AN INTEGRAL OF THE CONSENSUS ALGORITHM
// The decision of consensus upon receiving a request
// can be independent of the current state of the node

// maki: jthis method should be a part of the consensus algorithm
// todo: right now this method doesn't check the current state of the node
// todo: checks the voting works correctly for any state of the node, candidate or leader or follower
// todo: not sure what state shall be changed inside or outside in the caller
func (node *Node) voting(req *rpc.RequestVoteRequest) *rpc.RequestVoteResponse {
	var response rpc.RequestVoteResponse
	if req.Term > node.CurrentTerm {
		node.VotedFor = req.CandidateId
		node.CurrentTerm = req.Term
		response = rpc.RequestVoteResponse{
			Term:        node.CurrentTerm,
			VoteGranted: true,
		}
	} else if req.Term < node.CurrentTerm {
		response = rpc.RequestVoteResponse{
			Term:        node.CurrentTerm,
			VoteGranted: false,
		}
	} else {
		if node.VotedFor == "" || node.VotedFor == req.CandidateId {
			node.VotedFor = req.CandidateId
			response = rpc.RequestVoteResponse{
				Term:        node.CurrentTerm,
				VoteGranted: true,
			}
		} else {
			response = rpc.RequestVoteResponse{
				Term:        node.CurrentTerm,
				VoteGranted: false,
			}
		}
	}
	return &response
}

// maki: jthis method should be a part of the consensus algorithm
// todo: right now this method doesn't check the current state of the node
// todo: not sure what state shall be changed inside or outside in the caller
func (node *Node) appendEntries(req *rpc.AppendEntriesRequest) *rpc.AppendEntriesResponse {
	var response rpc.AppendEntriesResponse
	reqTerm := int32(req.Term)
	if reqTerm > node.CurrentTerm {
		// todo: tell the leader/candidate to change the state to follower
		response = rpc.AppendEntriesResponse{
			Term:    node.CurrentTerm,
			Success: true,
		}
	} else if reqTerm < node.CurrentTerm {
		response = rpc.AppendEntriesResponse{
			Term:    node.CurrentTerm,
			Success: false,
		}
	} else {
		// should accecpet it directly?
		response = rpc.AppendEntriesResponse{
			Term:    node.CurrentTerm,
			Success: true,
		}
	}
	return &response
}
