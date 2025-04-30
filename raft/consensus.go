package raft

import (
	"context"
	"errors"

	"github.com/maki3cat/mkraft/rpc"
	"github.com/maki3cat/mkraft/util"
)

var sugarLogger = util.GetSugarLogger()

type MajorityAppendEntriesResp struct {
	Term            int32
	Success         bool
	SingleResponses []rpc.AppendEntriesResponse
	OriginalRequest rpc.AppendEntriesRequest
}

type MajorityRequestVoteResp struct {
	Term            int32
	VoteGranted     bool
	SingleResponses []rpc.RequestVoteResponse
	OriginalRequest rpc.RequestVoteRequest
	Error           error
}

// CONSENSUS MODULE
// the response returns error when the majority of peers failed
func RequestVoteSendForConsensus(ctx context.Context, request *rpc.RequestVoteRequest, resultChannel chan *MajorityRequestVoteResp) {
	logger := util.GetSugarLogger()

	total := memberMgr.GetMemberCount()
	majority := total/2 + 1
	logger.Debugw("RequestVoteSendForConsensus", "request", request, "total", total, "majority", majority)

	memberClients, err := memberMgr.GetAllPeerClients()
	if err != nil {
		sugarLogger.Error("error in getting all peer clients", err)
		resultChannel <- &MajorityRequestVoteResp{
			Error: err,
		}
		return
	}
	if len(memberClients)+1 < majority {
		sugarLogger.Error("no member clients found")
		resultChannel <- &MajorityRequestVoteResp{
			Error: errors.New("no member clients found"),
		}
		return
	}

	peersCount := len(memberClients)
	resChan := make(chan rpc.RPCRespWrapper[*rpc.RequestVoteResponse], peersCount) // buffered with len(members) to prevent goroutine leak
	for _, member := range memberClients {
		// FAN-OUT
		// maki: todo topic for go gynastics
		go func() {
			memberHandle := member
			logger.Debugw("fan out to request vote", "member", memberHandle)
			timeout := util.GetConfig().GetElectionTimeout()
			util.GetSugarLogger().Debugw("send request vote", "timeout", timeout)
			ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			// FAN-IN
			resChan <- <-memberHandle.SendRequestVote(ctxWithTimeout, request)
		}()
	}

	// FAN-IN WITH STOPPING SHORT
	sugarLogger.Debugw("current setup of membership", "majority", majority, "total", total, "peersCount", peersCount)
	voteAccumulated := 1 // the node itself is counted as a vote
	voteFailed := 0
	for range peersCount {
		select {
		case res := <-resChan:
			if err := res.Err; err != nil {
				voteFailed++
				sugarLogger.Errorf("error in sending request vote to one node: %v", err)
				remains := peersCount - voteFailed - voteAccumulated
				needs := majority - voteAccumulated
				if remains < needs {
					resultChannel <- &MajorityRequestVoteResp{
						VoteGranted: false,
						Error:       errors.New("majority of nodes failed to respond"),
					}
					return
				} else {
					continue
				}
			} else {
				resp := res.Resp
				// if someone responds with a term greater than the current term
				if resp.Term > request.Term {
					sugarLogger.Info("peer's term is greater than the node's current term")
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
						voteFailed++
						if calculateIfAlreadyFail(total, peersCount, voteAccumulated, voteFailed) {
							resultChannel <- &MajorityRequestVoteResp{
								VoteGranted: false,
								Error:       errors.New("majority of nodes failed to respond"),
							}
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

func calculateIfAlreadyFail(total, peersCount, voteAccumulated, voteFailed int) bool {
	majority := total/2 + 1
	majorityNeeded := majority - 1
	needed := majorityNeeded - voteAccumulated
	possibleRespondant := peersCount - voteFailed - voteAccumulated
	return possibleRespondant < needed
}

/*
* send append entries to all peers
* and wait for the majority of them to respond
 */
func AppendEntriesSendForConsensus(
	ctx context.Context, request *rpc.AppendEntriesRequest) (*MajorityAppendEntriesResp, error) {

	logger := util.GetSugarLogger()
	total := memberMgr.GetMemberCount()
	majority := total/2 + 1
	logger.Debugw("AppendEntriesSendForConsensus", "request", request, "total", total, "majority", majority)

	peerClients, err := memberMgr.GetAllPeerClients()
	if err != nil {
		sugarLogger.Error("error in getting all peer clients", err)
		return nil, err
	}
	if len(peerClients)+1 < majority {
		sugarLogger.Error("not enough peer clients found")
		return nil, errors.New("not enough peer clients found")
	}

	allRespChan := make(chan rpc.RPCRespWrapper[*rpc.AppendEntriesResponse], len(peerClients))
	for _, member := range peerClients {
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
	successAccumulated := 0
	failAccumulated := 0
	sugarLogger.Debugw("current setup of membership", "majority", majority, "total", total, "memberCount", memberCount)

	for range memberCount - 1 {
		select {
		case res := <-allRespChan:
			if err := res.Err; err != nil {
				sugarLogger.Error("error in sending append entries to one node", err)
				continue
			} else {
				resp := res.Resp
				if resp.Term > request.Term {
					sugarLogger.Info("term is greater than current term")
					return &MajorityAppendEntriesResp{
						Term:    resp.Term,
						Success: false,
					}
				}
				if resp.Term == request.Term {
					if resp.Success {
						successAccumulated++
						if successAccumulated >= majority {
							return &MajorityAppendEntriesResp{
								Term:    request.Term,
								Success: true,
							}
						}
					} else {
						failAccumulated++
						if failAccumulated > total-majority {
							sugarLogger.Warn("another node with same term becomes the leader")
							return &MajorityAppendEntriesResp{
								Term:    request.Term,
								Success: false,
							}
						}
					}
				}
				if resp.Term < request.Term {
					sugarLogger.Errorw(
						"invairant failed, smaller term is not overwritten by larger term",
						"request", request,
						"response", resp)
					panic("this should not happen, the consensus algorithm is not implmented correctly")
				}
			}
		case <-ctx.Done():
			sugarLogger.Info("context canceled")
			return &MajorityAppendEntriesResp{
				Error: errors.New("context canceled"),
			}
		}
	}
	panic("this should not happen, the consensus algorithm is not implmented correctly")
}

// TODO: THE WHOLE MODULE SHALL BE REFACTORED TO BE AN INTEGRAL OF THE CONSENSUS ALGORITHM
// The decision of consensus upon receiving a request
// can be independent of the current state of the node

// maki: jthis method should be a part of the consensus algorithm
// todo: right now this method doesn't check the current state of the node
// todo: checks the voting works correctly for any state of the node, candidate or leader or follower
// todo: not sure what state shall be changed inside or outside in the caller
func (node *Node) voting(req *rpc.RequestVoteRequest) *rpc.RequestVoteResponse {
	sugarLogger := util.GetSugarLogger()
	sugarLogger.Debugw("consensus module handling voting request", "request", req)
	var response rpc.RequestVoteResponse
	if req.Term > node.CurrentTerm {
		node.VotedFor = req.CandidateId
		node.CurrentTerm = req.Term
		response = rpc.RequestVoteResponse{
			Term:        req.Term,
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
	sugarLogger.Debugw(
		"consensus returns voting response",
		"response.term", response.Term, "response.voteGranted", response.VoteGranted)
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
