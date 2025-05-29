package node

import (
	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
)

// shared by leader/follower/candidate

// TODO: THE WHOLE MODULE SHALL BE REFACTORED TO BE AN INTEGRAL OF THE CONSENSUS ALGORITHM
// The decision of consensus upon receiving a request
// can be independent of the current state of the node

// maki: jthis method should be a part of the consensus algorithm
// todo: right now this method doesn't check the current state of the node
// todo: checks the handleVoteRequest works correctly for any state of the node, candidate or leader or follower
// todo: not sure what state shall be changed inside or outside in the caller
func (node *Node) handleVoteRequest(req *rpc.RequestVoteRequest) *rpc.RequestVoteResponse {

	var response rpc.RequestVoteResponse
	currentTerm, voteFor := node.getCurrentTermAndVoteFor()

	if req.Term > currentTerm {
		err := node.storeCurrentTermAndVotedFor(req.Term, req.CandidateId) // did vote for the candidate
		if err != nil {
			node.logger.Error(
				"error in storeCurrentTermAndVotedFor", zap.Error(err),
				zap.String("nId", node.NodeId),
				zap.Uint32("term", req.Term), zap.String("candidateId", req.CandidateId))
			panic(err) // critical error, cannot continue
		}
		response = rpc.RequestVoteResponse{
			Term:        req.Term,
			VoteGranted: true,
		}
	} else if req.Term < currentTerm {
		response = rpc.RequestVoteResponse{
			Term:        currentTerm,
			VoteGranted: false,
		}
	} else {
		if voteFor == "" {
			node.logger.Error("shouldn't happen, but voteFor is empty")
		}
		if voteFor == req.CandidateId {
			response = rpc.RequestVoteResponse{
				Term:        currentTerm,
				VoteGranted: true,
			}
		} else {
			response = rpc.RequestVoteResponse{
				Term:        currentTerm,
				VoteGranted: false,
			}
		}
	}
	return &response
}

// maki: jthis method should be a part of the consensus algorithm
// todo: right now this method doesn't check the current state of the node
// todo: not sure what state shall be changed inside or outside in the caller
func (n *Node) handlerAppendEntries(req *rpc.AppendEntriesRequest) *rpc.AppendEntriesResponse {
	var response rpc.AppendEntriesResponse
	reqTerm := uint32(req.Term)
	currentTerm := n.getCurrentTerm()

	if reqTerm > currentTerm {
		err := n.storeCurrentTermAndVotedFor(reqTerm, "") // did not vote for anyone
		if err != nil {
			n.logger.Error(
				"error in storeCurrentTermAndVotedFor", zap.Error(err),
				zap.String("nId", n.NodeId))
			panic(err) // critical error, cannot continue
		}
		response = rpc.AppendEntriesResponse{
			Term:    currentTerm,
			Success: true,
		}
	} else if reqTerm < currentTerm {
		response = rpc.AppendEntriesResponse{
			Term:    currentTerm,
			Success: false,
		}
	} else {
		// should accecpet it directly?
		response = rpc.AppendEntriesResponse{
			Term:    currentTerm,
			Success: true,
		}
	}
	return &response
}
