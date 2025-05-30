package node

import (
	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
)

// leader shall reply yet others not
func (node *Node) handleApplyCommand() {

}

// shared by leader/follower/candidate
// this method doesn't check the current state of the node
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
			// temporary solution, should be fixed with a safer implementation later
			panic("shouldn't happen, but voteFor is empty")
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
