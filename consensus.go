package main

func (node *Node) voting(req RequestVoteRequest, response RequestVoteResponse) RequestVoteResponse {
	if req.Term > node.CurrentTerm {
		node.VotedFor = req.CandidateID
		node.CurrentTerm = req.Term
		response = RequestVoteResponse{
			Term:        node.CurrentTerm,
			VoteGranted: true,
		}
	} else if req.Term < node.CurrentTerm {
		response = RequestVoteResponse{
			Term:        node.CurrentTerm,
			VoteGranted: false,
		}
	} else {
		if node.VotedFor == -1 || node.VotedFor == req.CandidateID {
			node.VotedFor = req.CandidateID
			response = RequestVoteResponse{
				Term:        node.CurrentTerm,
				VoteGranted: true,
			}
		} else {
			response = RequestVoteResponse{
				Term:        node.CurrentTerm,
				VoteGranted: false,
			}
		}
	}
	return response
}
