package main

// server side
func RPCServerSendRequestVote(request RequestVoteRequest) RequestVoteResponse {
	if request.Term < nodeInstance.CurrentTerm {
		return RequestVoteResponse{
			Term:        nodeInstance.CurrentTerm,
			VoteGranted: false,
		}
	} else if request.Term > nodeInstance.CurrentTerm {
		// the current node can be a candidate/leader/follower
		// TODO: should make the current node a follower?
		nodeInstance.VotedFor = request.CandidateID
		// TODO: not sure if we need to update the current term right now
		// if we don't update the term now, the next branch is impacted
		nodeInstance.CurrentTerm = request.Term

		return RequestVoteResponse{
			Term:        nodeInstance.CurrentTerm,
			VoteGranted: true,
		}
	} else {
		// the term is equal
		// TODO: it seems, we should not vote for a equal term?
		// but the paper doesn't say anything about it
		return RequestVoteResponse{
			Term:        nodeInstance.CurrentTerm,
			VoteGranted: false,
		}
	}
}

func RPCServerAppendEntries(request AppendEntriesRequest) AppendEntriesResponse {
	if request.Term <= nodeInstance.CurrentTerm {
		response := AppendEntriesResponse{
			Term:    nodeInstance.CurrentTerm,
			Success: false,
		}
		return response
	} else {
		// todo: check if the log is consistent?
		nodeInstance.appendEntryChannel <- request
		response := AppendEntriesResponse{
			Term:    request.Term,
			Success: true,
		}
		return response
	}
}
