package main

func RPCServerSendRequestVote(request RequestVoteRequest) RequestVoteResponse {
	voteGranted, term := RequestVoteRecv(request.Term, request.CandidateID)
	return RequestVoteResponse{
		Term:        term,
		VoteGranted: voteGranted,
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
