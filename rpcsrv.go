package main

func RPCServerSendRequestVote(request RequestVoteRequest) RequestVoteResponse {
	res := <-nodeInstance.VoteRequest(request)
	return res
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
		nodeInstance.appendEntryChan <- request
		response := AppendEntriesResponse{
			Term:    request.Term,
			Success: true,
		}
		return response
	}
}
