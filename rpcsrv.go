package main

// server side
func ServerRequestVoteHandler(request RequestVoteRequest) {

}

func ServerAppendEntriesHandler(request AppendEntriesRequest) AppendEntriesResponse {
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

func ServerCallbackRequestVoteResponse(response RequestVoteResponse) {
}

func ServerCallbackAppendEntriesResponse(response AppendEntriesResponse) {
}
