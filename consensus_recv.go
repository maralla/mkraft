package main

func RequestVoteRecv(term int, candidateID int) (bool, int) {
	if term < nodeInstance.CurrentTerm {
		return false, nodeInstance.CurrentTerm
	}
	// todo: other conditions of log
	// todo: should the term be updated here?
	// paper doesn't say anything about it
	if nodeInstance.VotedFor == -1 || nodeInstance.VotedFor == candidateID {
		nodeInstance.VotedFor = candidateID
		nodeInstance.CurrentTerm = term
		// maki:very important
		// todo: should change the running state of the server??
		// if the server right now is a stale leader?
		// if the server right now a candidate?
		return true, term
	}
	return false, term
}
