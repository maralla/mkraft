package main

import (
	"context"
	"time"

	"github.com/maki3cat/mkraft/conf"
	"golang.org/x/sync/semaphore"
)

var nodeInstance *Node

func init() {
	// maki: some design details to be documented
	// todo: for now, we just create a node with a fixed ID
	nodeInstance = NewNode(1)
	nodeInstance.Start(context.Background())
}

type NodeState int

const (
	StateFollower NodeState = iota
	StateCandidate
	StateLeader
)

func (state NodeState) String() string {
	switch state {
	case StateFollower:
		return "Follower"
	case StateCandidate:
		return "Candidate"
	case StateLeader:
		return "Leader"
	}
	// should never reach here
	return "Unknown State"
}

type Node struct {
	sem         semaphore.Weighted
	LeaderID    int
	NodeID      int // maki: nodeID uuid or number or something else?
	CurrentTerm int
	State       NodeState

	VotedFor    int // candidateID
	VoteGranted bool

	appendEntryChan   chan AppendEntriesRequest // for follower: can come from leader's append rpc or candidate's vote rpc
	clientCommandChan chan string               // todo: to be implemented
	// todo: does leader and candidate need to answer this channel
	requestVoteChan chan RequestVoteInternal
}

// todo: this channel doesn't contain error
// todo: the paper doesn't mention it but the voting shalle be handled by all candidates/follower/leader
func (node *Node) VoteRequest(req RequestVoteRequest) chan RequestVoteResponse {
	dto := RequestVoteInternal{
		request: req,
		resChan: make(chan RequestVoteResponse),
	}
	node.requestVoteChan <- dto
	return dto.resChan
}

func (node *Node) AppendEntryRequest(req AppendEntriesRequest) chan AppendEntriesResponse {
	// todo: this is not implemented yet
	return nil
}

func NewNode(nodeID int) *Node {
	return &Node{
		State:             StateFollower, // servers start up as followers
		appendEntryChan:   make(chan AppendEntriesRequest),
		clientCommandChan: make(chan string),
		NodeID:            nodeID,
	}
}

// servers start up as followers
// TODO: is should there be a daemon pattern so that the main thread doesn't exit
func (node *Node) Start(ctx context.Context) {
	go node.RunAsFollower(ctx)
}

// gracefully stop the node and cleanup
func (node *Node) Stop() {
	close(node.appendEntryChan)
	close(node.clientCommandChan)
}

// # todo: should this be replaced with ticker
/*
PAPER quote: RULEs for Servers
Respond to RPCs from candidates and leaders
If election timeout elapses without receiving AppendEntries RPC from current leader
or granting vote to candidate: convert to candidate
*/
func (node *Node) RunAsFollower(ctx context.Context) {
	if node.State != StateFollower {
		panic("node is not in FOLLOWER state")
	}

	sugarLogger.Info("node acquires to run in FOLLOWER state")
	node.sem.Acquire(ctx, 1)
	sugarLogger.Info("acquired semaphore in FOLLOWER state")
	defer node.sem.Release(1)

	for {
		timerForElection := time.NewTimer(conf.GetRandomElectionTimeout())
		select {
		case <-ctx.Done():
			sugarLogger.Info("context done")
			timerForElection.Stop()
			return
		case <-timerForElection.C:
			node.State = StateCandidate
			go node.RunAsCandidate(ctx)
			return
		case requestVoteInternal := <-node.requestVoteChan:
			timerForElection.Stop()
			req := requestVoteInternal.request
			var response RequestVoteResponse
			// todo: grant vote has more complicated logic with log
			response = node.voting(req, response)
			requestVoteInternal.resChan <- response
		case <-node.appendEntryChan:
			timerForElection.Stop()
			// todo: shall handle and return the append entry response
		}
	}
}

/*
PAPER(5.2) quote:
On conversion to a candidate, start election:
(1) increment currentTerm
(2) vote for self
(3) send RequestVote RPCs to all other servers
if votes received from majority of servers: become leader
if AppendEntries RPC received from new leader: convert to follower
if election timeout elapses: start new election
*/
func (node *Node) RunAsCandidate(ctx context.Context) {
	if node.State != StateCandidate {
		panic("node is not in CANDIDATE state")
	}

	sugarLogger.Info("node acquires to run in CANDIDATE state")
	node.sem.Acquire(ctx, 1)
	sugarLogger.Info("acquired semaphore in CANDIDATE state")
	defer node.sem.Release(1)

	changeStateChan := make(chan MajorityRequestVoteResp)
	var voteCancel context.CancelFunc
	var ctxTimeout context.Context

	// the first trial of election
	node.CurrentTerm++
	node.VotedFor = node.NodeID
	node.VoteGranted = false
	req := RequestVoteRequest{
		Term:        node.CurrentTerm,
		CandidateID: node.NodeID,
	}
	ctxTimeout, voteCancel = context.WithTimeout(
		ctx, time.Duration(conf.REUQEST_TIMEOUT_IN_MS)*time.Millisecond)
	go RequestVoteSend(ctxTimeout, req, changeStateChan)

	for {
		timer := time.NewTimer(conf.GetRandomElectionTimeout())
		select {
		case <-timer.C:
			voteCancel()                                          // cancel previous trial of election if not finished
			changeStateChan := make(chan MajorityRequestVoteResp) // reset the channel
			node.CurrentTerm++
			node.VotedFor = node.NodeID
			node.VoteGranted = false
			req := RequestVoteRequest{
				Term:        node.CurrentTerm,
				CandidateID: node.NodeID,
			}
			ctxTimeout, voteCancel = context.WithTimeout(
				ctx, time.Duration(conf.REUQEST_TIMEOUT_IN_MS)*time.Millisecond)
			go RequestVoteSend(ctxTimeout, req, changeStateChan)
		case request := <-node.appendEntryChan:
			voteCancel() // cancel previous trial of election if not finished
			if request.Term >= node.CurrentTerm {
				node.State = StateFollower
				node.CurrentTerm = request.Term
				timer.Stop()
				go node.RunAsFollower(ctx)
				return
			} else {
				// todo: check the error handling strategy is correct or not
				// Should be rejected directly by the server handler
				// Shouldn't reach here
				panic("node received a heartbeat from a node with a lower term")
			}
		case response := <-changeStateChan:
			voteCancel()
			if response.VoteGranted {
				node.VoteGranted = true
				timer.Stop()
				node.State = StateLeader
				go node.RunAsLeader(ctx)
				return
			} else {
				node.State = StateFollower
				node.CurrentTerm = response.Term
				timer.Stop()
				go node.RunAsFollower(ctx)
				return
			}
		}
	}
}

/*
WIP: maki - this is not implemented yet
PAPER quote: RULEs for Servers
(1) Upon election: send initial empty AppendEntries (heartbeat) RPCs to each reserver; repeat during idle periods to prevent election timeouts; (5.2)
(2) If command received from client: append entry to local log, respond after entry applied to state machine; (5.3)
(3) If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex for the follower;
If successful: update nextIndex and matchIndex for follower
If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
(4) If there exists and N such that N > committedIndex, a majority of matchIndex[i] ≥ N, ... (5.3/5.4)
maki: this paper doesn't mention how a stale leader catches up and becomes a follower
todo: check this point
*/
func (node *Node) RunAsLeader(ctx context.Context) {

	if node.State != StateLeader {
		panic("node is not in LEADER state")
	}

	sugarLogger.Info("node acquires to run in LEADER state")
	node.sem.Acquire(ctx, 1)
	sugarLogger.Info("acquired semaphore in LEADER state")
	defer node.sem.Release(1)

	errChan := make(chan error, 1)
	resChan := make(chan MajorityAppendEntriesResp, 1)
	recedeChan := make(chan int)
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				sugarLogger.Info("context done")
			case response := <-resChan:
				if response.Term > node.CurrentTerm {
					recedeChan <- response.Term
				} else if response.Success {
					sugarLogger.Info("majority of append entries response received")
					sugarLogger.Info("handle corresponding client request")
					// todo: handle the client request
				} else {
					sugarLogger.Info("majority of append entries response not received")
					// todo: handle the client request
				}
			case err := <-errChan:
				sugarLogger.Info("error in sending append entries", err)
			}
		}
	}(ctx)

	for {
		timerForHeartbeat := time.NewTimer(time.Duration(conf.LEADER_HEARTBEAT_PERIOD_IN_MS) * time.Millisecond)
		select {
		case newTerm := <-recedeChan:
			// paper: if a leader receives a heartbeat from a node with a higher term,
			// it becomes a follower
			node.State = StateFollower
			node.CurrentTerm = newTerm
			timerForHeartbeat.Stop()
			go node.RunAsFollower(ctx)
			return
		case <-timerForHeartbeat.C:
			heartbeatReq := AppendEntriesRequest{
				Term:     node.CurrentTerm,
				LeaderID: node.NodeID,
			}
			go AppendEntriesSend(ctx, heartbeatReq, resChan, errChan)
		case req := <-node.clientCommandChan:
			timerForHeartbeat.Stop()
			// todo: need to get result if the request is successful
			// possibillty the leader is stale itself
			appendEntryReq := AppendEntriesRequest{
				Term:     node.CurrentTerm,
				LeaderID: node.NodeID,
				Entries: []LogEntry{
					{
						Term:  node.CurrentTerm,
						Index: 0, // todo: dummy log data
						Data:  req,
					},
				},
			}
			go AppendEntriesSend(ctx, appendEntryReq, resChan, errChan)
		}
	}
}

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
