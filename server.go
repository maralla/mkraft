package main

import (
	"context"
	"time"

	"github.com/maki3cat/mkraft/rpc"
	"github.com/maki3cat/mkraft/util"
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

// the data structure to req/respond to the raft-server
type RequestVoteInternal struct {
	request rpc.RequestVoteRequest
	resChan chan rpc.RequestVoteResponse
}

type AppendEntriesInternal struct {
	request rpc.AppendEntriesRequest
	resChan chan rpc.AppendEntriesResponse
}

type ClientCommandInternal struct {
	request []byte
	resChan chan []byte
}

type Node struct {
	sem *semaphore.Weighted

	LeaderID int
	NodeID   int // maki: nodeID uuid or number or something else?
	State    NodeState

	clientCommandChan chan ClientCommandInternal
	requestVoteChan   chan RequestVoteInternal
	appendEntryChan   chan AppendEntriesInternal

	// Persistent state on all servers
	// todo: how/why to make it persistent? (embedded db?)
	CurrentTerm int
	VotedFor    int // candidateID
	// LogEntries

	// Volatile state on all servers
	// todo: in the logging part
	commitIndex int
	lastApplied int

	// Volatile state on leaders only
	// todo: in the logging part
	nextIndex  []int
	matchIndex []int
}

func (node *Node) VoteRequest(req RequestVoteInternal) {
	node.requestVoteChan <- req
}

func (node *Node) AppendEntryRequest(req AppendEntriesInternal) {
	node.appendEntryChan <- req
}

func NewNode(nodeID int) *Node {
	return &Node{
		State:             StateFollower, // servers start up as followers
		NodeID:            nodeID,
		sem:               semaphore.NewWeighted(1),
		CurrentTerm:       0,
		VotedFor:          -1,
		clientCommandChan: make(chan ClientCommandInternal),
		requestVoteChan:   make(chan RequestVoteInternal),
		appendEntryChan:   make(chan AppendEntriesInternal),
		LeaderID:          -1,
	}
}

// servers start up as followers
func (node *Node) Start(ctx context.Context) {
	go node.RunAsFollower(ctx)
}

// gracefully stop the node and cleanup
func (node *Node) Stop() {
	close(node.appendEntryChan)
	close(node.clientCommandChan)
}

/*
PAPER (quote):
Shared Rule: if any RPC request or response is received from a server with a higher term,
convert to follower
How the Shared Rule works for Followers:
(2) AppendEntries RPC from a server with a higher term
(3) requestVote RPC from a server with a higher term

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

	electionTicker := time.NewTicker(util.GetRandomElectionTimeout())
	defer electionTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			sugarLogger.Warn("context done")
			return

		case <-electionTicker.C:
			node.State = StateCandidate
			go node.RunAsCandidate(ctx)
			return

		case requestVoteInternal := <-node.requestVoteChan:
			electionTicker.Stop()

			// for the follower, the node state has no reason to change because of the request
			req := requestVoteInternal.request
			response := node.voting(req)
			requestVoteInternal.resChan <- response

			electionTicker.Reset(util.GetRandomElectionTimeout())

		case appendEntriesInternal := <-node.appendEntryChan:
			electionTicker.Stop()

			// for the follower, the node state has no reason to change because of the request
			req := appendEntriesInternal.request
			resChan := appendEntriesInternal.resChan
			resChan <- node.appendEntries(req)

			electionTicker.Reset(util.GetRandomElectionTimeout())
		}
	}
}

/*
PAPER (quote):
Shared Rule: if any RPC request or response is received from a server with a higher term,
convert to follower
How the Shared Rule works for Candidates:
(1) response of RequestVote RPC from a server with a higher term
(2) AppendEntries RPC from a server with a higher term
(3) requestVote RPC from a server with a higher term

Specifical Rule for Candidates:
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

	var consensusChan chan MajorityRequestVoteResp
	var voteCancel context.CancelFunc
	defer voteCancel()
	var ctxTimeout context.Context

	// leverage closure
	tryElection := func() {
		consensusChan = make(chan MajorityRequestVoteResp)
		node.CurrentTerm++
		node.VotedFor = node.NodeID
		req := rpc.RequestVoteRequest{
			Term:        node.CurrentTerm,
			CandidateID: node.NodeID,
		}
		ctxTimeout, voteCancel = context.WithTimeout(
			ctx, time.Duration(util.GetRandomElectionTimeout())*time.Millisecond)
		go RequestVoteSend(ctxTimeout, req, consensusChan)
	}

	// the first trial of election
	tryElection()

	ticker := time.NewTicker(util.GetRandomElectionTimeout())
	for {
		select {
		case response := <-consensusChan: // some response from last election
			// I don't think we need to reset the ticker here
			voteCancel() // cancel the rest
			if response.VoteGranted {
				node.State = StateLeader
				go node.RunAsLeader(ctx)
				return
			} else {
				if response.Term > node.CurrentTerm {
					// some one has become a leader
					voteCancel()
					node.CurrentTerm = response.Term
					node.VotedFor = -1
					node.State = StateFollower
					go node.RunAsFollower(ctx)
					return
				} else {
					sugarLogger.Infof(
						"not enough votes, re-elect again, current term: %d, candidateID: %d",
						node.CurrentTerm, node.NodeID,
					)
				}
			}
		case <-ticker.C: // last election timeout withno response
			voteCancel()
			tryElection()
		case requestVote := <-node.requestVoteChan: // from another candidate
			req := requestVote.request
			resChan := requestVote.resChan
			// from another candidate
			if req.Term > node.CurrentTerm {
				voteCancel()
				node.State = StateFollower
				node.CurrentTerm = req.Term
				node.VotedFor = req.CandidateID
				resChan <- rpc.RequestVoteResponse{
					Term:        node.CurrentTerm,
					VoteGranted: true,
				}
				go node.RunAsFollower(ctx)
				return
			} else {
				resChan <- rpc.RequestVoteResponse{
					Term:        node.CurrentTerm,
					VoteGranted: false,
				}
			}
		case request := <-node.appendEntryChan: // from a leader which can be stale or new
			req := request.request
			resChan := request.resChan
			voteCancel() // cancel previous trial of election if not finished
			if req.Term >= node.CurrentTerm {
				// some one has become a leader
				voteCancel()
				node.State = StateFollower
				node.CurrentTerm = req.Term
				resChan <- rpc.AppendEntriesResponse{
					Term:    node.CurrentTerm,
					Success: true,
				}
				go node.RunAsFollower(ctx)
				return
			} else {
				// some old leader comes back to life
				resChan <- rpc.AppendEntriesResponse{
					Term:    node.CurrentTerm,
					Success: false,
				}
			}
		}
	}
}

/*
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
		timerForHeartbeat := time.NewTimer(time.Duration(util.LEADER_HEARTBEAT_PERIOD_IN_MS) * time.Millisecond)
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

// maki: jthis method should be a part of the consensus algorithm
// todo: right now this method doesn't check the current state of the node
func (node *Node) voting(req rpc.RequestVoteRequest) rpc.RequestVoteResponse {
	var response rpc.RequestVoteResponse
	if req.Term > node.CurrentTerm {
		node.VotedFor = req.CandidateID
		node.CurrentTerm = req.Term
		response = rpc.RequestVoteResponse{
			Term:        node.CurrentTerm,
			VoteGranted: true,
		}
	} else if req.Term < node.CurrentTerm {
		response = rpc.RequestVoteResponse{
			Term:        node.CurrentTerm,
			VoteGranted: false,
		}
	} else {
		if node.VotedFor == -1 || node.VotedFor == req.CandidateID {
			node.VotedFor = req.CandidateID
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
	return response
}

// maki: jthis method should be a part of the consensus algorithm
// todo: right now this method doesn't check the current state of the node
func (node *Node) appendEntries(req rpc.AppendEntriesRequest) rpc.AppendEntriesResponse {
	var response rpc.AppendEntriesResponse
	if req.Term > node.CurrentTerm {
		node.CurrentTerm = req.Term
		node.VotedFor = req.LeaderID
		node.State = StateFollower
		// todo: tell the leader/candidate to change the state to follower
		response = rpc.AppendEntriesResponse{
			Term:    node.CurrentTerm,
			Success: true,
		}
	} else if req.Term < node.CurrentTerm {
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
	return response
}
