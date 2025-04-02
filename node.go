package main

import (
	"context"
	"math/rand"
	"time"
)

var nodeInstance *Node

func init() {
	// todo: for now, we just create a node with a fixed ID
	nodeInstance = NewNode(1)
	nodeInstance.Start(context.Background())
}

// maki: I don't know how to test it actually
const LEADER_HEARTBEAT_PERIOD_IN_MS = 100
const ELECTION_TIMEOUT_MIN_IN_MS = 150
const ELECTION_TIMEOUT_MAX_IN_MS = 350
const REUQEST_TIMEOUT_IN_MS = 200

func getRandomElectionTimeout() time.Duration {
	diff := ELECTION_TIMEOUT_MAX_IN_MS - ELECTION_TIMEOUT_MIN_IN_MS
	randomMs := rand.Intn(diff) + ELECTION_TIMEOUT_MIN_IN_MS
	return time.Duration(randomMs) * time.Millisecond
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
	LeaderID    int
	NodeID      int // maki: nodeID uuid or number or something else?
	CurrentTerm int
	State       NodeState

	VotedFor    int // candidateID
	VoteGranted bool

	// todo: does candidate wait for heartbeat?
	appendEntryChannel   chan AppendEntriesRequest // for follower: can come from leader's append rpc or candidate's vote rpc
	clientRequestChannel chan string               // for leader: shall add batching
}

func NewNode(nodeID int) *Node {
	return &Node{
		State:                StateFollower, // servers start up as followers
		appendEntryChannel:   make(chan AppendEntriesRequest),
		clientRequestChannel: make(chan string),
		NodeID:               nodeID,
	}
}

// servers start up as followers
// TODO: is should there be a daemon pattern so that the main thread doesn't exit
func (node *Node) Start(ctx context.Context) {
	go node.RunAsFollower(ctx)
}

// maki:I think here has a potential problem, because
// if the node is busy with accumulated heartbeats of previous time, it will
// lost track of the election timeout
func (node *Node) RunAsFollower(ctx context.Context) {
	if node.State != StateFollower {
		panic("node is not in FOLLOWER state")
	}
	for {
		timerForElection := time.NewTimer(getRandomElectionTimeout())
		select {
		case <-node.appendEntryChannel:
			// paper: a server remains as a follower as long as it receives heartbeats
			// from a leader or candidate
			timerForElection.Stop()
		case <-timerForElection.C:
			// paper:
			// election timeout, assumes no leader exists and starts a new election
			// increment term, vote for self, send request to all other nodes
			node.State = StateCandidate
			go node.RunAsCandidate(ctx)
			return
		}
	}
}

// maki: key question, the timeout and cancel of context
func (node *Node) RunAsCandidate(ctx context.Context) {
	if node.State != StateCandidate {
		panic("node is not in CANDIDATE state")
	}
	// start for election
	// todo: add error handling
	// send request to all other nodes
	for {
		node.CurrentTerm++
		node.VotedFor = node.NodeID
		node.VoteGranted = false
		req := RequestVoteRequest{
			Term:        node.CurrentTerm,
			CandidateID: node.NodeID,
		}
		// todo: how to add timeout
		// ctx = ctx.WithTimeout(ctx, time.Duration(REUQEST_TIMEOUT_IN_MS)*time.Millisecond)
		// should start with future?
		responseChannel := make(chan RequestVoteResponse)
		ctxTimeout, cancel := context.WithTimeout(
			ctx, time.Duration(REUQEST_TIMEOUT_IN_MS)*time.Millisecond)
		go ClientSendRequestVoteToAll(ctxTimeout, req, responseChannel)

		timer := time.NewTimer(getRandomElectionTimeout())
		select {
		case request := <-node.appendEntryChannel:
			if request.Term >= node.CurrentTerm {
				node.State = StateFollower
				node.CurrentTerm = request.Term
				timer.Stop()
				cancel()
				go node.RunAsFollower(ctx)
			} else {
				// Should be rejected directly by the server handler
				// Shouldn't reach here
				cancel()
				panic("node received a heartbeat from a node with a lower term")
			}
		case response := <-responseChannel:
			cancel()
			if response.Term > node.CurrentTerm {
				// paper: if a candidate receives a request vote from a higher term,
				// it becomes a follower
				node.State = StateFollower
				node.CurrentTerm = response.Term
				timer.Stop()
				go node.RunAsFollower(ctx)
				return
			} else if response.VoteGranted {
				node.VoteGranted = true
				timer.Stop()
				node.State = StateLeader
				go node.RunAsLeader(ctx)
				return
			} else {
				// could be term = current term and vote granted = false
				// start another round of election
				continue
			}
		case <-ctxTimeout.Done():
			// timeout, start another round of election
			cancel()
		}
	}
}

func (node *Node) RunAsLeader(ctx context.Context) {
	if node.State != StateLeader {
		panic("node is not in LEADER state")
	}
	for {
		// maki: here we need to send heartbeats to all the followers
		timerForHeartbeat := time.NewTimer(time.Duration(LEADER_HEARTBEAT_PERIOD_IN_MS) * time.Millisecond)
		select {
		// todo: here we need to wait for their response synchronously or continue for the next batch?
		case request := <-node.clientRequestChannel:
			timerForHeartbeat.Stop()
			// todo: to be implemented
			_ = ClientSendAppendEntriesToAll(ctx, AppendEntriesRequest{})
			appendLog(request)
		case <-timerForHeartbeat.C:
			// leaders send periodic heartbeats to all the followers to maintain
			// their authority
			// todo: to be implemented
			ClientSendAppendEntriesToAll(ctx, AppendEntriesRequest{
				Term:     node.CurrentTerm,
				LeaderID: node.NodeID,
			})
		}
	}
}

// gracefully stop the node
func (node *Node) Stop() {
	close(node.appendEntryChannel)
	close(node.clientRequestChannel)
}
