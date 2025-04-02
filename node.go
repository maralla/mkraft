package main

import (
	"context"
	"math/rand"
	"time"
)

const LEADER_HEARTBEAT_PERIOD_IN_MS = 100
const ELECTION_TIMEOUT_MIN_IN_MS = 150
const ELECTION_TIMEOUT_MAX_IN_MS = 350

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
	VotedFor    map[int]bool

	// todo: does candidate wait for heartbeat?
	appendEntryChannel   chan AppendEntriesRequest // for follower: can come from leader's append rpc or candidate's vote rpc
	clientRequestChannel chan string               // for leader: shall add batching
	voteChannel          chan bool                 // for candidate: if vote succeeds
}

func NewNode(nodeID int) *Node {
	return &Node{
		State:                StateFollower, // servers start up as followers
		appendEntryChannel:   make(chan AppendEntriesRequest),
		clientRequestChannel: make(chan string),
		voteChannel:          make(chan bool),
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
			node.CurrentTerm++
			node.State = StateCandidate
			node.VotedFor = make(map[int]bool) // maki: here we need to reset the map
			node.VotedFor[node.NodeID] = true

			req := RequestVoteRequest{
				Term:        node.CurrentTerm,
				CandidateID: node.NodeID,
			}
			_ = ClientSendRequestVoteToAll(ctx, req)

			// todo: add error handling
			// send request to all other nodes
			go node.RunAsCandidate(ctx)
			return
		}
	}
}

func (node *Node) RunAsCandidate(ctx context.Context) {
	if node.State != StateCandidate {
		panic("node is not in CANDIDATE state")
	}
	for {
		timer := time.NewTimer(getRandomElectionTimeout())
		select {
		case <-node.voteChannel:
			// todo: havent implemented yet
			// paper: if it wins an election, it becomes a leader
			node.State = StateLeader
			timer.Stop()
			go node.RunAsLeader(ctx)
			return
		case request := <-node.appendEntryChannel:
			if request.Term >= node.CurrentTerm {
				node.State = StateFollower
				node.CurrentTerm = request.Term
				timer.Stop()
				go node.RunAsFollower(ctx)
			} else {
				// Should be rejected directly by the server handler
				// Shouldn't reach here
				panic("node received a heartbeat from a node with a lower term")
			}
		case <-timer.C:
			// paper: no winner, so it starts a new election
			node.CurrentTerm++
			node.VotedFor = make(map[int]bool)
			node.VotedFor[node.NodeID] = true
			request := RequestVoteRequest{
				Term:        node.CurrentTerm,
				CandidateID: node.NodeID,
			}
			_ = ClientSendRequestVoteToAll(ctx, request)
			// todo: add error handling
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
			SendAppendEntriesToAll()

			appendLog(request)
			// todo: consensesus, not sure if not reached
			consensusReached := node.AppendEntries(request, getCurrentCommitID())
			if consensusReached {
				_ = commitLog(request)
			} else {
				resetLog()
			}
		case <-timerForHeartbeat.C:
			// leaders send periodic heartbeats to all the followers to maintain
			// their authority
			node.AppendEntries("", getCurrentCommitID())
		}
	}
}

func (node *Node) ReceiveHeartbeat() {
	node.appendEntryChannel <- true
}

// together with heartbeat
func (node *Node) AppendEntries(data string, lastCommitID int) bool {
	// maki: send heartbeat to all the others
	// maki: how to handle error in this case?
	return true
}

// gracefully stop the node
func (node *Node) Stop() {
	close(node.appendEntryChannel)
	close(node.clientRequestChannel)
}
