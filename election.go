package main

import (
	"context"
	"time"
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

		resChan := make(chan MajorityRequestVoteResp)
		ctxTimeout, cancel := context.WithTimeout(
			ctx, time.Duration(REUQEST_TIMEOUT_IN_MS)*time.Millisecond)
		go RequestVoteSend(ctxTimeout, req, resChan)

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
		case response := <-resChan:
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

// (1) the raft leader sends heartbeats to all other nodes
// (2) the raft heartbeats are triggered by timeouts and by clients
// (3) the raft leader waits for response from all other nodes
// but when the raft leader waits for response of appendEntries, should it send it again?
// to simplify the leader for now, we start sending and wait for a shorter timeout then the 2 timeouts get coupled
func (node *Node) RunAsLeader(ctx context.Context) {

	if node.State != StateLeader {
		panic("node is not in LEADER state")
	}

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
		timerForHeartbeat := time.NewTimer(time.Duration(LEADER_HEARTBEAT_PERIOD_IN_MS) * time.Millisecond)
		select {
		case newTerm := <-recedeChan:
			// paper: if a leader receives a heartbeat from a node with a higher term,
			// it becomes a follower
			node.State = StateFollower
			node.CurrentTerm = newTerm
			timerForHeartbeat.Stop()
			go node.RunAsFollower(ctx)
		case <-timerForHeartbeat.C:
			heartbeatReq := AppendEntriesRequest{
				Term:     node.CurrentTerm,
				LeaderID: node.NodeID,
			}
			go AppendEntriesSend(ctx, heartbeatReq, resChan, errChan)
		case req := <-node.clientRequestChannel:
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

// gracefully stop the node and cleanup
func (node *Node) Stop() {
	close(node.appendEntryChannel)
	close(node.clientRequestChannel)
}
