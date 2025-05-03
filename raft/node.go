package raft

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maki3cat/mkraft/rpc"
	"github.com/maki3cat/mkraft/util"
	"golang.org/x/sync/semaphore"
)

var nodeInstance *Node
var nodeInitOnce = &sync.Once{}

func StartRaftNode(ctx context.Context) {
	nodeInitOnce.Do(func() {
		nodeInstance = NewNode(memberMgr.GetCurrentNodeID())
		nodeInstance.Start(ctx)
	})
}

func GetRaftNode() *Node {
	if nodeInstance == nil {
		panic("raft node is not initialized")
	}
	return nodeInstance
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

type RequestVoteInternal struct {
	Request    *rpc.RequestVoteRequest
	RespWraper chan *rpc.RPCRespWrapper[*rpc.RequestVoteResponse]
	IsTimeout  atomic.Bool
}

type AppendEntriesInternal struct {
	Request    *rpc.AppendEntriesRequest
	RespWraper chan *rpc.RPCRespWrapper[*rpc.AppendEntriesResponse]
	IsTimeout  atomic.Bool
}

type ClientCommandInternal struct {
	request  []byte
	respChan chan []byte
	errChan  chan error
}

type TermRank int

// the Raft Server Node
// maki: go gymnastics for sync values
// todo: add sync for these values?
type Node struct {
	sem *semaphore.Weighted

	LeaderId string
	NodeId   string // maki: nodeID uuid or number or something else?
	State    NodeState

	// leader only channels
	// gracefully clean every time a leader degrades to a follower
	// reset these 2 data structures everytime a new leader is elected
	clientCommandChan     chan *ClientCommandInternal
	leaderDegradationChan chan TermRank

	// shared by all states
	requestVoteChan chan *RequestVoteInternal
	appendEntryChan chan *AppendEntriesInternal

	// Persistent state on all servers
	// todo: how/why to make it persistent? (embedded db?)
	// todo: change to concurrency safe
	CurrentTerm int32
	VotedFor    string // candidateID
	// LogEntries

	// Volatile state on all servers
	// todo: in the logging part
	// commitIndex int
	// lastApplied int

	// Volatile state on leaders only
	// todo: in the logging part
	// nextIndex  []int
	// matchIndex []int
}

// todo: use this to replace all
func (node *Node) GracefulShutdown(ctx context.Context) {
	logger.Info("raft node starting graceful shutdown")
	memberMgr.GracefulShutdown()
	logger.Info("raft node has just finished graceful shutdown")
}

// maki: go gymnastics for sync values
// todo: add sync for these values?
func (node *Node) SetVoteForAndTerm(voteFor string, term int32) {
	node.VotedFor = voteFor
	node.CurrentTerm = term
}

func (node *Node) ResetVoteFor() {
	node.VotedFor = ""
}

func (node *Node) VoteRequest(req *RequestVoteInternal) {
	node.requestVoteChan <- req
}

func (node *Node) AppendEntryRequest(req *AppendEntriesInternal) {
	node.appendEntryChan <- req
}

func (node *Node) ClientCommandRequest(request []byte) {

}

func NewNode(nodeId string) *Node {
	bufferSize := util.GetConfig().GetRaftNodeRequestBufferSize()
	return &Node{
		State:             StateFollower, // servers start up as followers
		NodeId:            nodeId,
		sem:               semaphore.NewWeighted(1),
		CurrentTerm:       0,
		VotedFor:          "",
		clientCommandChan: make(chan *ClientCommandInternal, bufferSize),
		requestVoteChan:   make(chan *RequestVoteInternal, bufferSize),
		appendEntryChan:   make(chan *AppendEntriesInternal, bufferSize),
		LeaderId:          "",
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

func (node *Node) runOneElection(ctx context.Context) chan *MajorityRequestVoteResp {
	consensusChan := make(chan *MajorityRequestVoteResp, 1)
	node.CurrentTerm++
	node.VotedFor = node.NodeId
	req := &rpc.RequestVoteRequest{
		Term:        node.CurrentTerm,
		CandidateId: node.NodeId,
	}
	ctxTimeout, _ := context.WithTimeout(
		ctx, time.Duration(util.GetConfig().GetElectionTimeout()))
	go func() {
		resp, err := consensus.RequestVoteSendForConsensus(ctxTimeout, req)
		if err != nil {
			logger.Error("error in RequestVoteSendForConsensus", err)
			return
		} else {
			logger.Debugw("received request vote response", "response", resp)
			consensusChan <- resp
		}
	}()
	return consensusChan
}

// TODO: THE WHOLE MODULE SHALL BE REFACTORED TO BE AN INTEGRAL OF THE CONSENSUS ALGORITHM
// The decision of consensus upon receiving a request
// can be independent of the current state of the node

// maki: jthis method should be a part of the consensus algorithm
// todo: right now this method doesn't check the current state of the node
// todo: checks the receiveVoteRequest works correctly for any state of the node, candidate or leader or follower
// todo: not sure what state shall be changed inside or outside in the caller
func (node *Node) receiveVoteRequest(req *rpc.RequestVoteRequest) *rpc.RequestVoteResponse {
	logger := util.GetSugarLogger()
	logger.Debugw("consensus module handling voting request", "request", req)
	var response rpc.RequestVoteResponse
	if req.Term > node.CurrentTerm {
		node.VotedFor = req.CandidateId
		node.CurrentTerm = req.Term
		response = rpc.RequestVoteResponse{
			Term:        req.Term,
			VoteGranted: true,
		}
	} else if req.Term < node.CurrentTerm {
		response = rpc.RequestVoteResponse{
			Term:        node.CurrentTerm,
			VoteGranted: false,
		}
	} else {
		if node.VotedFor == "" || node.VotedFor == req.CandidateId {
			node.VotedFor = req.CandidateId
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
	logger.Debugw(
		"consensus returns voting response",
		"response.term", response.Term, "response.voteGranted", response.VoteGranted)
	return &response
}

// maki: jthis method should be a part of the consensus algorithm
// todo: right now this method doesn't check the current state of the node
// todo: not sure what state shall be changed inside or outside in the caller
func (node *Node) receiveAppendEntires(req *rpc.AppendEntriesRequest) *rpc.AppendEntriesResponse {
	var response rpc.AppendEntriesResponse
	reqTerm := int32(req.Term)
	if reqTerm > node.CurrentTerm {
		// todo: tell the leader/candidate to change the state to follower
		response = rpc.AppendEntriesResponse{
			Term:    node.CurrentTerm,
			Success: true,
		}
	} else if reqTerm < node.CurrentTerm {
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
	return &response
}
