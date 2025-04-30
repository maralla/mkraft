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
	request []byte
	// resChan chan []byte
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

	// only leader handles this channel
	// todo: others reject this directly
	clientCommandChan chan *ClientCommandInternal

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
	bufferSize := util.GetConfig().GetRaftNodeRequestBuffer()
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

// todo, maki: shall check if we need priority of a speicifc channel in our select
/*
PAPER (quote):
Shared Rule: if any RPC request or response is received from a server with a higher term,
convert to follower
How the Shared Rule works for Followers:
(1) handle request of AppendEntriesRPC initiated by another server
(2) handle reuqest of RequestVoteRPC initiated by another server

Speicial Rules for Followers
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

	electionTicker := time.NewTicker(util.GetConfig().GetElectionTimeout())
	defer electionTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			sugarLogger.Warn("context done")
			memberMgr.GracefulShutdown()
			return
		case <-electionTicker.C:
			node.State = StateCandidate
			go node.RunAsCandidate(ctx)
			return
		case requestVoteInternal := <-node.requestVoteChan:
			if requestVoteInternal.IsTimeout.Load() {
				sugarLogger.Warn("request vote is timeout")
				continue
			}
			electionTicker.Stop()
			// for the follower, the node state has no reason to change because of the request
			resp := node.voting(requestVoteInternal.Request)
			wrapper := &rpc.RPCRespWrapper[*rpc.RequestVoteResponse]{
				Resp: resp,
				Err:  nil,
			}
			requestVoteInternal.RespWraper <- wrapper
			electionTicker.Reset(util.GetConfig().GetElectionTimeout())
		case req := <-node.appendEntryChan:
			electionTicker.Stop()
			// for the follower, the node state has no reason to change because of the request
			resp := node.appendEntries(req.Request)
			wrappedResp := &rpc.RPCRespWrapper[*rpc.AppendEntriesResponse]{
				Resp: resp,
				Err:  nil,
			}
			req.RespWraper <- wrappedResp
			electionTicker.Reset(util.GetConfig().GetElectionTimeout())
		}
	}
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
	go RequestVoteSendForConsensus(ctxTimeout, req, consensusChan)
	return consensusChan
}

/*
PAPER (quote):
Shared Rule: if any RPC request or response is received from a server with a higher term,
convert to follower
How the Shared Rule works for Candidates:
(1) handle the response of RequestVoteRPC initiated by itself
(2) handle request of AppendEntriesRPC initiated by another server
(3) handle reuqest of RequestVoteRPC initiated by another server

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

	sugarLogger.Info("node starts to acquiring CANDIDATE state")
	node.sem.Acquire(ctx, 1)
	sugarLogger.Info("node has acquired semaphore in CANDIDATE state")
	defer node.sem.Release(1)

	consensusChan := node.runOneElection(ctx)

	ticker := time.NewTicker(util.GetConfig().GetElectionTimeout())
	for {
		select {
		case <-ctx.Done():
			sugarLogger.Warn("raft node main context done")
			memberMgr.GracefulShutdown()
			return
		case response := <-consensusChan: // some response from last election
			// I don't think we need to reset the ticker here
			// voteCancel() // cancel the rest
			if response.VoteGranted {
				node.State = StateLeader
				go node.RunAsLeader(ctx)
				return
			} else {
				if response.Term > node.CurrentTerm {
					// some one has become a leader
					// voteCancel()
					node.CurrentTerm = response.Term
					node.ResetVoteFor()
					node.State = StateFollower
					go node.RunAsFollower(ctx)
					return
				} else {
					sugarLogger.Infof(
						"not enough votes, re-elect again, current term: %d, candidateID: %d",
						node.CurrentTerm, node.NodeId,
					)
				}
			}
		case <-ticker.C: // last election timeout withno response
			// voteCancel()
			consensusChan = node.runOneElection(ctx)
		case requestVoteInternal := <-node.requestVoteChan: // commonRule: handling voteRequest from another candidate
			if requestVoteInternal.IsTimeout.Load() {
				sugarLogger.Warn("request vote is timeout")
				continue
			}
			req := requestVoteInternal.Request
			resChan := requestVoteInternal.RespWraper
			resp := node.voting(req)
			wrapper := &rpc.RPCRespWrapper[*rpc.RequestVoteResponse]{
				Resp: resp,
				Err:  nil,
			}
			resChan <- wrapper
			// this means other candiate has a higher term
			if resp.VoteGranted {
				node.State = StateFollower
				go node.RunAsFollower(ctx)
				return
			}
		case req := <-node.appendEntryChan: // commonRule: handling appendEntry from a leader which can be stale or new
			resp := node.appendEntries(req.Request)
			wrappedResp := &rpc.RPCRespWrapper[*rpc.AppendEntriesResponse]{
				Resp: resp,
				Err:  nil,
			}
			req.RespWraper <- wrappedResp
			if resp.Success {
				// this means there is a leader there
				node.State = StateFollower
				node.CurrentTerm = req.Request.Term
				go node.RunAsFollower(ctx)
				return
			}
		}
	}
}
