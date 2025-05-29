package node

import (
	"context"
	"sync"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/peers"
	"github.com/maki3cat/mkraft/mkraft/plugs"
	"github.com/maki3cat/mkraft/mkraft/utils"
	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

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

type TermRank int

var _ NodeIface = (*Node)(nil)

type NodeIface interface {
	// todo: lost requestID and other values in the context
	VoteRequest(req *utils.RequestVoteInternalReq)
	// todo: lost requestID and other values in the context
	AppendEntryRequest(req *utils.AppendEntriesInternalReq)
	// todo: lost requestID and other values in the context
	ClientCommand(req *utils.ClientCommandInternalReq)

	Start(ctx context.Context)
	Stop(ctx context.Context)
}

// not only new a class but also catch up statemachine, so it may cost time
func NewNode(
	nodeId string,
	cfg common.ConfigIface,
	logger *zap.Logger,
	membership peers.MembershipMgrIface,
) NodeIface {
	bufferSize := cfg.GetRaftNodeRequestBufferSize()

	// todo: can be a problem of these two intializations
	// zero can be problematic, but these can also be a problem
	// we can fix with happy path first like start from a new cluster, and never fails
	// lastAppliedIdx := statemachine.GetLatestAppliedIndex()
	// lastCommitIdx, _ := raftlog.GetLastLogIdxAndTerm()

	node := &Node{
		membership: membership,
		cfg:        cfg,
		logger:     logger,

		stateRWLock: &sync.RWMutex{},
		sem:         semaphore.NewWeighted(1),

		NodeId:            nodeId,
		State:             StateFollower, // servers start up as followers
		clientCommandChan: make(chan *utils.ClientCommandInternalReq, bufferSize),
		// todo:  what should the length of the channel be?
		leaderDegradationChan: make(chan TermRank, 1),
		requestVoteChan:       make(chan *utils.RequestVoteInternalReq, bufferSize),
		appendEntryChan:       make(chan *utils.AppendEntriesInternalReq, bufferSize),

		// persistent state on all servers
		CurrentTerm: 0,
		VotedFor:    "",

		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[string]uint64, 6),
		matchIndex:  make(map[string]uint64, 6),
	}

	// initialize the raft log and statemachine
	raftLogIface := plugs.NewRaftLogsImplAndLoad(cfg.GetRaftLogFilePath())
	statemachine := plugs.NewStateMachineNoOpImpl()
	node.raftLog = raftLogIface
	node.statemachine = statemachine

	// load persistent state
	err := node.loadCurrentTermAndVotedFor()
	if err != nil {
		node.logger.Error("error loading current term and voted for", zap.Error(err))
		panic(err)
	}
	// load logs
	node.catchupAppliedIdxOnStartup()

	node.sem.Acquire(context.Background(), 1)
	defer node.sem.Release(1)

	return node
}

// the Raft Server Node
// maki: go gymnastics for sync values
// todo: add sync for these values?
type Node struct {
	raftLog      plugs.RaftLogsIface // required, persistent
	membership   peers.MembershipMgrIface
	cfg          common.ConfigIface
	logger       *zap.Logger
	statemachine plugs.StateMachineIface

	// for the node state
	sem *semaphore.Weighted
	// a RW mutex for all the internal states in this node
	stateRWLock *sync.RWMutex

	NodeId string // maki: nodeID uuid or number or something else?
	State  NodeState

	// leader only channels
	// gracefully clean every time a leader degrades to a follower
	// reset these 2 data structures everytime a new leader is elected
	clientCommandChan     chan *utils.ClientCommandInternalReq
	leaderDegradationChan chan TermRank

	// shared by all states
	requestVoteChan chan *utils.RequestVoteInternalReq
	appendEntryChan chan *utils.AppendEntriesInternalReq

	// Persistent state on all servers
	CurrentTerm uint32 // required, persistent (todo: haven't get persisted yet)
	VotedFor    string // required, persistent (todo: haven't get persisted yet, why ?)
	// LogEntries

	// Paper page 4:
	commitIndex uint64 // required, volatile on all servers
	lastApplied uint64 // required, volatile on all servers

	// required, volatile, on leaders only, reinitialized after election, initialized to leader last log index+1
	nextIndex  map[string]uint64 // map[peerID]nextIndex, index of the next log entry to send to that server
	matchIndex map[string]uint64 // map[peerID]matchIndex, index of highest log entry known to be replicated on that server
}

func (node *Node) Start(ctx context.Context) {
	go node.RunAsFollower(ctx)
}

// gracefully stop the node and cleanup
func (node *Node) Stop(ctx context.Context) {
	close(node.appendEntryChan)
	close(node.clientCommandChan)
}

func (node *Node) VoteRequest(req *utils.RequestVoteInternalReq) {
	node.requestVoteChan <- req
}

func (node *Node) AppendEntryRequest(req *utils.AppendEntriesInternalReq) {
	node.appendEntryChan <- req
}

func (n *Node) ClientCommand(req *utils.ClientCommandInternalReq) {
	if n.State != StateLeader {
		n.logger.Warn("Client command received but node is not a leader, dropping request",
			zap.String("nodeID", n.NodeId))
		req.RespChan <- &utils.RPCRespWrapper[*rpc.ClientCommandResponse]{
			Err: utils.ErrNotLeader,
		}
		return
	}
	n.clientCommandChan <- req
}
