package mkraft

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

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
	raftlog plugs.RaftLogsIface,
	statemachine plugs.StateMachineIface,
) NodeIface {
	bufferSize := cfg.GetRaftNodeRequestBufferSize()

	// todo: can be a problem of these two intializations
	// zero can be problematic, but these can also be a problem
	// we can fix with happy path first like start from a new cluster, and never fails
	// lastAppliedIdx := statemachine.GetLatestAppliedIndex()
	// lastCommitIdx, _ := raftlog.GetLastLogIdxAndTerm()

	node := &Node{
		raftLog:      raftlog,
		membership:   membership,
		cfg:          cfg,
		logger:       logger,
		statemachine: statemachine,

		stateRWLock: &sync.RWMutex{},
		sem:         semaphore.NewWeighted(1),

		NodeId:            nodeId,
		State:             StateFollower, // servers start up as followers
		clientCommandChan: make(chan *utils.ClientCommandInternalReq, bufferSize),
		// todo:  what should the length of the channel be?
		leaderDegradationChan: make(chan TermRank, 1),
		requestVoteChan:       make(chan *utils.RequestVoteInternalReq, bufferSize),
		appendEntryChan:       make(chan *utils.AppendEntriesInternalReq, bufferSize),

		// todo: how should this be initialized and maintained
		CurrentTerm: 0,
		VotedFor:    "",

		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[string]uint64, 6),
		matchIndex:  make(map[string]uint64, 6),
	}

	// load persistent state
	err := node.loadCurrentTermAndVotedFor()
	if err != nil {
		node.logger.Error("error loading current term and voted for", zap.Error(err))
		panic(err)
	}
	// load logs
	node.catchupAppliedIdx()

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

func (n *Node) getStateFileName() string {
	return "raftstate"
}

// load from file system, shall be called at the beginning of the node
func (n *Node) loadCurrentTermAndVotedFor() error {
	n.stateRWLock.Lock()
	defer n.stateRWLock.Unlock()

	// if not exists, initialize to default values
	if _, err := os.Stat(n.getStateFileName()); os.IsNotExist(err) {
		n.logger.Info("raft state file does not exist, initializing to default values")
		n.CurrentTerm = 0
		n.VotedFor = ""
		return nil
	}

	file, err := os.Open(n.getStateFileName())
	if err != nil {
		if os.IsNotExist(err) {
			n.logger.Info("raft state file does not exist, initializing to default values")
			n.CurrentTerm = 0
			n.VotedFor = ""
			return nil
		}
		return err
	}
	defer file.Close()

	var term uint32
	var voteFor string
	cnt, err := fmt.Fscanf(file, "%d,%s", &term, &voteFor)
	if err != nil {
		return fmt.Errorf("error reading raft state file: %w", err)
	}

	if cnt != 2 {
		return fmt.Errorf("expected 2 values in raft state file, got %d", cnt)
	}

	n.CurrentTerm = term
	n.VotedFor = voteFor

	n.logger.Debug("loadCurrentTermAndVotedFor",
		zap.String("fileName", n.getStateFileName()),
		zap.Int("bytesRead", cnt),
		zap.Uint32("term", n.CurrentTerm),
		zap.String("voteFor", n.VotedFor),
	)
	return nil
}

// store to file system, shall be called when the term or votedFor changes
func (n *Node) storeCurrentTermAndVotedFor(term uint32, voteFor string) error {
	n.stateRWLock.Lock()
	defer n.stateRWLock.Unlock()
	shouldReturn, err := n.unsafeUpdateNodeTermAndVoteFor(term, voteFor)
	if shouldReturn {
		return err
	}
	n.CurrentTerm = term
	return nil
}

func (n *Node) updateCurrentTermAndVotedForAsCandidate() error {
	n.stateRWLock.Lock()
	defer n.stateRWLock.Unlock()
	term := n.CurrentTerm + 1
	voteFor := n.NodeId
	shouldReturn, err := n.unsafeUpdateNodeTermAndVoteFor(term, voteFor)
	if shouldReturn {
		return err
	}
	n.CurrentTerm = term
	return nil
}

func (n *Node) unsafeUpdateNodeTermAndVoteFor(term uint32, voteFor string) (bool, error) {
	formatted := time.Now().Format("20060102150405.000")
	numericTimestamp := formatted[:len(formatted)-4] + formatted[len(formatted)-3:]
	fileName := fmt.Sprintf("%s_%s.tmp", n.getStateFileName(), numericTimestamp)

	file, err := os.Create(fileName)
	if err != nil {
		return true, err
	}

	cnt, err := file.WriteString(fmt.Sprintf("%d,%s", term, voteFor))
	n.logger.Debug("storeCurrentTermAndVotedFor",
		zap.String("fileName", fileName),
		zap.Int("bytesWritten", cnt),
		zap.Uint32("term", term),
		zap.String("voteFor", voteFor),
		zap.Error(err),
	)

	err = file.Sync()
	if err != nil {
		n.logger.Error("error syncing file", zap.String("fileName", fileName), zap.Error(err))
		return true, err
	}
	err = file.Close()
	if err != nil {
		n.logger.Error("error closing file", zap.String("fileName", fileName), zap.Error(err))
		return true, err
	}

	err = os.Rename(fileName, n.getStateFileName())
	if err != nil {
		panic(err)
	}

	n.VotedFor = voteFor
	return false, nil
}

// normal read
func (n *Node) getCurrentTerm() uint32 {
	n.stateRWLock.RLock()
	defer n.stateRWLock.RUnlock()
	return n.CurrentTerm
}

func (node *Node) VoteRequest(req *utils.RequestVoteInternalReq) {
	node.requestVoteChan <- req
}

func (node *Node) AppendEntryRequest(req *utils.AppendEntriesInternalReq) {
	node.appendEntryChan <- req
}

// servers start up as followers
func (node *Node) Start(ctx context.Context) {
	go node.RunAsFollower(ctx)
}

// gracefully stop the node and cleanup
func (node *Node) Stop(ctx context.Context) {
	close(node.appendEntryChan)
	close(node.clientCommandChan)
}

// TODO: THE WHOLE MODULE SHALL BE REFACTORED TO BE AN INTEGRAL OF THE CONSENSUS ALGORITHM
// The decision of consensus upon receiving a request
// can be independent of the current state of the node

// maki: jthis method should be a part of the consensus algorithm
// todo: right now this method doesn't check the current state of the node
// todo: checks the receiveVoteRequest works correctly for any state of the node, candidate or leader or follower
// todo: not sure what state shall be changed inside or outside in the caller
func (node *Node) receiveVoteRequest(req *rpc.RequestVoteRequest) *rpc.RequestVoteResponse {

	var response rpc.RequestVoteResponse
	currentTerm := node.getCurrentTerm()

	if req.Term > currentTerm {
		err := node.storeCurrentTermAndVotedFor(req.Term, req.CandidateId) // did vote for the candidate
		if err != nil {
			node.logger.Error(
				"error in storeCurrentTermAndVotedFor", zap.Error(err),
				zap.String("nId", node.NodeId),
				zap.Uint32("term", req.Term), zap.String("candidateId", req.CandidateId))
			panic(err) // critical error, cannot continue
		}
		response = rpc.RequestVoteResponse{
			Term:        req.Term,
			VoteGranted: true,
		}
	} else if req.Term < currentTerm {
		response = rpc.RequestVoteResponse{
			Term:        currentTerm,
			VoteGranted: false,
		}
	} else {
		if node.VotedFor == "" || node.VotedFor == req.CandidateId {
			node.VotedFor = req.CandidateId
			response = rpc.RequestVoteResponse{
				Term:        currentTerm,
				VoteGranted: true,
			}
		} else {
			response = rpc.RequestVoteResponse{
				Term:        currentTerm,
				VoteGranted: false,
			}
		}
	}
	return &response
}

// maki: jthis method should be a part of the consensus algorithm
// todo: right now this method doesn't check the current state of the node
// todo: not sure what state shall be changed inside or outside in the caller
func (n *Node) receiveAppendEntires(req *rpc.AppendEntriesRequest) *rpc.AppendEntriesResponse {
	var response rpc.AppendEntriesResponse
	reqTerm := uint32(req.Term)
	currentTerm := n.getCurrentTerm()

	if reqTerm > currentTerm {
		err := n.storeCurrentTermAndVotedFor(reqTerm, "") // did not vote for anyone
		if err != nil {
			n.logger.Error(
				"error in storeCurrentTermAndVotedFor", zap.Error(err),
				zap.String("nId", n.NodeId))
			panic(err) // critical error, cannot continue
		}
		response = rpc.AppendEntriesResponse{
			Term:    currentTerm,
			Success: true,
		}
	} else if reqTerm < currentTerm {
		response = rpc.AppendEntriesResponse{
			Term:    currentTerm,
			Success: false,
		}
	} else {
		// should accecpet it directly?
		response = rpc.AppendEntriesResponse{
			Term:    currentTerm,
			Success: true,
		}
	}
	return &response
}
