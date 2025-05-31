package node

// "ATOMIC" UNIT OF JOBS FOR THE FOLLOWER/CANDIDATE
import (
	"context"
	"sync"
	"time"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/utils"
	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
)

func (n *Node) noleaderApplyCommandToStateMachine() {
	commitIdx, lastApplied := n.getCommitIdxAndLastApplied()
	if commitIdx > lastApplied {
		logs, err := n.raftLog.GetLogsFromIdxIncluded(lastApplied + 1)
		if err != nil {
			n.logger.Error("failed to get logs from index", zap.Error(err))
			panic(err)
		}
		// apply the command
		for _, log := range logs {
			// the no-leader node doesn't need to respond to clients
			_, err := n.statemachine.ApplyCommand(log.Commands)
			if err != nil {
				n.logger.Error("failed to apply command to state machine", zap.Error(err))
				panic(err)
			} else {
				n.incrementLastApplied(1)
			}
		}
	}
}

// here is actually a variate pipeline pattern:
// first step is log replication, second step is apply to the state machine
// and this is the 2nd step in the pipeline of log (1-replication, 2-apply)
// shall NOT reset the chan inside this function because the main thread may write to it simultaneously
func (n *Node) noLeaderWorkerToApplyCommandToStateMachine(ctx context.Context, workerWaitGroup *sync.WaitGroup) {
	defer workerWaitGroup.Done()

	tickerTriggered := time.NewTicker(time.Duration(500 * time.Microsecond))
	defer tickerTriggered.Stop()

	for {
		select {
		case <-ctx.Done():
			n.logger.Warn("apply-worker, exiting leader's worker for applying commands")
		default:
			select {
			// try to signal this channel this everytime with heartbeat
			case <-tickerTriggered.C:
				n.noleaderApplyCommandToStateMachine()
			case <-n.noleaderApplySignalChan:
				utils.DrainChannel(n.noleaderApplySignalChan)
				n.noleaderApplyCommandToStateMachine()
			case <-ctx.Done():
				n.logger.Warn("apply-worker, exiting leader's worker for applying commands")
				return
			}
		}
	}
}

// maki: lastLogIndex, commitIndex, lastApplied can be totally different from each other
// shall be called when the node is not a leader
// the raft server is generally single-threaded, so there is no other thread to change the commitIdx
func (n *Node) handlerAppendEntriesAsNoLeader(ctx context.Context, req *rpc.AppendEntriesRequest) *rpc.AppendEntriesResponse {

	var response rpc.AppendEntriesResponse
	reqTerm := uint32(req.Term)
	currentTerm := n.getCurrentTerm()

	// return FALSE CASES:
	// (1) fast track for the stale term
	// (2) check the prevLogIndex and prevLogTerm
	if reqTerm < currentTerm || !n.raftLog.CheckPreLog(req.PrevLogIndex, req.PrevLogTerm) {
		response = rpc.AppendEntriesResponse{
			Term:    currentTerm,
			Success: false,
		}
		return &response
	}

	// return TRUE CASES:

	// 1. udpate commitIdx, and trigger the apply
	defer func() {
		// the updateCommitIdx will find the min(leaderCommit, index of last new entry in the log), so the update
		// doesn't require result of appendLogs
		n.updateCommitIdx(req.LeaderCommit)
		n.noleaderApplySignalChan <- true
	}()

	// 2. update the term
	if reqTerm > currentTerm {
		err := n.storeCurrentTermAndVotedFor(reqTerm, "") // did not vote for anyone
		if err != nil {
			n.logger.Error(
				"error in storeCurrentTermAndVotedFor", zap.Error(err),
				zap.String("nId", n.NodeId))
			panic(err) // todo: critical error, cannot continue, not sure how to handle this
		}
	}

	// 3. append logs
	if len(req.Entries) > 0 {
		logs := make([][]byte, len(req.Entries))
		for idx, entry := range req.Entries {
			logs[idx] = entry.Data
		}
		err := n.raftLog.UpdateLogsInBatch(ctx, req.PrevLogIndex, logs, req.Term)
		if err != nil {
			// this error cannot be not match,
			// because the prevLogIndex and prevLogTerm has been checked
			panic(err) // todo: critical error, cannot continue, not sure how to handle this
		}
	}

	response = rpc.AppendEntriesResponse{
		Term:    currentTerm,
		Success: true,
	}
	return &response
}

// Specifical Rule for Candidate Election:
// (1) increment currentTerm
// (2) vote for self
// (3) send RequestVote RPCs to all other servers
// if votes received from majority of servers: become leader
// if AppendEntries RPC received from new leader: convert to follower
// if election timeout elapses: start new election
func (node *Node) runOneElectionAsCandidate(ctx context.Context) chan *MajorityRequestVoteResp {
	ctx, requestID := common.GetOrGenerateRequestID(ctx)
	consensusChan := make(chan *MajorityRequestVoteResp, 1)

	err := node.updateCurrentTermAndVotedForAsCandidate()
	if err != nil {
		node.logger.Error(
			"error in updateCurrentTermAndVotedForAsCandidate", zap.String("requestID", requestID), zap.Error(err))
		panic(err) // critical error, cannot continue
	}

	req := &rpc.RequestVoteRequest{
		Term:        node.getCurrentTerm(),
		CandidateId: node.NodeId,
	}
	// todo: must I call the cancel function?
	ctxTimeout, _ := context.WithTimeout(
		ctx, time.Duration(node.cfg.GetElectionTimeout()))
	go func() {
		resp, err := node.ConsensusRequestVote(ctxTimeout, req)
		if err != nil {
			node.logger.Error(
				"error in RequestVoteSendForConsensus", zap.String("requestID", requestID), zap.Error(err))
			return
		} else {
			consensusChan <- resp
		}
	}()
	return consensusChan
}
