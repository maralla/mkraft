package node

import (
	"context"
	"sync"
	"time"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/utils"
	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
)

// be called when the node becomes a leader
// todo: need to reconcile with the "safey" chapter
func (n *Node) cleanupApplyLogsBeforeToLeader() {
	utils.DrainChannel(n.noleaderApplySignalCh)
	n.noleaderSyncDoApplyLogs()
}

// WORKER:(2)
// this worker is used to handle client commands
// which can be run independently of the main logic
// maki: the tricky part is that the client command needs NOT to be drained but the apply signal needs to be drained
func (n *Node) noleaderWorkerForClientCommand(ctx context.Context, workerWaitGroup *sync.WaitGroup) {
	defer workerWaitGroup.Done()
	for {
		select {
		case <-ctx.Done():
			n.logger.Warn("client-command-worker, exiting leader's worker for client commands")
			return
		default:
			select {
			case <-ctx.Done():
				n.logger.Warn("client-command-worker, exiting leader's worker for client commands")
				return
			case cmd := <-n.leaderApplyCh:
				n.logger.Warn("client-command-worker, received client command")
				// easy trivial work, can be done in parallel with the main logic
				// feature: add delegation to the leader
				cmd.RespChan <- &utils.RPCRespWrapper[*rpc.ClientCommandResponse]{
					Resp: &rpc.ClientCommandResponse{
						Result: nil,
					},
					Err: common.ErrNotLeader,
				}
			}
		}
	}
}

// WORKER:(1)
// here is actually a variate pipeline pattern:
// first step is log replication, second step is apply to the state machine
// and this is the 2nd step in the pipeline of log (1-replication, 2-apply)
// shall NOT reset the chan inside this function because the main thread may write to it simultaneously
func (n *Node) noleaderWorkerToApplyLogs(ctx context.Context, workerWaitGroup *sync.WaitGroup) {
	defer workerWaitGroup.Done()

	tickerTriggered := time.NewTicker(time.Duration(100 * time.Microsecond)) // empirical number
	defer tickerTriggered.Stop()

	for {
		select {
		case <-ctx.Done():
			n.logger.Warn("apply-worker, exiting leader's worker for applying commands")
			return
		default:
			select {
			case <-tickerTriggered.C:
				n.noleaderSyncDoApplyLogs()
			case <-n.noleaderApplySignalCh:
				utils.DrainChannel(n.noleaderApplySignalCh)
				n.noleaderSyncDoApplyLogs()
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
func (n *Node) noleaderHandleAppendEntries(ctx context.Context, req *rpc.AppendEntriesRequest) *rpc.AppendEntriesResponse {

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
		n.noleaderApplySignalCh <- true
	}()

	// 2. update the term
	if reqTerm > currentTerm {
		err := n.storeCurrentTermAndVotedFor(reqTerm, "", false) // did not vote for anyone
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

// apply the committed yet not applied logs to the state machine
func (n *Node) noleaderSyncDoApplyLogs() {
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

// Specifical Rule for Candidate Election:
// (1) increment currentTerm
// (2) vote for self
// (3) send RequestVote RPCs to all other servers
// if votes received from majority of servers: become leader
// if AppendEntries RPC received from new leader: convert to follower
// if election timeout elapses: start new election

// the implementation:
// triggers peerCnt + 1 goroutines for fan-out rpc and fan-in the responses
func (node *Node) candidateAsyncDoElection(ctx context.Context) chan *MajorityRequestVoteResp {

	ctx, requestID := common.GetOrGenerateRequestID(ctx)
	consensusChan := make(chan *MajorityRequestVoteResp, 1)

	err := node.updateCurrentTermAndVotedForAsCandidate(false)
	if err != nil {
		// todo: probably we shall recover in the man runAs??
		node.logger.Error(
			"this shouldn't happen, bugs in updateCurrentTermAndVotedForAsCandidate",
			zap.String("requestID", requestID), zap.Error(err))
		panic(err) // critical error, cannot continue
	}

	go func() {
		req := &rpc.RequestVoteRequest{
			Term:        node.getCurrentTerm(),
			CandidateId: node.NodeId,
		}
		resp, err := node.ConsensusRequestVote(ctx, req)
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
