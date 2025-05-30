package node

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/utils"
	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
)

type JobResult struct {
	ShallDegrade bool
	// if the shallDegrade is false, there is no need to fill in the term, and voteFor
	Term TermRank
	// if the current node doesn't vote for anyone when term changes, the voteFor is empty
	VotedFor string
}

// maki: take this simple version as the baseline, and add more gorouintes if this has performance issues
// analysis of why this pattern can be used:
// task2, task4 have data race if using different goroutine -> they both change raftlog/state machine in a serial order
// so one way is to use the same goroutine for task2 and task4 to handle log replication
// normally, the leader only has task4, and only in unstable conditions, it has to handle task2 and task4 at the same time
// if task4 accepts the log replication, the leader shall degrade to follower with graceful shutdown

// task3 also happens in unstable conditions, but it is less frequent than task2 so can also be in the same goroutine
// task1 has lower priority than task3

// the only worker thread needed is the log applicaiton thread
func (n *Node) runAsLeaderImpl(ctx context.Context) {

	if n.State != StateLeader {
		panic("node is not in LEADER state")
	}
	n.logger.Info("acquiring the Semaphore as the LEADER state")
	n.sem.Acquire(ctx, 1)
	n.logger.Info("acquired the Semaphore as the LEADER state")
	defer n.sem.Release(1)

	// debugging
	n.recordLeaderState()

	// reset the states
	go n.workerForLogApplication(ctx)

	heartbeatDuration := n.cfg.GetLeaderHeartbeatPeriod()
	tickerForHeartbeat := time.NewTicker(heartbeatDuration)
	defer tickerForHeartbeat.Stop()
	var singleJobResult JobResult
	var err error

	for {
		if singleJobResult.ShallDegrade {
			n.storeCurrentTermAndVotedFor(uint32(singleJobResult.Term), singleJobResult.VotedFor)
			n.leaderGracefulDegradation(ctx)
			return
		}
		select {
		case <-ctx.Done(): // give ctx higher priority
			n.logger.Warn("raft node main context done, exiting")
			return
		default:
			select {

			case <-ctx.Done():
				n.logger.Warn("raft node main context done, exiting")
				return

			// task1: send the heartbeat -> as leader, may degrade to follower
			case <-tickerForHeartbeat.C:
				tickerForHeartbeat.Reset(heartbeatDuration)
				singleJobResult, err = n.syncDoHeartbeatAsLeader(ctx)
				if err != nil {
					n.logger.Error("error in sending heartbeat, omit it and continue", zap.Error(err))
				}

			// task2: handle the client command, need to change raftlog/state machine -> as leader, may degrade to follower
			case clientCmd := <-n.clientCommandChan:
				tickerForHeartbeat.Reset(heartbeatDuration)
				batchingSize := n.cfg.GetRaftNodeRequestBufferSize() - 1
				clientCommands := utils.ReadMultipleFromChannel(n.clientCommandChan, batchingSize)
				clientCommands = append(clientCommands, clientCmd)
				singleJobResult, err = n.syncDoLogReplicationAsLeader(ctx, clientCommands)
				if err != nil {
					// todo: as is same with most other panics, temporary solution, shall handle the error properly
					panic(err)
				}

			// task3: handle the requestVoteChan -> as a node, may degrade to follower
			case internalReq := <-n.requestVoteChan:
				singleJobResult, err = n.handleRequestVoteAsLeader(ctx, internalReq)
				if err != nil {
					n.logger.Error("error in handling request vote", zap.Error(err))
					panic(err)
				}
			// task4: handle the appendEntryChan, need to change raftlog/state machine -> as a node, may degrade to follower
			case internalReq := <-n.appendEntryChan:
				singleJobResult, err = n.handlerAppendEntriesAsLeader(ctx, internalReq)
				if err != nil {
					n.logger.Error("error in handling append entries", zap.Error(err))
					panic(err)
				}
			}
		}
	}
}

// @return: shall degrade to follower or not
func (n *Node) syncDoHeartbeatAsLeader(ctx context.Context) (JobResult, error) {
	ctx, requestID := common.GetOrGenerateRequestID(ctx)
	currentTerm := n.getCurrentTerm()
	peerNodeIDs, err := n.membership.GetAllPeerNodeIDs()
	if err != nil {
		return JobResult{ShallDegrade: false}, err
	}
	cathupLogsForPeers, err := n.getLogsToCatchupForPeers(peerNodeIDs)
	if err != nil {
		return JobResult{ShallDegrade: false}, err
	}
	reqs := make(map[string]*rpc.AppendEntriesRequest, len(peerNodeIDs))
	for nodeID, catchup := range cathupLogsForPeers {
		catchupCommands := make([]*rpc.LogEntry, len(catchup.Entries))
		for i, log := range catchup.Entries {
			catchupCommands[i] = &rpc.LogEntry{
				Data: log.Commands,
			}
		}
		reqs[nodeID] = &rpc.AppendEntriesRequest{
			Term:         currentTerm,
			LeaderId:     n.NodeId,
			PrevLogIndex: catchup.LastLogIndex,
			PrevLogTerm:  catchup.LastLogTerm,
			Entries:      catchupCommands,
		}
	}

	ctxTimeout, cancel := context.WithTimeout(ctx, n.cfg.GetRPCRequestTimeout())
	defer cancel()
	resp, err := n.ConsensusAppendEntries(ctxTimeout, reqs, n.CurrentTerm)
	if err != nil {
		n.logger.Error("error in sending append entries to one node", zap.Error(err))
		return JobResult{ShallDegrade: false}, err
	}

	if resp.Success {
		n.logger.Info("append entries success", zap.String("requestID", requestID))
		return JobResult{ShallDegrade: false}, nil
	} else {
		if resp.Term > currentTerm {
			n.logger.Warn("peer's term is greater than current term", zap.String("requestID", requestID))
			return JobResult{ShallDegrade: true, Term: TermRank(resp.Term)}, nil
		} else {
			// todo: the unsafe panic is temporarily used for debugging
			panic("failed append entries, but without not a higher term, should not happen")
		}
	}
}

// happy path: 1) the leader is alive and the followers are alive (done)
// problem-1: the leader is alive but minority followers are dead -> can be handled by the retry mechanism
// problem-2: the leader is alive but majority followers are dead
// problem-3: the leader is stale
// @return: shall degrade to follower or not, and the error
func (n *Node) syncDoLogReplicationAsLeader(ctx context.Context, clientCommands []*utils.ClientCommandInternalReq) (JobResult, error) {

	var subTasksToWait sync.WaitGroup
	subTasksToWait.Add(2)
	currentTerm := n.getCurrentTerm()

	// prep:
	// get logs from the raft logs for each client
	// before the task-1 trying to change the logs and task-2 reading the logs in parallel and we don't know who is faster
	peerNodeIDs, err := n.membership.GetAllPeerNodeIDs()
	if err != nil {
		return JobResult{ShallDegrade: false}, err
	}
	cathupLogsForPeers, err := n.getLogsToCatchupForPeers(peerNodeIDs)
	if err != nil {
		return JobResult{ShallDegrade: false}, err
	}

	// task1: appends the command to the local as a new entry
	errorChanTask1 := make(chan error, 1)
	go func(ctx context.Context) {
		defer subTasksToWait.Done()
		commands := make([][]byte, len(clientCommands))
		for i, clientCommand := range clientCommands {
			commands[i] = clientCommand.Req.Command
		}
		errorChanTask1 <- n.raftLog.AppendLogsInBatch(ctx, commands, currentTerm)
	}(ctx)

	// task2 sends the command of appendEntries to all the followers in parallel to replicate the entry
	respChan := make(chan *AppendEntriesConsensusResp, 1)
	errorChanTask2 := make(chan error, 1)
	go func(ctx context.Context) {
		defer subTasksToWait.Done()
		ctxTimeout, _ := context.WithTimeout(ctx, n.cfg.GetRPCRequestTimeout())
		newCommands := make([]*rpc.LogEntry, len(clientCommands))
		for i, clientCommand := range clientCommands {
			newCommands[i] = &rpc.LogEntry{
				Data: clientCommand.Req.Command,
			}
		}
		reqs := make(map[string]*rpc.AppendEntriesRequest, len(peerNodeIDs))
		for nodeID, catchup := range cathupLogsForPeers {
			catchupCommands := make([]*rpc.LogEntry, len(catchup.Entries))
			for i, log := range catchup.Entries {
				catchupCommands[i] = &rpc.LogEntry{
					Data: log.Commands,
				}
			}
			reqs[nodeID] = &rpc.AppendEntriesRequest{
				Term:         currentTerm,
				LeaderId:     n.NodeId,
				PrevLogIndex: catchup.LastLogIndex,
				PrevLogTerm:  catchup.LastLogTerm,
				Entries:      append(catchupCommands, newCommands...),
			}
		}
		resp, err := n.ConsensusAppendEntries(ctxTimeout, reqs, n.getCurrentTerm())
		respChan <- resp
		errorChanTask2 <- err
	}(ctx)

	// todo: shall retry forever?
	// todo: what if task1 fails and task2 succeeds?
	// todo: what if task1 succeeds and task2 fails?
	// task3 when the entry has been safely replicated, the leader applies the entry to the state machine
	subTasksToWait.Wait()
	// maki: not sure how to handle the error?
	if err := <-errorChanTask1; err != nil {
		n.logger.Error("error in appending logs to raft log", zap.Error(err))
		panic("not sure how to handle the error")
	}
	if err := <-errorChanTask2; err != nil {
		n.logger.Error("error in sending append entries to one node", zap.Error(err))
		panic("not sure how to handle the error")
	}
	resp := <-respChan
	if !resp.Success { // no consensus
		if resp.Term > currentTerm {
			return JobResult{ShallDegrade: true, Term: TermRank(resp.Term)}, nil
		} else {
			// todo: the unsafe panic is temporarily used for debugging
			panic("failed append entries, but without not a higher term")
		}
	} else { // consensus
		// (4) the leader applies the command, and responds to the client
		newCommitID := n.raftLog.GetLastLogIdx()
		n.updateCommitIdx(newCommitID)

		// (5) send to the apply command channel
		n.applyToStateMachineSignalChan <- true
		return JobResult{ShallDegrade: false, Term: TermRank(currentTerm)}, nil
	}
}

func (n *Node) handlerAppendEntriesAsLeader(ctx context.Context, internalReq *utils.AppendEntriesInternalReq) (JobResult, error) {
	req := internalReq.Req
	reqTerm := uint32(req.Term)
	currentTerm := n.getCurrentTerm()

	if reqTerm > currentTerm {
		result := JobResult{ShallDegrade: true, Term: TermRank(reqTerm)}
		// maki: this may be a very tricky design in implementation,
		// but this simplifies the logic here
		// 3rd reply the response, we directly reject this reject, and fix the log after
		// the leader degrade to follower
		internalReq.RespChan <- &utils.RPCRespWrapper[*rpc.AppendEntriesResponse]{
			Resp: &rpc.AppendEntriesResponse{
				Term:    currentTerm,
				Success: false,
			},
			Err: nil,
		}
		return result, nil
	} else if reqTerm < currentTerm {
		internalReq.RespChan <- &utils.RPCRespWrapper[*rpc.AppendEntriesResponse]{
			Resp: &rpc.AppendEntriesResponse{
				Term:    currentTerm,
				Success: false,
			},
			Err: nil,
		}
		return JobResult{ShallDegrade: false}, nil
	} else {
		panic("shouldn't happen, break the property of Election Safety")
	}
}

func (n *Node) handleRequestVoteAsLeader(ctx context.Context, internalReq *utils.RequestVoteInternalReq) (JobResult, error) {
	resp := n.handleVoteRequest(internalReq.Req)
	wrapper := utils.RPCRespWrapper[*rpc.RequestVoteResponse]{
		Resp: resp,
		Err:  nil,
	}
	internalReq.RespChan <- &wrapper
	if resp.VoteGranted {
		return JobResult{ShallDegrade: true, Term: TermRank(internalReq.Req.Term)}, nil
	} else {
		return JobResult{ShallDegrade: false}, nil
	}
}

func (n *Node) recordLeaderState() {
	stateFilePath := "leader.tmp"
	file, err := os.OpenFile(stateFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		n.logger.Error("failed to open state file", zap.Error(err))
		panic(err)
	}
	defer file.Close()

	currentTime := time.Now().Format(time.RFC3339)
	entry := currentTime + " " + n.NodeId + "\n"

	_, writeErr := file.WriteString(entry)
	if writeErr != nil {
		n.logger.Error("failed to append NodeID to state file", zap.Error(writeErr))
		panic(writeErr)
	}
}

// todo: to be implemented
func (n *Node) leaderGracefulDegradation(ctx context.Context) {
	// todo:
	// no need to reject clientCommands now, we can
	// delegate this kind of job to the folllower
	// reject all not handled clientCommands

	// apply all committed logs and send reply to clients

}

// todo: to be implemented
func (n *Node) workerForLogApplication(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		}
	}
}
