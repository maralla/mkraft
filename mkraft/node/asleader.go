package node

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/plugs"
	"github.com/maki3cat/mkraft/mkraft/utils"
	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
)

/*
SECTION1: THE COMMON RULE (paper)
If any RPC request or response is received from a server with a higher term,
convert to follower

How the Shared Rule works for Leaders with 3 scenarios:
(MAKI comments)
(1) response of AppendEntries RPC sent by itself (OK)
(2) receive request of AppendEntries RPC from a server with a higher term (OK)
(3) receive request of RequestVote RPC from a server with a higher term (OK)

// SECTION2: SPECIFICAL RULE FOR LEADERS (paper)
(1) Upon election:

	send initial empty AppendEntries (heartbeat) RPCs to each reserver;
	repeat during idle periods to prevent election timeouts; (5.2) (OK)

(2) If command received from client:

	append entry to local log, respond after entry applied to state machine; (5.3) (OK)

(3) If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex for the follower;
If successful: update nextIndex and matchIndex for follower
If AppendEntries fails because of log inconsistency: decrement nextIndex and retry

(4) If there exists and N such that N > committedIndex, a majority of matchIndex[i] ≥ N, ... (5.3/5.4)
todo: this paper doesn't mention how a stale leader catches up and becomes a follower
*/
func (n *Node) RunAsLeader(ctx context.Context) {

	if n.State != StateLeader {
		panic("node is not in LEADER state")
	}
	n.logger.Info("acquiring the Semaphore as the LEADER state")
	n.sem.Acquire(ctx, 1)
	n.logger.Info("acquired the Semaphore as the LEADER state")
	defer n.sem.Release(1)

	// debugging
	stateFilePath := "state.tmp"
	file, err := os.OpenFile(stateFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		n.logger.Error("failed to open state file", zap.Error(err))
		return
	}
	defer file.Close()

	currentTime := time.Now().Format(time.RFC3339)
	entry := currentTime + " " + n.NodeId + "\n"

	_, writeErr := file.WriteString(entry)
	if writeErr != nil {
		n.logger.Error("failed to append NodeID to state file", zap.Error(writeErr))
		return
	}

	// leader:
	// (1) leader-task-1: handle client commands and send append entries -> goroutine-worker-1 handled by clientCommandsTaskWorker
	// (2) leader-task-2: send heartbeats to all the followers -> goroutine-main
	// (3) common-task-1: handle voting requests from other candidates -> goroutine-worker-2
	// (4) common-task-2: handle append entries requests from other candidates -> goroutine-worker-2

	n.clientCommandChan = make(chan *utils.ClientCommandInternalReq, n.cfg.GetRaftNodeRequestBufferSize())
	n.leaderDegradationChan = make(chan TermRank, n.cfg.GetRaftNodeRequestBufferSize())

	// these channels cannot be closed because they are created in the main thread but used in the worker
	// we need to clean the channel when the leader quits,
	// and reset the channels when the leader comes back

	// workers for the leader, subgoroutines
	ctxForWorker, cancelWorker := context.WithCancel(ctx)
	defer cancelWorker()

	go n.leaderWorkerForCommon(ctxForWorker)

	heartbeatDuration := n.cfg.GetLeaderHeartbeatPeriod()
	tickerForHeartbeat := time.NewTicker(heartbeatDuration)
	defer tickerForHeartbeat.Stop()
	go n.leaderWorkerForClient(ctxForWorker, tickerForHeartbeat)

	for {
		select {
		case <-ctx.Done(): // give ctx higher priority
			n.logger.Warn("raft node main context done, exiting")
			return
		default:
			select {
			case <-ctx.Done():
				n.logger.Warn("raft node main context done, exiting")
				return
			case newTerm := <-n.leaderDegradationChan:
				n.State = StateFollower
				err := n.storeCurrentTermAndVotedFor(uint32(newTerm), "") // downgrade to follower but did not vote for anyone
				if err != nil {
					n.logger.Error("error in storing current term and voted for", zap.Error(err))
					panic(err) // critical error, cannot continue
				}
				go n.RunAsFollower(ctx)
				return
			case <-tickerForHeartbeat.C:
				// it seems here can block until the heartbeat finishes
				// the assumption is that the rpc timeout is short and the handling of rpc response
				// is fast enough
				tickerForHeartbeat.Stop()
				err := n.handleHeartbeat(ctx)
				if err != nil {
					n.logger.Error("error in sending heartbeat", zap.Error(err))
				}
				tickerForHeartbeat.Reset(heartbeatDuration)
			}
		}
	}
}

// todo: can be merged to main
// separate goroutine for the this task
// handling requests from clients in a serializable and batched way
func (n *Node) leaderWorkerForClient(ctx context.Context, heartbeatTicker *time.Ticker) {
	defer n.closeClientCommandChan()
	for {
		select {
		case <-ctx.Done():
			n.logger.Warn("exiting leader's worker for handling commands Task")
			return
		default:
			select {
			case <-ctx.Done():
				n.logger.Warn("exiting leader's worker for handling commands Task")
				return
			case clientCmd := <-n.clientCommandChan:
				// todo: has race with the heartbeat timer, but since it is idempotent, it is okay
				heartbeatTicker.Stop()

				batchingSize := n.cfg.GetRaftNodeRequestBufferSize() - 1
				clientCommands := utils.ReadMultipleFromChannel(n.clientCommandChan, batchingSize)
				clientCommands = append(clientCommands, clientCmd)
				error := n.handleClientCommand(ctx, clientCommands)
				if error != nil {
					n.logger.Error("error in handling client command", zap.Error(error))
					for _, clientCommand := range clientCommands {
						clientCommand.RespChan <- &utils.RPCRespWrapper[*rpc.ClientCommandResponse]{
							Resp: nil,
							Err:  error,
						}
					}
				}
				heartbeatTicker.Reset(n.cfg.GetLeaderHeartbeatPeriod())
			}
		}
	}
}

// separate goroutine for the this task
func (n *Node) leaderWorkerForCommon(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			n.logger.Warn("leader's worker context done, exiting")
			return
		default:
			select {
			case <-ctx.Done():
				n.logger.Warn("leader's worker context done, exiting")
				return
			case internalReq := <-n.requestVoteChan: // commonRule: same with candidate
				if !internalReq.IsTimeout.Load() {
					// no-IO operation
					req := internalReq.Req
					resp := n.handleVoteRequest(req)
					wrappedResp := &utils.RPCRespWrapper[*rpc.RequestVoteResponse]{
						Resp: resp,
						Err:  nil,
					}
					internalReq.RespChan <- wrappedResp
					if resp.VoteGranted {
						n.leaderDegradationChan <- TermRank(resp.Term)
						return
					}
				}
			case internalReq := <-n.appendEntryChan: // commonRule: same with candidate
				// todo: shall add appendEntry operations which shall be a goroutine
				resp := n.handlerAppendEntries(internalReq.Req)
				wrapper := utils.RPCRespWrapper[*rpc.AppendEntriesResponse]{
					Resp: resp,
					Err:  nil,
				}
				internalReq.RespChan <- &wrapper
				if resp.Success {
					n.leaderDegradationChan <- TermRank(resp.Term)
					return
				}
			}
		}
	}
}

func (n *Node) getLogsToCatchupForPeers(peerNodeIDs []string) (map[string]plugs.CatchupLogs, error) {
	result := make(map[string]plugs.CatchupLogs)
	for _, peerNodeID := range peerNodeIDs {
		// todo: can be batch reading
		nextID := n.getPeersNextIndex(peerNodeID)
		logs, err := n.raftLog.GetLogsFromIdxIncluded(nextID)
		if err != nil {
			n.logger.Error("failed to get logs from index", zap.Error(err))
			return nil, err
		}
		prevLogIndex := nextID - 1
		prevTerm, error := n.raftLog.GetTermByIndex(prevLogIndex)
		if error != nil {
			n.logger.Error("failed to get term by index", zap.Error(error))
			return nil, error
		}
		result[peerNodeID] = plugs.CatchupLogs{
			LastLogIndex: prevLogIndex,
			LastLogTerm:  prevTerm,
			Entries:      logs,
		}
	}
	return result, nil
}

// synchronous call
// todo: shall add batching
// happy path: 1) the leader is alive and the followers are alive (done)
// problem-1: the leader is alive but minority followers are dead -> can be handled by the retry mechanism
// problem-2: the leader is alive but majority followers are dead
// problem-3: the leader is stale
func (n *Node) handleClientCommand(ctx context.Context, clientCommands []*utils.ClientCommandInternalReq) error {

	var subTasksToWait sync.WaitGroup
	subTasksToWait.Add(2)
	currentTerm := n.getCurrentTerm()

	// prep:
	// get logs from the raft logs for each client
	// before the task-1 trying to change the logs and task-2 reading the logs in parallel and we don't know who is faster
	peerNodeIDs, err := n.membership.GetAllPeerNodeIDs()
	if err != nil {
		return err
	}
	cathupLogsForPeers, err := n.getLogsToCatchupForPeers(peerNodeIDs)
	if err != nil {
		return err
	}

	// task1: appends the command to the local as a new entry
	errorChanTask1 := make(chan error, 1)
	go func(ctx context.Context) {
		defer subTasksToWait.Done()
		commands := make([][]byte, len(clientCommands))
		for i, clientCommand := range clientCommands {
			commands[i] = clientCommand.Req.Command
		}
		errorChanTask1 <- n.raftLog.AppendLogsInBatch(ctx, commands, int(currentTerm))
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
	if !resp.Success {
		if resp.Term > currentTerm {
			n.leaderDegradationChan <- TermRank(resp.Term)
			return nil
		} else {
			// todo: the unsafe panic is temporarily used for debugging
			panic("failed append entries, but without not a higher term")
		}
	}

	// (4) the leader applies the command, and responds to the client
	newCommitID := n.raftLog.GetLastLogIdx()
	n.updateCommitIdx(newCommitID)

	// (5) apply the command
	// todo: shall refine the statemachine interface
	for _, clientCommand := range clientCommands {
		internalReq := clientCommand
		// todo: possibly, the statemachine shall has a unique ID for the command
		// todo: the apply command shall be async with apply and get result
		applyResp, err := n.statemachine.ApplyCommand(internalReq.Req.Command, newCommitID)
		n.incrementLastApplied(1)
		if err != nil {
			n.logger.Error("error in applying command to state machine", zap.Error(err))
			internalReq.RespChan <- &utils.RPCRespWrapper[*rpc.ClientCommandResponse]{
				Resp: nil,
				Err:  err,
			}
		} else {
			clientResp := &rpc.ClientCommandResponse{
				Result: applyResp,
			}
			internalReq.RespChan <- &utils.RPCRespWrapper[*rpc.ClientCommandResponse]{
				Resp: clientResp,
				Err:  nil,
			}
		}
	}
	// (5) if the follower run slowly or crash, the leader will retry to send the appendEntries indefinitely
	// this is send thru appendEntreis triggered by heartbeat or the client command
	return nil
}

// synchronous
func (n *Node) handleHeartbeat(ctx context.Context) error {
	ctx, requestID := common.GetOrGenerateRequestID(ctx)
	currentTerm := n.getCurrentTerm()
	peerNodeIDs, err := n.membership.GetAllPeerNodeIDs()
	if err != nil {
		return err
	}
	cathupLogsForPeers, err := n.getLogsToCatchupForPeers(peerNodeIDs)
	if err != nil {
		return err
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
	errChan := make(chan error, 1)
	respChan := make(chan *AppendEntriesConsensusResp, 1)

	go func(ctx context.Context) {
		ctxTimeout, _ := context.WithTimeout(ctx, n.cfg.GetRPCRequestTimeout())
		resp, err := n.ConsensusAppendEntries(ctxTimeout, reqs, n.CurrentTerm)
		errChan <- err
		respChan <- resp
	}(ctx)

	select {
	case <-ctx.Done():
		n.logger.Warn("raft node main context done, exiting", zap.String("requestID", requestID))
		return common.ContextDoneErr()
	case err := <-errChan:
		n.logger.Error(
			"error in sending append entries to one node", zap.Error(err), zap.String("requestID", requestID))
		return err
	case resp := <-respChan:
		if resp.Success {
			// maintain the index
			n.logger.Info("append entries success", zap.String("requestID", requestID))
			// todo: shall deliver the result
		} else {
			if resp.Term > currentTerm {
				n.logger.Warn("peer's term is greater than current term", zap.String("requestID", requestID))
				n.leaderDegradationChan <- TermRank(resp.Term)
			} else {
				// todo: the unsafe panic is temporarily used for debugging
				panic("failed append entries, but without not a higher term")
			}
		}
		return nil
	}
}

func (n *Node) closeClientCommandChan() {
	close(n.clientCommandChan)
	for request := range n.clientCommandChan {
		if request != nil {
			request.RespChan <- &utils.RPCRespWrapper[*rpc.ClientCommandResponse]{
				Resp: nil,
				Err:  errors.New("raft node is not in leader state"),
			}
		}
	}
}

func (n *Node) ClientCommand(req *utils.ClientCommandInternalReq) {
	n.clientCommandChan <- req
}
