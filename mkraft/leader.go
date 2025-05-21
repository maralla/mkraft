package mkraft

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/maki3cat/mkraft/common"
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

	--todo: not implemented yet

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

	// Append the current NodeID to a file called "state"
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
	// (1) major task-1: handle client commands and send append entries
	// (2) major task-2: send heartbeats to all the followers
	// (3) common task-1: handle voting requests from other candidates
	// (4) common task-2: handle append entries requests from other candidates

	n.clientCommandChan = make(chan *ClientCommandInternalReq, n.cfg.GetRaftNodeRequestBufferSize())
	n.leaderDegradationChan = make(chan TermRank, n.cfg.GetRaftNodeRequestBufferSize())

	// these channels cannot be closed because they are created in the main thread but used in the worker
	// we need to clean the channel when the leader quits,
	// and reset the channels when the leader comes back

	ctxForWorker, cancelWorker := context.WithCancel(ctx)
	defer cancelWorker()
	go n.leaderCommonTaskWorker(ctxForWorker)

	heartbeatDuration := n.cfg.GetLeaderHeartbeatPeriod()
	tickerForHeartbeat := time.NewTicker(heartbeatDuration)
	defer tickerForHeartbeat.Stop()
	go n.clientCommandsTaskWorker(ctxForWorker, tickerForHeartbeat)
	go n.resendSlowerFollowerWorker(ctxForWorker)

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
				n.SetVoteForAndTerm(n.VotedFor, uint32(newTerm))
				// shall first change the state, so that new client commands won't flood in when we close the channel
				n.closeClientCommandChan()
				go n.RunAsFollower(ctx)
				return
			case <-tickerForHeartbeat.C:
				heartbeatReq := &rpc.AppendEntriesRequest{
					Term:     n.CurrentTerm,
					LeaderId: n.NodeId,
				}
				go n.callAppendEntries(ctx, heartbeatReq)
			}
		}
	}
}

func (n *Node) resendSlowerFollowerWorker(ctx context.Context) {
	n.logger.Warn("resendSlowerFollowerWorker has not been implemented yet")
}

func (n *Node) leaderCommonTaskWorker(ctx context.Context) {
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
					resp := n.receiveVoteRequest(req)
					wrappedResp := &RPCRespWrapper[*rpc.RequestVoteResponse]{
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
				resp := n.receiveAppendEntires(internalReq.Req)
				wrapper := RPCRespWrapper[*rpc.AppendEntriesResponse]{
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

func (n *Node) clientCommandsTaskWorker(ctx context.Context, heartbeatTicker *time.Ticker) {
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
			case clientCmd := <-n.clientCommandChan:
				heartbeatTicker.Reset(n.cfg.GetLeaderHeartbeatPeriod())
				// one-goroutine handles this request in serialization,
				// but we use batching to improve performance
				// todo: batching is not implemented yet
				batchingSize := n.cfg.GetRaftNodeRequestBufferSize() - 1
				clientCommands := readMultipleFromChannel(n.clientCommandChan, batchingSize)
				clientCommands = append(clientCommands, clientCmd)
				n.handleClientCommand(ctx, clientCommands)
			}
		}
	}
}

// todo: shall add batching
// happy path: 1) the leader is alive and the followers are alive
// problem-1: the leader is alive but minority followers are dead -> can be handled by the retry mechanism
// problem-2: the leader is alive but majority followers are dead
// problem-3: the leader is stale
func (n *Node) handleClientCommand(ctx context.Context, clientCommands []*ClientCommandInternalReq) {

	var subTasksToWait sync.WaitGroup
	subTasksToWait.Add(2)
	errors := make(chan error, 2)

	// (1) appends the command to the local as a new entry
	// todo: how to get the response
	go func(ctx context.Context) {
		defer subTasksToWait.Done()
		commands := make([][]byte, len(clientCommands))
		for i, clientCommand := range clientCommands {
			commands[i] = clientCommand.Req.Command
		}
		errors <- n.raftLog.AppendLogsInBatch(ctx, commands, int(n.CurrentTerm))
	}(ctx)

	// (2) sends the command of appendEntries to all the followers in parallel to replicate the entry
	index, term := n.raftLog.GetLastLogIdxAndTerm()
	entries := make([]*rpc.LogEntry, len(clientCommands))
	for i, clientCommand := range clientCommands {
		entries[i] = &rpc.LogEntry{
			Data: clientCommand.Req.Command,
		}
	}
	req := &rpc.AppendEntriesRequest{
		Term:         n.CurrentTerm,
		LeaderId:     n.NodeId,
		PrevLogIndex: index,
		PrevLogTerm:  term,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	// todo: how to get the response
	// todo: how to maintain the node state?
	go func(ctx context.Context) {
		defer subTasksToWait.Done()
		err := n.callAppendEntries(ctx, req)
		errors <- err
	}(ctx)

	// (3) when the entry has been safely replicated, the leader applies the entry to the state machine
	subTasksToWait.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			n.logger.Error("error in appending log to raft log", zap.Error(err))
			// todo: maki, the paper doesn't mention how to handle this error, so we handle happy path only
			panic("I don't know how to handle this error")
		}
	}

	// (4) the leader applies the command, and responds to the client
	newCommitID := req.PrevLogIndex + uint64(len(req.Entries))
	n.updateCommitIdx(newCommitID)

	// (5) apply the command
	for idx, clientCommand := range clientCommands {
		count := idx + 1
		internalReq := clientCommand
		// todo: possibly, the statemachine shall has a unique ID for the command
		// todo: the apply command shall be async with apply and get result
		applyResp, err := n.statemachine.ApplyCommand(internalReq.Req.Command, newCommitID)
		n.updateLastAppliedIdx(req.PrevLogIndex + uint64(count))
		if err != nil {
			n.logger.Error("error in applying command to state machine", zap.Error(err))
			internalReq.RespChan <- &RPCRespWrapper[*rpc.ClientCommandResponse]{
				Resp: nil,
				Err:  err,
			}
		} else {
			clientResp := &rpc.ClientCommandResponse{
				Result: applyResp,
			}
			internalReq.RespChan <- &RPCRespWrapper[*rpc.ClientCommandResponse]{
				Resp: clientResp,
				Err:  nil,
			}
		}
	}

	// (5) if the follower run slowly or crash, the leader will retry to send the appendEntries indefinitely
	// todo: how to do get the slower follower and resend the req?
	// can start a goroutine to send the appendEntries to the slower follower

	// todo: how to maintain the server index
}

// synchronous, should be called in a goroutine
// todo: (1) the leader shall send the appendEntries from the each peer's nextIndex, so the logs are not the same for each peer
// todo: (2) on response, the leader shall update the nextIndex and matchIndex for the follower
// todo: (3) the leader election and inconsistency logic is not implemented yet
func (n *Node) callAppendEntriesV2(ctx context.Context, req *rpc.AppendEntriesRequest) error {
	// todo: to be implemented
	peerNodeIDtoClient, err := n.membership.GetAllPeerClientsV2()
	if err != nil {
		n.logger.Error("failed to get all peer clients", zap.Error(err))
	}
	var reqForEachPeer map[string]*rpc.AppendEntriesRequest
	for nodeID, _ := range peerNodeIDtoClient {
		nextID := n.getPeersNextIndex(nodeID)
		logs, err := n.raftLog.GetLogsFromIdx(nextID)
		if err != nil {
			n.logger.Error("failed to get logs from index", zap.Error(err))
			return err
		}
		prevLogIndex := nextID - 1
		prevLogTerm, err := n.raftLog.GetTermByIndex(prevLogIndex)
		if err != nil {
			n.logger.Error("failed to get term by index", zap.Error(err))
		}
		copiedReq := &rpc.AppendEntriesRequest{
			Term:         req.Term,
			LeaderId:     req.LeaderId,
			LeaderCommit: req.LeaderCommit,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
		}
		copiedReq.Entries = make([]*rpc.LogEntry, len(logs))
		for i, log := range logs {
			copiedReq.Entries[i] = &rpc.LogEntry{
				Data: log.Commands,
			}
		}
		reqForEachPeer[nodeID] = copiedReq
	}
	n.consensus.AppendEntriesSendForConsensusV2(ctx, req)
	return nil
}

// synchronous, should be called in a goroutine
// todo: (1) the leader shall send the appendEntries from the each peer's nextIndex, so the logs are not the same for each peer
// todo: (2) on response, the leader shall update the nextIndex and matchIndex for the follower
// todo: (3) the leader election and inconsistency logic is not implemented yet
func (n *Node) callAppendEntries(ctx context.Context, req *rpc.AppendEntriesRequest) error {

	ctx, requestID := common.GetOrGenerateRequestID(ctx)
	errChan := make(chan error, 1)
	respChan := make(chan *AppendEntriesConsensusResp, 1)
	go func(ctx context.Context) {
		ctxTimeout, _ := context.WithTimeout(ctx, n.cfg.GetRPCRequestTimeout())
		// since normally we don't wait for the stragglers,
		// if we call cancel, there will be errors in the clients to the stragglers
		consensusResp, err := n.consensus.AppendEntriesSendForConsensus(ctxTimeout, req)
		if err != nil {
			errChan <- err
		} else {
			respChan <- consensusResp
		}
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
			if len(req.Entries) != 0 {

			}
			n.logger.Info("append entries success", zap.String("requestID", requestID))
			// todo: shall deliver the result
		} else {
			n.leaderDegradationChan <- TermRank(resp.Term)
			n.logger.Warn("append entries failed", zap.String("requestID", requestID))
		}
		return nil
	}

}

func (n *Node) closeClientCommandChan() {
	close(n.clientCommandChan)
	for request := range n.clientCommandChan {
		if request != nil {
			request.RespChan <- &RPCRespWrapper[*rpc.ClientCommandResponse]{
				Resp: nil,
				Err:  errors.New("raft node is not in leader state"),
			}
		}
	}
}

func (n *Node) ClientCommand(req *ClientCommandInternalReq) {
	n.clientCommandChan <- req
}
