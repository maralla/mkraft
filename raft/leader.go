package raft

import (
	"context"
	"errors"
	"os"
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

	n.clientCommandChan = make(chan *ClientCommandInternal, n.cfg.GetRaftNodeRequestBufferSize())
	n.leaderDegradationChan = make(chan TermRank, n.cfg.GetRaftNodeRequestBufferSize())

	// these channels cannot be closed because they are created in the main thread but used in the worker
	// we need to clean the channel when the leader quits,
	// and reset the channels when the leader comes back

	heartbeatDuration := n.cfg.GetLeaderHeartbeatPeriod()
	tickerForHeartbeat := time.NewTicker(heartbeatDuration)
	defer tickerForHeartbeat.Stop()

	ctxForWorker, cancelWorker := context.WithCancel(ctx)
	go n.leaderCommonTaskWorker(ctxForWorker)
	defer cancelWorker()

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
				n.SetVoteForAndTerm(n.VotedFor, int32(newTerm))
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
			case clientCmdReq := <-n.clientCommandChan:
				// todo: the client command request, should go into the callAppendEntries to handle
				// todo: we omit the client command for now
				tickerForHeartbeat.Stop()
				log := &rpc.LogEntry{
					Data: clientCmdReq.request,
				}
				appendEntryReq := &rpc.AppendEntriesRequest{
					Term:     n.CurrentTerm,
					LeaderId: n.NodeId,
					Entries:  []*rpc.LogEntry{log},
				}
				go n.callAppendEntries(ctx, appendEntryReq)
				tickerForHeartbeat.Reset(heartbeatDuration)
			}
		}
	}
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
			case requestVote := <-n.requestVoteChan: // commonRule: same with candidate
				if !requestVote.IsTimeout.Load() {
					// no-IO operation
					req := requestVote.Request
					resChan := requestVote.RespWraper
					resp := n.receiveVoteRequest(req)
					wrapper := &rpc.RPCRespWrapper[*rpc.RequestVoteResponse]{
						Resp: resp,
						Err:  nil,
					}
					resChan <- wrapper
					if resp.VoteGranted {
						n.leaderDegradationChan <- TermRank(resp.Term)
						return
					}
				}
			case req := <-n.appendEntryChan: // commonRule: same with candidate
				// todo: shall add appendEntry operations which shall be a goroutine
				resp := n.receiveAppendEntires(req.Request)
				wrapper := rpc.RPCRespWrapper[*rpc.AppendEntriesResponse]{
					Resp: resp,
					Err:  nil,
				}
				req.RespWraper <- &wrapper
				if resp.Success {
					n.leaderDegradationChan <- TermRank(resp.Term)
					return
				}
			}
		}
	}
}

// synchronous, should be called in a goroutine
// todo: suppose we don't need response now
func (n *Node) callAppendEntries(ctx context.Context, req *rpc.AppendEntriesRequest) {
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
	case err := <-errChan:
		n.logger.Error(
			"error in sending append entries to one node", zap.Error(err), zap.String("requestID", requestID))
	case resp := <-respChan:
		if resp.Success {
			n.logger.Info("append entries success", zap.String("requestID", requestID))
			// todo: shall deliver the result
		} else {
			n.leaderDegradationChan <- TermRank(resp.Term)
			n.logger.Warn("append entries failed", zap.String("requestID", requestID))
		}
	}
}

func (n *Node) closeClientCommandChan() {
	close(n.clientCommandChan)
	for request := range n.clientCommandChan {
		if request != nil {
			request.errChan <- errors.New("raft node is not in leader state")
		}
	}
}
