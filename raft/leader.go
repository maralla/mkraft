package raft

import (
	"context"
	"errors"
	"time"

	"github.com/maki3cat/mkraft/rpc"
	"github.com/maki3cat/mkraft/util"
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

SECTION2: SPECIFICAL RULE FOR LEADERS (paper)
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
func (node *Node) RunAsLeader(ctx context.Context) {

	if node.State != StateLeader {
		panic("node is not in LEADER state")
	}
	logger.Info("acquiring the Semaphore as the LEADER state")
	node.sem.Acquire(ctx, 1)
	logger.Info("acquired the Semaphore as the LEADER state")
	defer node.sem.Release(1)

	// leader:
	// (1) major task-1: handle client commands and send append entries
	// (2) major task-2: send heartbeats to all the followers
	// (3) common task-1: handle voting requests from other candidates
	// (4) common task-2: handle append entries requests from other candidates

	node.clientCommandChan = make(chan *ClientCommandInternal, util.GetConfig().LeaderBufferSize)
	node.leaderDegradationChan = make(chan TermRank, util.GetConfig().RaftNodeRequestBufferSize)
	// these channels cannot be closed because they are created in the main thread but used in the worker
	// we need to clean the channel when the leader quits,
	// and reset the channels when the leader comes back

	heartbeatDuration := util.GetConfig().GetLeaderHeartbeatPeriod()
	tickerForHeartbeat := time.NewTicker(heartbeatDuration)
	defer tickerForHeartbeat.Stop()

	ctxForWorker, cancelWorker := context.WithCancel(ctx)
	go node.leaderCommonTaskWorker(ctxForWorker)
	defer cancelWorker()

	for {
		select {
		case <-ctx.Done(): // give ctx higher priority
			logger.Warn("raft node main context done, exiting")
			node.GracefulShutdown(ctx)
			return
		default:
			select {
			case <-ctx.Done():
				logger.Warn("raft node main context done, exiting")
				node.GracefulShutdown(ctx)
				return
			case newTerm := <-node.leaderDegradationChan:
				node.State = StateFollower
				node.SetVoteForAndTerm(node.VotedFor, int32(newTerm))
				// shall first change the state, so that new client commands won't flood in when we close the channel
				node.closeClientCommandChan(ctx)
				go node.RunAsFollower(ctx)
				return
			case <-tickerForHeartbeat.C:
				heartbeatReq := &rpc.AppendEntriesRequest{
					Term:     node.CurrentTerm,
					LeaderId: node.NodeId,
				}
				go node.callAppendEntries(ctx, heartbeatReq)
			case clientCmdReq := <-node.clientCommandChan:
				// todo: the client command request, should go into the callAppendEntries to handle
				// todo: we omit the client command for now
				tickerForHeartbeat.Stop()
				log := &rpc.LogEntry{
					Data: clientCmdReq.request,
				}
				appendEntryReq := &rpc.AppendEntriesRequest{
					Term:     node.CurrentTerm,
					LeaderId: node.NodeId,
					Entries:  []*rpc.LogEntry{log},
				}
				go node.callAppendEntries(ctx, appendEntryReq)
				tickerForHeartbeat.Reset(heartbeatDuration)
			}
		}
	}
}

func (node *Node) leaderCommonTaskWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			logger.Warn("leader's worker context done, exiting")
			return
		default:
			select {
			case <-ctx.Done():
				logger.Warn("leader's worker context done, exiting")
				return
			case requestVote := <-node.requestVoteChan: // commonRule: same with candidate
				if !requestVote.IsTimeout.Load() {
					// no-IO operation
					req := requestVote.Request
					resChan := requestVote.RespWraper
					resp := node.voting(req)
					wrapper := &rpc.RPCRespWrapper[*rpc.RequestVoteResponse]{
						Resp: resp,
						Err:  nil,
					}
					resChan <- wrapper
					if resp.VoteGranted {
						node.leaderDegradationChan <- TermRank(resp.Term)
						return
					}
				}
			case req := <-node.appendEntryChan: // commonRule: same with candidate
				// todo: shall add appendEntry operations which shall be a goroutine
				resp := node.appendEntries(req.Request)
				wrapper := rpc.RPCRespWrapper[*rpc.AppendEntriesResponse]{
					Resp: resp,
					Err:  nil,
				}
				req.RespWraper <- &wrapper
				if resp.Success {
					node.leaderDegradationChan <- TermRank(resp.Term)
					return
				}
			}
		}
	}
}

// synchronous, should be called in a goroutine
// todo: suppose we don't need response now
func (node *Node) callAppendEntries(ctx context.Context, req *rpc.AppendEntriesRequest) {
	errChan := make(chan error, 1)
	respChan := make(chan *AppendEntriesConsensusResp, 1)
	go func(ctx context.Context) {
		ctxTimeout, cancel := context.WithTimeout(ctx, util.GetConfig().GetRPCRequestTimeout())
		defer cancel()
		consensusResp, err := AppendEntriesSendForConsensus(ctxTimeout, req)
		if err != nil {
			errChan <- err
		} else {
			respChan <- consensusResp
		}
	}(ctx)

	select {
	case <-ctx.Done():
		logger.Warn("raft node main context done, exiting")
	case err := <-errChan:
		logger.Error("error in sending append entries to one node", err)
	case resp := <-respChan:
		if resp.Success {
			logger.Info("append entries success")
			// todo: shall deliver the result
		} else {
			node.leaderDegradationChan <- TermRank(resp.Term)
			logger.Warn("append entries failed")
		}
	}
}

func (node *Node) closeClientCommandChan(ctx context.Context) {
	close(node.clientCommandChan)
	for request := range node.clientCommandChan {
		if request != nil {
			request.errChan <- errors.New("raft node is not in leader state")
		}
	}
}
