package raft

import (
	"context"
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
	sugarLogger.Info("acquiring the Semaphore as the LEADER state")
	node.sem.Acquire(ctx, 1)
	sugarLogger.Info("acquired the Semaphore as the LEADER state")
	defer node.sem.Release(1)

	// THE COMPLEXITY OF THE LEADER IS MUCH HIGHER THAN THE CANDIDATE
	// unlike the candidate which issues one request at a time
	// the leader is more complex
	// (1)
	// because it shall send multiple AppendEntries requests at the same time
	// and if any of them gets a response with a higher term
	// the leader shall become a follower
	// (2) (3)
	// at the same time, it shall send heartbeats to all the followers
	// at the same time, it shall handle the voting requests from other candidates
	// (4)
	// the leader itself shall be gracefully closed

	// maki: it is designed to be RENDEZVOUS
	// so that we don't handle more than one possible degradation
	degradationChan := make(chan TermRank)
	defer close(degradationChan)
	tickerForHeartbeat := time.NewTicker(util.GetConfig().GetLeaderHeartbeatPeriod())
	defer tickerForHeartbeat.Stop()

	// respReaderCtx, respCancel := context.WithCancel(ctx)
	// defer respCancel()
	// appendEntriesRespChan := make(chan *MajorityAppendEntriesResp, util.GetConfig().LeaderBufferSize)
	// defer close(appendEntriesRespChan) // todo: gorouting writing to it may panic the entire program

	// go node.workerForLeader(respReaderCtx, appendEntriesRespChan, degradationChan)

	// // this timeout is one consensus timeout, the internal should be one rpc request timeout
	for {
		select {
		case <-ctx.Done(): // give ctx higher priority
			sugarLogger.Warn("raft node main context done, exiting")
			node.GracefulShutdown(ctx)
			return
		default:
			select {
			case <-ctx.Done():
				sugarLogger.Warn("raft node main context done, exiting")
				node.GracefulShutdown(ctx)
				return
			case newTerm := <-degradationChan:
				node.SetVoteForAndTerm(node.VotedFor, int32(newTerm))

				node.leaderStopWorkers(ctx)
				node.rejectAllClientRequests(ctx)

				node.State = StateFollower
				go node.RunAsFollower(ctx)
				return
			case <-tickerForHeartbeat.C:
				heartbeatReq := &rpc.AppendEntriesRequest{
					Term:     node.CurrentTerm,
					LeaderId: node.NodeId,
				}
				go callAppendEntries(heartbeatReq)

			case clientCmdReq := <-node.clientCommandChan:
				// todo: should add rate limit the client command
				// todo: add batching to appendEntries when we do logging
				// todo: we don't handle single client command
				tickerForHeartbeat.Stop()
				log := &rpc.LogEntry{
					Data: clientCmdReq.request,
				}
				appendEntryReq := &rpc.AppendEntriesRequest{
					Term:     node.CurrentTerm,
					LeaderId: node.NodeId,
					Entries:  []*rpc.LogEntry{log},
				}
				go callAppendEntries(appendEntryReq)
				tickerForHeartbeat.Reset(heartbeatDuration)

			case requestVote := <-node.requestVoteChan: // commonRule: same with candidate
				if !requestVote.IsTimeout.Load() {
					req := requestVote.Request
					resChan := requestVote.RespWraper
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
				}
			case req := <-node.appendEntryChan: // commonRule: same with candidate
				resp := node.appendEntries(req.Request)
				wrapper := rpc.RPCRespWrapper[*rpc.AppendEntriesResponse]{
					Resp: resp,
					Err:  nil,
				}
				req.RespWraper <- &wrapper
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
}

func (node *Node) callAppendEntries(ctx context.Context, req *rpc.AppendEntriesRequest) {

	errChan := make(chan error, 1)
	respChan := make(chan *AppendEntriesConsensusResp, 1)
	call := func(ctx context.Context) {
		ctxTimeout, cancel := context.WithTimeout(ctx, util.GetConfig().GetRPCRequestTimeout())
		defer cancel()
		consensusResp, err := AppendEntriesSendForConsensus(ctxTimeout, req, respChan)
		if err != nil {
			errChan <- err
		}else{
			respChan <- consensusResp
		}
	}
	call(ctx)

	select {
	case <-ctx.Done():
		sugarLogger.Warn("raft node main context done, exiting")
	case err := <-errChan:
		sugarLogger.Error("error in sending append entries to one node", err)
	case resp := <-respChan:
		if resp.Success {
			sugarLogger.Info("append entries success")
		} else {
			sugarLogger.Warn("append entries failed")
		}
	

}

func (node *Node) GracefulShutdown(ctx context.Context) {
	// todo: to add more
	if node.State == StateLeader {
		sugarLogger.Info("leader is shutting down")
		node.leaderStopWorkers(ctx)
	}

	// same to all
	memberMgr.GracefulShutdown()
}

// three responsibilities for the workers:
// (1) handle appendEntries from other possible leaders
// (2) handle requestVote from other possible leaders
// (3) handle client command and send appendEntries from others
// and we use 2 workers for these
// the workers respond to the main for the following messages:
// (1) the main shall quit as leader and degrade into a follower and gracefully shutdown all the workers
// (2) the main shall tune the ticker when the appendEntries is already sent
func (node *Node) handlerPeers(ctx context.Context) {
	// if some new leader send this, the current node shall become a follower
	panic("not implemented yet")
}

func (node *Node) handleClient(ctx context.Context) {
	// if some new leader send this, the current node shall become a follower
	panic("not implemented yet")
}

func (node *Node) leaderStopWorkers(ctx context.Context) {
	// if some new leader send this, the current node shall become a follower
	panic("not implemented yet")
}

func (node *Node) rejectAllClientRequests(ctx context.Context) {
	// if some new leader send this, the current node shall become a follower
	panic("not implemented yet")
}

func (node *Node) workerForLeader(
	ctx context.Context, appendEntriesRespChan chan *MajorityAppendEntriesResp,
	leaderRecedeToFollowerChan chan TermRank,
) {
	for {
		select {
		case <-ctx.Done():
			sugarLogger.Warn("raft node main context done")
			memberMgr.GracefulShutdown()
			return
		case response := <-appendEntriesRespChan:
			// BATCHING, BATCHING takes all in buffer now, we'll see if we need a batch-size config
			remainingSize := len(appendEntriesRespChan)
			responseBatch := make([]*MajorityAppendEntriesResp, remainingSize+1)
			responseBatch[0] = response
			for i := range remainingSize {
				responseBatch[i] = <-appendEntriesRespChan
			}

			highestTerm := node.CurrentTerm
			for _, resp := range responseBatch {
				if resp.Term > node.CurrentTerm {
					highestTerm = max(resp.Term, highestTerm)
				}
			}
			if highestTerm > node.CurrentTerm {
				sugarLogger.Warn("term is greater than current term")
				leaderRecedeToFollowerChan <- TermRank(highestTerm)
				return
			} else {
				// calculate the committed index
				// todo: not impletmented yet
			}
		}
	}
}
