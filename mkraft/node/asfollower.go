package node

import (
	"context"
	"time"

	"github.com/maki3cat/mkraft/mkraft/utils"
	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
)

// todo: add handle appendEntries from a leader which may delete/overwrite logs
/*
PAPER:

Follower are passive and they don't initiate any requests,
and only respond to requests from candidates and leaders.

If times out, the follower will convert to candidate state.
*/
func (n *Node) RunAsFollower(ctx context.Context) {

	if n.State != StateFollower {
		panic("node is not in FOLLOWER state")
	}

	n.logger.Info("node acquires to run in FOLLOWER state")
	n.sem.Acquire(ctx, 1)
	n.logger.Info("acquired semaphore in FOLLOWER state")
	defer n.sem.Release(1)

	electionTicker := time.NewTicker(n.cfg.GetElectionTimeout())
	defer electionTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			n.logger.Warn("raft node's main context done, exiting")
			return
		default:
			{
				select {
				case <-ctx.Done():
					n.logger.Warn("context done")
					n.membership.GracefulShutdown()
					return
				case <-electionTicker.C:
					n.State = StateCandidate
					go n.RunAsCandidate(ctx)
					return
				case requestVoteInternal := <-n.requestVoteChan:
					if requestVoteInternal.IsTimeout.Load() {
						n.logger.Warn("request vote is timeout")
						continue
					}
					electionTicker.Stop()
					// for the follower, the node state has no reason to change because of the request
					resp := n.handleVoteRequest(requestVoteInternal.Req)
					wrappedResp := utils.RPCRespWrapper[*rpc.RequestVoteResponse]{
						Resp: resp,
						Err:  nil,
					}
					requestVoteInternal.RespChan <- &wrappedResp
					electionTicker.Reset(n.cfg.GetElectionTimeout())
				case req := <-n.appendEntryChan:
					// todo: to maintain logs and index and state machine of the follower
					electionTicker.Stop()
					// for the follower, the node state has no reason to change because of the request
					resp := n.handlerAppendEntriesAsFollower(req.Req)
					wrappedResp := utils.RPCRespWrapper[*rpc.AppendEntriesResponse]{
						Resp: resp,
						Err:  nil,
					}
					req.RespChan <- &wrappedResp
					electionTicker.Reset(n.cfg.GetElectionTimeout())
				}
			}
		}
	}
}

func (n *Node) handlerAppendEntriesAsFollower(req *rpc.AppendEntriesRequest) *rpc.AppendEntriesResponse {
	var response rpc.AppendEntriesResponse
	reqTerm := uint32(req.Term)
	currentTerm := n.getCurrentTerm()
	if reqTerm < currentTerm {
		response = rpc.AppendEntriesResponse{
			Term:    currentTerm,
			Success: false,
		}
		return &response
	}

	if n.raftLog.CheckPreLog(req.PrevLogIndex, req.PrevLogTerm) == false {
		response = rpc.AppendEntriesResponse{
			Term:    currentTerm,
			Success: false,
		}
		return &response
	}

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
	} else {
		// should accecpet it directly?
		response = rpc.AppendEntriesResponse{
			Term:    currentTerm,
			Success: true,
		}
	}
	return &response
}
