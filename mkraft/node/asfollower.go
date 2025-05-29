package node

import (
	"context"
	"time"

	"github.com/maki3cat/mkraft/common"
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
					electionTicker.Stop()
					resp := n.handlerAppendEntriesAsFollower(ctx, req.Req)
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

func (n *Node) handlerAppendEntriesAsFollower(ctx context.Context, req *rpc.AppendEntriesRequest) *rpc.AppendEntriesResponse {
	defer n.updateCommitIdx(req.LeaderCommit)
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

	// update the term
	if reqTerm > currentTerm {
		err := n.storeCurrentTermAndVotedFor(reqTerm, "") // did not vote for anyone
		if err != nil {
			n.logger.Error(
				"error in storeCurrentTermAndVotedFor", zap.Error(err),
				zap.String("nId", n.NodeId))
			panic(err) // todo: critical error, cannot continue, not sure how to handle this
		}
	}

	// append logs
	logs := make([][]byte, len(req.Entries))
	for idx, entry := range req.Entries {
		logs[idx] = entry.Data
	}
	err := n.raftLog.AppendLogsInBatchWithCheck(ctx, req.PrevLogIndex, logs, req.Term)
	if err != nil {
		if err == common.ErrPreLogNotMatch {
			response = rpc.AppendEntriesResponse{
				Term:    currentTerm,
				Success: false,
			}
			return &response
		} else {
			panic(err) // todo: critical error, cannot continue, not sure how to handle this
		}
	} else {
		response = rpc.AppendEntriesResponse{
			Term:    currentTerm,
			Success: true,
		}
		// update the commit index
		return &response
	}

}
