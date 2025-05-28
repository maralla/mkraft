package node

import (
	"context"
	"time"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/utils"
	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
)

/*
PAPER (quote):
Shared Rule: if any RPC request or response is received from a server with a higher term,
convert to follower
How the Shared Rule works for Candidates:
(1) handle the response of RequestVoteRPC initiated by itself
(2) handle request of AppendEntriesRPC initiated by another server
(3) handle reuqest of RequestVoteRPC initiated by another server

Specifical Rule for Candidates:
On conversion to a candidate, start election:
(1) increment currentTerm
(2) vote for self
(3) send RequestVote RPCs to all other servers
if votes received from majority of servers: become leader
if AppendEntries RPC received from new leader: convert to follower
if election timeout elapses: start new election
*/
func (n *Node) RunAsCandidate(ctx context.Context) {
	if n.State != StateCandidate {
		panic("node is not in CANDIDATE state")
	}

	n.logger.Info("node starts to acquiring CANDIDATE state")
	n.sem.Acquire(ctx, 1)
	n.logger.Info("node has acquired semaphore in CANDIDATE state")
	defer n.sem.Release(1)

	consensusChan := n.runOneElection(ctx)

	ticker := time.NewTicker(n.cfg.GetElectionTimeout())
	for {
		currentTerm := n.getCurrentTerm()
		select {
		case <-ctx.Done():
			n.logger.Warn("raft node's main context done, exiting")
			return
		default:
			{
				select {
				case <-ctx.Done():
					n.logger.Warn("raft node main context done")
					n.membership.GracefulShutdown()
					return
				case response := <-consensusChan: // some response from last election
					// I don't think we need to reset the ticker here
					// voteCancel() // cancel the rest
					if response.VoteGranted {
						n.State = StateLeader
						go n.RunAsLeader(ctx)
						return
					} else {
						if response.Term > currentTerm {
							// some one has become a leader
							// voteCancel()
							err := n.storeCurrentTermAndVotedFor(response.Term, "") // did not vote for anyone
							if err != nil {
								n.logger.Error(
									"error in storeCurrentTermAndVotedFor", zap.Error(err),
									zap.String("nId", n.NodeId))
								panic(err) // critical error, cannot continue
							}
							n.State = StateFollower
							go n.RunAsFollower(ctx)
							return
						} else {
							n.logger.Info(
								"not enough votes, re-elect again",
								zap.Int("term", int(currentTerm)), zap.String("nId", n.NodeId))
						}
					}
				case <-ticker.C: // last election timeout withno response
					// voteCancel()
					consensusChan = n.runOneElection(ctx)
				case requestVoteInternal := <-n.requestVoteChan: // commonRule: handling voteRequest from another candidate
					if requestVoteInternal.IsTimeout.Load() {
						n.logger.Warn("request vote is timeout")
						continue
					}
					req := requestVoteInternal.Req
					resChan := requestVoteInternal.RespChan
					resp := n.handleVoteRequest(req)
					wrappedResp := utils.RPCRespWrapper[*rpc.RequestVoteResponse]{
						Resp: resp,
						Err:  nil,
					}
					resChan <- &wrappedResp
					// this means other candiate has a higher term
					if resp.VoteGranted {
						n.State = StateFollower
						go n.RunAsFollower(ctx)
						return
					}
				case req := <-n.appendEntryChan: // commonRule: handling appendEntry from a leader which can be stale or new
					resp := n.handlerAppendEntries(req.Req)
					wrappedResp := utils.RPCRespWrapper[*rpc.AppendEntriesResponse]{
						Resp: resp,
						Err:  nil,
					}
					req.RespChan <- &wrappedResp
					if resp.Success {
						// this means there is a leader there
						n.State = StateFollower
						go n.RunAsFollower(ctx)
						return
					}
				}
			}
		}
	}
}

func (node *Node) runOneElection(ctx context.Context) chan *MajorityRequestVoteResp {
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
