package raft

import (
	"context"
	"time"

	util "github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/rpc"
)

// todo, maki: shall check if we need priority of a speicifc channel in our select
/*
PAPER (quote):
Shared Rule: if any RPC request or response is received from a server with a higher term,
convert to follower
How the Shared Rule works for Followers:
(1) handle request of AppendEntriesRPC initiated by another server
(2) handle reuqest of RequestVoteRPC initiated by another server

Speicial Rules for Followers
Respond to RPCs from candidates and leaders
If election timeout elapses without receiving AppendEntries RPC from current leader
or granting vote to candidate: convert to candidate
*/
func (node *Node) RunAsFollower(ctx context.Context) {

	if node.State != StateFollower {
		panic("node is not in FOLLOWER state")
	}

	logger.Info("node acquires to run in FOLLOWER state")
	node.sem.Acquire(ctx, 1)
	logger.Info("acquired semaphore in FOLLOWER state")
	defer node.sem.Release(1)

	electionTicker := time.NewTicker(util.GetConfig().GetElectionTimeout())
	defer electionTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Warn("raft node's main context done, exiting")
			node.GracefulShutdown(ctx)
			return
		default:
			{
				select {
				case <-ctx.Done():
					logger.Warn("context done")
					memberMgr.GracefulShutdown()
					return
				case <-electionTicker.C:
					node.State = StateCandidate
					go node.RunAsCandidate(ctx)
					return
				case requestVoteInternal := <-node.requestVoteChan:
					if requestVoteInternal.IsTimeout.Load() {
						logger.Warn("request vote is timeout")
						continue
					}
					electionTicker.Stop()
					// for the follower, the node state has no reason to change because of the request
					resp := node.receiveVoteRequest(requestVoteInternal.Request)
					wrapper := &rpc.RPCRespWrapper[*rpc.RequestVoteResponse]{
						Resp: resp,
						Err:  nil,
					}
					requestVoteInternal.RespWraper <- wrapper
					electionTicker.Reset(util.GetConfig().GetElectionTimeout())
				case req := <-node.appendEntryChan:
					electionTicker.Stop()
					// for the follower, the node state has no reason to change because of the request
					resp := node.receiveAppendEntires(req.Request)
					wrappedResp := &rpc.RPCRespWrapper[*rpc.AppendEntriesResponse]{
						Resp: resp,
						Err:  nil,
					}
					req.RespWraper <- wrappedResp
					electionTicker.Reset(util.GetConfig().GetElectionTimeout())
				}
			}
		}
	}
}

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
func (node *Node) RunAsCandidate(ctx context.Context) {
	if node.State != StateCandidate {
		panic("node is not in CANDIDATE state")
	}

	logger.Info("node starts to acquiring CANDIDATE state")
	node.sem.Acquire(ctx, 1)
	logger.Info("node has acquired semaphore in CANDIDATE state")
	defer node.sem.Release(1)

	consensusChan := node.runOneElection(ctx)

	ticker := time.NewTicker(util.GetConfig().GetElectionTimeout())
	for {
		select {
		case <-ctx.Done():
			logger.Warn("raft node's main context done, exiting")
			node.GracefulShutdown(ctx)
			return
		default:
			{
				select {
				case <-ctx.Done():
					logger.Warn("raft node main context done")
					memberMgr.GracefulShutdown()
					return
				case response := <-consensusChan: // some response from last election
					// I don't think we need to reset the ticker here
					// voteCancel() // cancel the rest
					if response.VoteGranted {
						node.State = StateLeader
						go node.RunAsLeader(ctx)
						return
					} else {
						if response.Term > node.CurrentTerm {
							// some one has become a leader
							// voteCancel()
							node.CurrentTerm = response.Term
							node.ResetVoteFor()
							node.State = StateFollower
							go node.RunAsFollower(ctx)
							return
						} else {
							logger.Infof(
								"not enough votes, re-elect again, current term: %d, candidateID: %d",
								node.CurrentTerm, node.NodeId,
							)
						}
					}
				case <-ticker.C: // last election timeout withno response
					// voteCancel()
					consensusChan = node.runOneElection(ctx)
				case requestVoteInternal := <-node.requestVoteChan: // commonRule: handling voteRequest from another candidate
					if requestVoteInternal.IsTimeout.Load() {
						logger.Warn("request vote is timeout")
						continue
					}
					req := requestVoteInternal.Request
					resChan := requestVoteInternal.RespWraper
					resp := node.receiveVoteRequest(req)
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
				case req := <-node.appendEntryChan: // commonRule: handling appendEntry from a leader which can be stale or new
					resp := node.receiveAppendEntires(req.Request)
					wrappedResp := &rpc.RPCRespWrapper[*rpc.AppendEntriesResponse]{
						Resp: resp,
						Err:  nil,
					}
					req.RespWraper <- wrappedResp
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
}
