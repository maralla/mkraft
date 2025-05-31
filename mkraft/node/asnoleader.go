package node

import (
	"context"
	"sync"
	"time"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/utils"
	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
)

/*
PAPER:

Follower are passive and they don't initiate any requests,
and only respond to requests from candidates and leaders.

If times out, the follower will convert to candidate state.
*/
func (n *Node) RunAsFollower(ctx context.Context) {

	if n.GetNodeState() != StateFollower {
		panic("node is not in FOLLOWER state")
	}
	n.logger.Info("node acquires to run in FOLLOWER state")
	n.sem.Acquire(ctx, 1)
	n.logger.Info("acquired semaphore in FOLLOWER state")

	workerCtx, workerCancel := context.WithCancel(ctx)
	workerWaitGroup := sync.WaitGroup{}
	workerWaitGroup.Add(1)
	electionTicker := time.NewTicker(n.cfg.GetElectionTimeout())
	n.noleaderApplySignalChan = make(chan bool, n.cfg.GetRaftNodeRequestBufferSize())
	go n.noleaderWorkerToApplyLogs(workerCtx, &workerWaitGroup)
	defer func() { // gracefully exit for follower state is easy
		electionTicker.Stop()
		workerCancel()
		workerWaitGroup.Wait() // cancel only closes the Done channel, it doesn't wait for the worker to exit
		n.logger.Info("follower worker exited successfully")
		n.sem.Release(1)
	}()

	for {
		select {
		case <-ctx.Done():
			n.logger.Warn("raft node's main context done, exiting")
			n.membership.GracefulShutdown() // this cannot be in defer
			return
		default:
			{
				select {
				case <-ctx.Done():
					n.logger.Warn("raft node's main context done, exiting")
					n.membership.GracefulShutdown() // this cannot be in defer
					return
				case <-electionTicker.C:
					n.SetNodeState(StateCandidate)
					go n.RunAsCandidate(ctx)
					return
				case requestVoteInternal := <-n.requestVoteChan:
					electionTicker.Reset(n.cfg.GetElectionTimeout())
					if requestVoteInternal.IsTimeout.Load() {
						n.logger.Warn("request vote is timeout")
						continue
					}
					resp := n.handleVoteRequest(requestVoteInternal.Req)
					wrappedResp := utils.RPCRespWrapper[*rpc.RequestVoteResponse]{
						Resp: resp,
						Err:  nil,
					}
					requestVoteInternal.RespChan <- &wrappedResp
				case appendEntryInternal := <-n.appendEntryChan:
					if appendEntryInternal.Req.Term >= n.getCurrentTerm() {
						electionTicker.Reset(n.cfg.GetElectionTimeout())
					}
					if appendEntryInternal.IsTimeout.Load() {
						n.logger.Warn("append entry is timeout")
						continue
					}
					resp := n.noleaderHandleAppendEntries(ctx, appendEntryInternal.Req)
					wrappedResp := utils.RPCRespWrapper[*rpc.AppendEntriesResponse]{
						Resp: resp,
						Err:  nil,
					}
					appendEntryInternal.RespChan <- &wrappedResp
				case clientCommandInternal := <-n.receiveClientCommandChan:
					// todo: add delegation to the leader
					if clientCommandInternal.IsTimeout.Load() {
						n.logger.Warn("client command is timeout")
						continue
					}
					n.logger.Warn("follower node gets client command")
					clientCommandInternal.RespChan <- &utils.RPCRespWrapper[*rpc.ClientCommandResponse]{
						Resp: &rpc.ClientCommandResponse{
							Result: nil,
						},
						Err: common.ErrNotLeader,
					}
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
*/
func (n *Node) RunAsCandidate(ctx context.Context) {

	if n.GetNodeState() != StateCandidate {
		panic("node is not in CANDIDATE state")
	}

	n.logger.Info("node starts to acquiring CANDIDATE state")
	n.sem.Acquire(ctx, 1)
	n.logger.Info("node has acquired semaphore in CANDIDATE state")

	workerCtx, workerCancel := context.WithCancel(ctx)
	workerWaitGroup := sync.WaitGroup{}
	workerWaitGroup.Add(1)
	n.noleaderApplySignalChan = make(chan bool, n.cfg.GetRaftNodeRequestBufferSize())
	go n.noleaderWorkerToApplyLogs(workerCtx, &workerWaitGroup)
	defer func() {
		workerCancel()
		workerWaitGroup.Wait() // cancel only closes the Done channel, it doesn't wait for the worker to exit
		n.logger.Info("candidate worker exited successfully")
		n.sem.Release(1)
	}()

	consensusChan := n.candidateAsyncDoElection(ctx)
	ticker := time.NewTicker(n.cfg.GetElectionTimeout())
	for {
		currentTerm := n.getCurrentTerm()
		select {
		case <-ctx.Done():
			n.logger.Warn("raft node's main context done, exiting")
			n.membership.GracefulShutdown() // this cannot be in defer
			return
		default:
			{
				select {
				case <-ctx.Done():
					n.logger.Warn("raft node's main context done, exiting")
					n.membership.GracefulShutdown() // this cannot be in defer
					return
				case response := <-consensusChan: // some response from last election
					// I don't think we need to reset the ticker here
					// voteCancel() // cancel the rest
					if response.VoteGranted {
						n.SetNodeState(StateLeader)
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
							n.SetNodeState(StateFollower)
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
					consensusChan = n.candidateAsyncDoElection(ctx)
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
						n.SetNodeState(StateFollower)
						go n.RunAsFollower(ctx)
						return
					}
				case req := <-n.appendEntryChan: // commonRule: handling appendEntry from a leader which can be stale or new
					resp := n.noleaderHandleAppendEntries(ctx, req.Req)
					wrappedResp := utils.RPCRespWrapper[*rpc.AppendEntriesResponse]{
						Resp: resp,
						Err:  nil,
					}
					req.RespChan <- &wrappedResp
					if req.Req.Term >= currentTerm { // maki: here allows the equal term, because current node is not leader
						n.SetNodeState(StateFollower)
						go n.RunAsFollower(ctx)
						return
					}
				case clientCommand := <-n.receiveClientCommandChan:
					// todo: add delegation to the leader
					n.logger.Warn("follower node gets client command")
					clientCommand.RespChan <- &utils.RPCRespWrapper[*rpc.ClientCommandResponse]{
						Resp: &rpc.ClientCommandResponse{
							Result: nil,
						},
						Err: common.ErrNotLeader,
					}
				}
			}
		}
	}
}
