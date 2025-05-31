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

	if n.State != StateFollower {
		panic("node is not in FOLLOWER state")
	}
	n.logger.Info("node acquires to run in FOLLOWER state")
	n.sem.Acquire(ctx, 1)
	n.logger.Info("acquired semaphore in FOLLOWER state")

	workerCtx, cancel := context.WithCancel(ctx)
	workerWaitGroup := sync.WaitGroup{}
	workerWaitGroup.Add(1)
	electionTicker := time.NewTicker(n.cfg.GetElectionTimeout())

	// here is actually a variate pipeline pattern:
	// first step is log replication, second step is apply to the state machine
	n.noleaderApplySignalChan = make(chan bool, n.cfg.GetRaftNodeRequestBufferSize())
	go n.noLeaderWorkerToApplyCommandToStateMachine(workerCtx, &workerWaitGroup)

	defer func() { // gracefully exit for follower state is easy
		electionTicker.Stop()
		cancel()
		workerWaitGroup.Wait() // cancel only closes the Done channel, it doesn't wait for the worker to exit
		n.logger.Info("follower worker exited successfully")
		n.sem.Release(1)
	}()

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
					resp := n.handlerAppendEntriesAsNoLeader(ctx, appendEntryInternal.Req)
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

	// when the node changes the state, the worker shall exit
	workerCtx, cancel := context.WithCancel(ctx)
	workerWaitGroup := sync.WaitGroup{}
	workerWaitGroup.Add(1)

	// here is actually a variate pipeline pattern:
	// first step is log replication, second step is apply to the state machine
	n.noleaderApplySignalChan = make(chan bool, n.cfg.GetRaftNodeRequestBufferSize())
	go n.noLeaderWorkerToApplyCommandToStateMachine(workerCtx, &workerWaitGroup)

	defer func() {
		cancel()
		workerWaitGroup.Wait() // cancel only closes the Done channel, it doesn't wait for the worker to exit
		n.logger.Info("candidate worker exited successfully")
		n.sem.Release(1)
	}()

	consensusChan := n.runOneElectionAsCandidate(ctx)

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
					consensusChan = n.runOneElectionAsCandidate(ctx)
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
					resp := n.handlerAppendEntriesAsNoLeader(ctx, req.Req)
					wrappedResp := utils.RPCRespWrapper[*rpc.AppendEntriesResponse]{
						Resp: resp,
						Err:  nil,
					}
					req.RespChan <- &wrappedResp
					if req.Req.Term >= currentTerm { // maki: here allows the equal term, because current node is not leader
						n.State = StateFollower
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

// maki: lastLogIndex, commitIndex, lastApplied can be totally different from each other
// shall be called when the node is not a leader
// the raft server is generally single-threaded, so there is no other thread to change the commitIdx
func (n *Node) handlerAppendEntriesAsNoLeader(ctx context.Context, req *rpc.AppendEntriesRequest) *rpc.AppendEntriesResponse {

	var response rpc.AppendEntriesResponse
	reqTerm := uint32(req.Term)
	currentTerm := n.getCurrentTerm()

	// return FALSE CASES:
	// (1) fast track for the stale term
	// (2) check the prevLogIndex and prevLogTerm
	if reqTerm < currentTerm || !n.raftLog.CheckPreLog(req.PrevLogIndex, req.PrevLogTerm) {
		response = rpc.AppendEntriesResponse{
			Term:    currentTerm,
			Success: false,
		}
		return &response
	}

	// return TRUE CASES:

	// 1. udpate commitIdx, and trigger the apply
	defer func() {
		// the updateCommitIdx will find the min(leaderCommit, index of last new entry in the log), so the update
		// doesn't require result of appendLogs
		n.updateCommitIdx(req.LeaderCommit)
		n.noleaderApplySignalChan <- true
	}()

	// 2. update the term
	if reqTerm > currentTerm {
		err := n.storeCurrentTermAndVotedFor(reqTerm, "") // did not vote for anyone
		if err != nil {
			n.logger.Error(
				"error in storeCurrentTermAndVotedFor", zap.Error(err),
				zap.String("nId", n.NodeId))
			panic(err) // todo: critical error, cannot continue, not sure how to handle this
		}
	}

	// 3. append logs
	if len(req.Entries) > 0 {
		logs := make([][]byte, len(req.Entries))
		for idx, entry := range req.Entries {
			logs[idx] = entry.Data
		}
		err := n.raftLog.UpdateLogsInBatch(ctx, req.PrevLogIndex, logs, req.Term)
		if err != nil {
			// this error cannot be not match,
			// because the prevLogIndex and prevLogTerm has been checked
			panic(err) // todo: critical error, cannot continue, not sure how to handle this
		}
	}

	response = rpc.AppendEntriesResponse{
		Term:    currentTerm,
		Success: true,
	}
	return &response
}

func (node *Node) runOneElectionAsCandidate(ctx context.Context) chan *MajorityRequestVoteResp {
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
	// todo: must I call the cancel function?
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

// leader shall reply yet others not
// currently, apply to the state machine in serial
func (n *Node) noLeaderWorkerToApplyCommandToStateMachine(ctx context.Context, workerWaitGroup *sync.WaitGroup) {
	defer workerWaitGroup.Done()

	// reset the channel, todo: the size of the channel should be re-considered
	n.noleaderApplySignalChan = make(chan bool, n.cfg.GetRaftNodeRequestBufferSize())
	tickerTriggered := time.NewTicker(time.Duration(500 * time.Microsecond))
	defer tickerTriggered.Stop()

	for {
		select {
		case <-ctx.Done():
			n.logger.Warn("apply-worker, exiting leader's worker for applying commands")
		default:
			select {
			// try to signal this channel this everytime with heartbeat
			case <-tickerTriggered.C:
				n.noleaderApplyCommandToStateMachine()
			case <-n.noleaderApplySignalChan:
				// this commitIdx is the min(leaderCommit, index of last new entry in the log)
				// so it must be greater or equal to lastApplied
				n.noleaderApplyCommandToStateMachine()
			case <-ctx.Done():
				n.logger.Warn("apply-worker, exiting leader's worker for applying commands")
				return
			}
		}
	}
}

func (n *Node) noleaderApplyCommandToStateMachine() {
	commitIdx, lastApplied := n.getCommitIdxAndLastApplied()
	if commitIdx > lastApplied {
		logs, err := n.raftLog.GetLogsFromIdxIncluded(lastApplied + 1)
		if err != nil {
			n.logger.Error("failed to get logs from index", zap.Error(err))
			panic(err)
		}
		// apply the command
		for _, log := range logs {
			// the no-leader node doesn't need to respond to clients
			_, err := n.statemachine.ApplyCommand(log.Commands)
			if err != nil {
				n.logger.Error("failed to apply command to state machine", zap.Error(err))
				panic(err)
			} else {
				n.incrementLastApplied(1)
			}
		}
	}
}
