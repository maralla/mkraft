package node

import (
	"context"

	"github.com/maki3cat/mkraft/mkraft/utils"
	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
)

// Implementation gap: Raft log application behavior across different node roles and transitions
// Implementation gap: is complicated and not well documented by the paper; thus, I dedicate a file to this.

// Case-1: AS A Leader, newly elected
// When a follower becomes the leader,
// it needs to apply previous committed logs (which doesn't require replies to the clients)
// to the state machine before it can reply to new client commands

// Case-2: AS A Leader, while serving
// When a leader stays as the leader,
// it needs to apply new committed logs to the state machine
// and reply to the clients
func (n *Node) leaderWorkerForLogApplication(ctx context.Context) {
	// Case-1: apply lagged commited logs
	err := n.applyAllLaggedCommitedLogs(ctx)
	if err != nil {
		n.logger.Error("failed to apply all lagged commited logs", zap.Error(err))
		panic(err)
	}
	// Case-2: apply new committed logs
	for {
		select {
		case <-ctx.Done():
			return
		case clientCommand := <-n.leaderApplyCh:
			result, err := n.statemachine.ApplyCommand(ctx, clientCommand.Req.Command)
			if err != nil {
				n.logger.Error("failed to apply command", zap.Error(err))
				panic(err) // todo: shall not panic, shall handle the error properly
			}
			clientCommand.RespChan <- &utils.RPCRespWrapper[*rpc.ClientCommandResponse]{
				Resp: &rpc.ClientCommandResponse{
					Result: result,
				},
				Err: nil,
			}
		}
	}
}

// AS A Leader, but stale
// Case(3) When a leader finds itself stale
// implementation gap:
// it can give up serving clients, just drain/return no leader leaderApplyCh, and leave the work to follower
func (n *Node) cleanupApplyLogsBeforeToFollower() {
	utils.DrainChannel(n.leaderApplyCh, n.cfg.GetRaftNodeRequestBufferSize())
}

// AS A Follower/Candidate
// Case(4) When a follower/candidate commits a new log
// it doesn't needs to apply the logs to the state machine which may be slow and the new leader need to start heartbeat soon
// so it just drains the noleaderApplySignalCh
func (n *Node) cleanupApplyLogsBeforeToLeader() {
	utils.DrainChannel(n.noleaderApplySignalCh, n.cfg.GetRaftNodeRequestBufferSize())
}

// ??⚠️ Case (5): Log entry committed but not yet applied, node crashes and restarts

// BASIC OPERATIONS:
// apply the committed yet not applied logs to the state machine
func (n *Node) applyAllLaggedCommitedLogs(ctx context.Context) error {

	if len(n.leaderApplyCh) > 0 {
		n.logger.Error("leaderApplyCh is not empty, this should not happen")
		// todo: temporary panic, need to fix
		panic("leaderApplyCh is not empty, this should not happen")
	}

	commitIdx, lastApplied := n.getCommitIdxAndLastApplied()
	if commitIdx > lastApplied {
		logs, err := n.raftLog.GetLogsFromIdxIncluded(lastApplied + 1)
		if err != nil {
			n.logger.Error("failed to get logs from index", zap.Error(err))
			panic(err)
		}
		cmds := make([][]byte, len(logs))
		for i, cmd := range logs {
			cmds[i] = cmd.Commands
		}
		_, err = n.statemachine.BatchApplyCommand(ctx, cmds)
		if err != nil {
			n.logger.Error("failed to apply command to state machine", zap.Error(err))
			return err
		}
		n.incrementLastApplied(uint64(len(logs)))
	}
	return nil
}
