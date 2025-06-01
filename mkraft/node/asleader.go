package node

import (
	"context"
	"time"

	"github.com/maki3cat/mkraft/mkraft/utils"
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

(3) If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex for the follower;
If successful: update nextIndex and matchIndex for follower
If AppendEntries fails because of log inconsistency: decrement nextIndex and retry

(4) If there exists and N such that N > committedIndex, a majority of matchIndex[i] ≥ N, ... (5.3/5.4)
todo: this paper doesn't mention how a stale leader catches up and becomes a follower
*/
func (n *Node) RunAsLeader(ctx context.Context) {
	n.runAsLeaderImpl(ctx)
}

// SINGLE GOROUTINE WITH BATCHING PATTERN
// maki: take this simple version as the baseline, and add more gorouintes if this has performance issues
// analysis of why this pattern can be used:
// task2, task4 have data race if using different goroutine -> they both change raftlog/state machine in a serial order
// so one way is to use the same goroutine for task2 and task4 to handle log replication
// normally, the leader only has task4, and only in unstable conditions, it has to handle task2 and task4 at the same time
// if task4 accepts the log replication, the leader shall degrade to follower with graceful shutdown

// task3 also happens in unstable conditions, but it is less frequent than task2 so can also be in the same goroutine
// task1 has lower priority than task3

// the only worker thread needed is the log applicaiton thread
func (n *Node) runAsLeaderImpl(ctx context.Context) {

	if n.GetNodeState() != StateLeader {
		panic("node is not in LEADER state")
	}
	n.logger.Info("acquiring the Semaphore as the LEADER state")
	n.sem.Acquire(ctx, 1)
	n.logger.Info("acquired the Semaphore as the LEADER state")
	defer n.sem.Release(1)

	// debugging
	n.recordLeaderState()

	// maki: this is a tricky design (the whole design of the log/client command application is tricky)
	// todo: catch up the log application to make sure lastApplied == commitIndex for the leader
	n.leaderApplyCh = make(chan *utils.ClientCommandInternalReq, n.cfg.GetRaftNodeRequestBufferSize())

	subWorkerCtx, subWorkerCancel := context.WithCancel(ctx)
	defer subWorkerCancel()
	go n.leaderWorkerForLogApplication(subWorkerCtx)

	heartbeatDuration := n.cfg.GetLeaderHeartbeatPeriod()
	tickerForHeartbeat := time.NewTicker(heartbeatDuration)
	defer tickerForHeartbeat.Stop()
	var singleJobResult JobResult
	var err error

	for {
		if singleJobResult.ShallDegrade {
			subWorkerCancel()
			n.storeCurrentTermAndVotedFor(uint32(singleJobResult.Term), singleJobResult.VotedFor, false)
			n.cleanupApplyLogsBeforeToFollower()
			return
		}
		select {
		case <-ctx.Done(): // give ctx higher priority
			n.logger.Warn("raft node main context done, exiting")
			return
		default:
			select {

			case <-ctx.Done():
				n.logger.Warn("raft node main context done, exiting")
				return

			// task1: send the heartbeat -> as leader, may degrade to follower
			case <-tickerForHeartbeat.C:
				tickerForHeartbeat.Reset(heartbeatDuration)
				singleJobResult, err = n.syncDoHeartbeat(ctx)
				if err != nil {
					n.logger.Error("error in sending heartbeat, omit it and continue", zap.Error(err))
				}

			// task2: handle the client command, need to change raftlog/state machine -> as leader, may degrade to follower
			case clientCmd := <-n.clientCommandCh:
				tickerForHeartbeat.Reset(heartbeatDuration)
				batchingSize := n.cfg.GetRaftNodeRequestBufferSize() - 1
				clientCommands := utils.ReadMultipleFromChannel(n.clientCommandCh, batchingSize)
				clientCommands = append(clientCommands, clientCmd)
				singleJobResult, err = n.syncDoLogReplication(ctx, clientCommands)
				if err != nil {
					// todo: as is same with most other panics, temporary solution, shall handle the error properly
					panic(err)
				}

			// task3: handle the requestVoteChan -> as a node, may degrade to follower
			case internalReq := <-n.requestVoteCh:
				singleJobResult, err = n.handleRequestVoteAsLeader(internalReq)
				if err != nil {
					n.logger.Error("error in handling request vote", zap.Error(err))
					panic(err)
				}
			// task4: handle the appendEntryChan, need to change raftlog/state machine -> as a node, may degrade to follower
			case internalReq := <-n.appendEntryCh:
				singleJobResult, err = n.handlerAppendEntriesAsLeader(internalReq)
				if err != nil {
					n.logger.Error("error in handling append entries", zap.Error(err))
					panic(err)
				}
			}
		}
	}
}
