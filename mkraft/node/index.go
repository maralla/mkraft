package node

import (
	"go.uber.org/zap"
)

// section1: for indices of commidID and lastApplied which are owned by all the nodes
func (n *Node) getCommitIdxAndLastApplied() (uint64, uint64) {
	n.stateRWLock.RLock()
	defer n.stateRWLock.RUnlock()
	return n.commitIndex, n.lastApplied
}

func (n *Node) getLastApplied() uint64 {
	n.stateRWLock.RLock()
	defer n.stateRWLock.RUnlock()
	return n.lastApplied
}

func (n *Node) getCommitIdx() uint64 {
	n.stateRWLock.RLock()
	defer n.stateRWLock.RUnlock()
	return n.commitIndex
}

func (n *Node) updateCommitIdx(commitIdx uint64) {
	n.stateRWLock.Lock()
	defer n.stateRWLock.Unlock()
	if commitIdx > n.commitIndex {
		// update the commitIndex = min(leaderCommit, index of last new entry in the log)
		newIndex := min(n.raftLog.GetLastLogIdx(), commitIdx)
		n.commitIndex = newIndex
	} else {
		n.logger.Warn("commit index is not updated, it is smaller than current commit index",
			zap.Uint64("commitIdx", commitIdx),
			zap.Uint64("currentCommitIndex", n.commitIndex))
	}
}

func (n *Node) incrementLastApplied(numberOfCommand uint64) {
	n.stateRWLock.Lock()
	defer n.stateRWLock.Unlock()
	n.lastApplied = n.lastApplied + numberOfCommand
}

// section2: for indices of leaders, nextIndex/matchIndex
// maki: Updating a follower’s match/next index is independent of whether consensus is reached.
// Updating matchIndex/nextIndex is a per-follower operation.
// Reaching consensus (a majority of nodes having the same entry) is a cluster-wide operation.
func (n *Node) incrementPeersNextIndexOnSuccess(nodeID string, logCnt uint64) {
	n.stateRWLock.Lock()
	defer n.stateRWLock.Unlock()

	// important
	// According to the Log Matching Property in Raft:
	// once appendEntries is successful,
	// the follower's matchIndex should be equal to the index of the last entry appended,
	// and the nextIndex should be matchIndex + 1

	if _, exists := n.nextIndex[nodeID]; exists {
		n.nextIndex[nodeID] = n.nextIndex[nodeID] + logCnt
	} else {
		n.nextIndex[nodeID] = logCnt + 1
	}
	n.matchIndex[nodeID] = n.nextIndex[nodeID] - 1
}

// important invariant: matchIndex[follower] ≤ nextIndex[follower] - 1
// if the matchIndex is less than nextIndex-1, appendEntries will fail and the follower's nextIndex will be decremented
func (n *Node) decrementPeersNextIndexOnFailure(nodeID string) {
	n.stateRWLock.Lock()
	defer n.stateRWLock.Unlock()

	// todo: there can be improvement of efficiency here, refer to the paper page 8 first paragraph
	n.logger.Warn("fixing inconsistent logs for peer",
		zap.String("nodeID", nodeID))

	if n.nextIndex[nodeID] > 1 {
		n.nextIndex[nodeID] -= 1
	} else {
		n.logger.Error("next index is already at 1, cannot decrement", zap.String("nodeID", nodeID))
	}
}

func (n *Node) getPeersNextIndex(nodeID string) uint64 {
	n.stateRWLock.RLock()
	defer n.stateRWLock.RUnlock()
	if index, ok := n.nextIndex[nodeID]; ok {
		return index
	} else {
		n.nextIndex[nodeID], n.matchIndex[nodeID] = n.getInitDefaultValuesForPeer()
		return n.nextIndex[nodeID]
	}
}

// returns nextIndex, matchIndex
func (n *Node) getInitDefaultValuesForPeer() (uint64, uint64) {
	return n.raftLog.GetLastLogIdx() + 1, 0
}
