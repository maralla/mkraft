package mkraft

import (
	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
)

// utils

// maki: after success for this node, but does this require majority?
func (n *Node) updatePeerIndexAfterAppendEntries(nodeID string, req *rpc.AppendEntriesRequest) {
	newIdx := req.PrevLogIndex + uint64(len(req.Entries))
	n.stateRWLock.Lock()
	defer n.stateRWLock.Unlock()
	n.matchIndex[nodeID] = newIdx
	n.nextIndex[nodeID] = newIdx
}

// func (n *Node) updatePeersIndex(nodeID string, nextIndex, matchIndex uint64) {
// 	n.stateRWLock.Lock()
// 	defer n.stateRWLock.Unlock()
// 	n.matchIndex[nodeID] = matchIndex
// 	n.nextIndex[nodeID] = nextIndex
// }

func (n *Node) getPeersMatchIndex(nodeID string) uint64 {
	if index, ok := n.matchIndex[nodeID]; ok {
		return index
	}
	return 0
}

func (n *Node) getPeersNextIndex(nodeID string) uint64 {
	if index, ok := n.nextIndex[nodeID]; ok {
		return index
	}
	return n.raftLog.GetLastLogIdx() + 1
}

func (node *Node) updateCommitIdx(commitIdx uint64) {
	node.commitIndex = commitIdx
}

func (node *Node) addLastAppliedIdx(numberOfCommand uint64) {
	node.lastApplied = node.lastApplied + numberOfCommand
}

// todo: this method can be very problematic
func (n *Node) catchupAppliedIdx() error {
	if n.lastApplied < n.commitIndex {
		logs, err := n.raftLog.GetLogsFromIdxIncluded(n.lastApplied + 1)
		if err != nil {
			n.logger.Error("failed to get logs from index", zap.Error(err))
			return err
		}
		for idx, log := range logs {
			n.statemachine.ApplyCommand(log.Commands, n.lastApplied+1+uint64(idx))
			n.addLastAppliedIdx(1)
		}
		return nil
	}
	return nil
}
