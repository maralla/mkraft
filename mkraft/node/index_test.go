package node

import (
	"os"
	"testing"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/peers"
	"github.com/maki3cat/mkraft/mkraft/plugs"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func TestGetIdxFileName(t *testing.T) {
	node := &Node{}
	assert.Equal(t, "index.rft", node.getIdxFileName())
}

func TestUnsafeSaveAndLoadIdx(t *testing.T) {
	// Setup
	node := &Node{
		commitIndex: 5,
		lastApplied: 3,
	}
	// Cleanup
	defer os.Remove(node.getIdxFileName())

	// Test save
	err := node.unsafeSaveIdx()
	assert.NoError(t, err)

	// Verify file exists
	_, err = os.Stat(node.getIdxFileName())
	assert.NoError(t, err)

	// Create new node to test load
	newNode := &Node{}
	err = newNode.unsafeLoadIdx()
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), newNode.commitIndex)
	assert.Equal(t, uint64(3), newNode.lastApplied)
}

func NewNodeForMock() *Node {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRaftLog := plugs.NewMockRaftLogsIface(ctrl)
	membership := peers.NewMockMembershipMgrIface(ctrl)
	config := common.NewMockConfigIface(ctrl)

	mockRaftLog.EXPECT().GetLastLogIdx().Return(uint64(0))
	node := NewNode("1", config, zap.NewNop(), membership)

	return &Node{
		raftLog:    mockRaftLog,
		membership: membership,
	}
}

func TestGetCommitIdxAndLastApplied(t *testing.T) {
	node := &Node{
		commitIndex: 10,
		lastApplied: 8,
	}

	commitIdx, lastApplied := node.getCommitIdxAndLastApplied()
	assert.Equal(t, uint64(10), commitIdx)
	assert.Equal(t, uint64(8), lastApplied)
}

func TestGetCommitIdx(t *testing.T) {
	node := &Node{
		commitIndex: 15,
	}

	commitIdx := node.getCommitIdx()
	assert.Equal(t, uint64(15), commitIdx)
}

func TestIncrementCommitIdx(t *testing.T) {
	node := &Node{
		commitIndex: 5,
	}

	node.incrementCommitIdx(3)
	assert.Equal(t, uint64(8), node.commitIndex)

	// Cleanup any created files
	os.Remove(node.getIdxFileName())
}

func TestIncrementLastApplied(t *testing.T) {
	node := &Node{
		lastApplied: 5,
	}

	node.incrementLastApplied(2)
	assert.Equal(t, uint64(7), node.lastApplied)

	// Cleanup any created files
	os.Remove(node.getIdxFileName())
}

func TestIncrementPeersNextIndexOnSuccess(t *testing.T) {
	node := &Node{
		nextIndex:  make(map[string]uint64),
		matchIndex: make(map[string]uint64),
	}

	// Test first time increment
	node.incrementPeersNextIndexOnSuccess("peer1", 3)
	assert.Equal(t, uint64(4), node.nextIndex["peer1"])
	assert.Equal(t, uint64(3), node.matchIndex["peer1"])

	// Test subsequent increment
	node.incrementPeersNextIndexOnSuccess("peer1", 2)
	assert.Equal(t, uint64(6), node.nextIndex["peer1"])
	assert.Equal(t, uint64(5), node.matchIndex["peer1"])
}

func TestDecrementPeersNextIndexOnFailure(t *testing.T) {
	node := &Node{
		nextIndex: map[string]uint64{
			"peer1": 5,
		},
	}

	node.decrementPeersNextIndexOnFailure("peer1")
	assert.Equal(t, uint64(4), node.nextIndex["peer1"])

	// Test minimum value
	node.nextIndex["peer1"] = 1
	node.decrementPeersNextIndexOnFailure("peer1")
	assert.Equal(t, uint64(1), node.nextIndex["peer1"])
}

func TestGetPeersNextIndex(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRaftLog := plugs.NewMockRaftLogsIface(ctrl)
	mockRaftLog.EXPECT().GetLastLogIdx().Return(uint64(0))

	node := &Node{
		nextIndex: map[string]uint64{
			"peer1": 10,
		},
		raftLog: mockRaftLog,
	}

	// Test existing peer
	index := node.getPeersNextIndex("peer1")
	assert.Equal(t, uint64(10), index)

	// Test new peer
	index = node.getPeersNextIndex("peer2")
	assert.Equal(t, uint64(1), index) // Assuming GetLastLogIdx returns 0 for empty log
}

func TestGetInitDefaultValuesForPeer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRaftLog := plugs.NewMockRaftLogsIface(ctrl)
	mockRaftLog.EXPECT().GetLastLogIdx().Return(uint64(0))

	node := &Node{
		raftLog: mockRaftLog,
	}

	nextIndex, matchIndex := node.getInitDefaultValuesForPeer()
	assert.Equal(t, uint64(1), nextIndex) // Assuming GetLastLogIdx returns 0 for empty log
	assert.Equal(t, uint64(0), matchIndex)
}
