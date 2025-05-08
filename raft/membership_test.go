package raft

import (
	"errors"
	"sync"
	"testing"

	"github.com/maki3cat/mkraft/rpc"
	"github.com/stretchr/testify/assert"
	gomock "go.uber.org/mock/gomock"
)

func TestInitStatisMembership(t *testing.T) {
	tests := []struct {
		name          string
		membership    *Membership
		expectedError error
	}{
		{
			name: "Valid membership",
			membership: &Membership{
				CurrentNodeID: "node1",
				AllMembers: []NodeAddr{
					{NodeID: "node1", NodeURI: "localhost:5001"},
					{NodeID: "node2", NodeURI: "localhost:5002"},
					{NodeID: "node3", NodeURI: "localhost:5003"},
				},
			},
			expectedError: nil,
		},
		{
			name: "Cluster size less than 3",
			membership: &Membership{
				CurrentNodeID: "node1",
				AllMembers: []NodeAddr{
					{NodeID: "node1", NodeURI: "localhost:5001"},
					{NodeID: "node2", NodeURI: "localhost:5002"},
				},
			},
			expectedError: errors.New("smallest cluster size is 3"),
		},
		{
			name: "Even member count",
			membership: &Membership{
				CurrentNodeID: "node1",
				AllMembers: []NodeAddr{
					{NodeID: "node1", NodeURI: "localhost:5001"},
					{NodeID: "node2", NodeURI: "localhost:5002"},
					{NodeID: "node3", NodeURI: "localhost:5003"},
					{NodeID: "node4", NodeURI: "localhost:5004"},
				},
			},
			expectedError: errors.New("the member count should be odd"),
		},
		{
			name: "Empty NodeID or NodeURI",
			membership: &Membership{
				CurrentNodeID: "node1",
				AllMembers: []NodeAddr{
					{NodeID: "node1", NodeURI: "localhost:5001"},
					{NodeID: "", NodeURI: "localhost:5002"},
					{NodeID: "node3", NodeURI: ""},
				},
			},
			expectedError: errors.New("node id and uri should not be empty"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := InitStatisMembership(tt.membership)
			if tt.expectedError != nil {
				assert.EqualError(t, err, tt.expectedError.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestStaticMembershipMgr_GetCurrentNodeID(t *testing.T) {
	membership := &Membership{
		CurrentNodeID: "node1",
		AllMembers: []NodeAddr{
			{NodeID: "node1", NodeURI: "localhost:5001"},
			{NodeID: "node2", NodeURI: "localhost:5002"},
			{NodeID: "node3", NodeURI: "localhost:5003"},
		},
	}
	mgr := &StaticMembershipMgr{membership: membership}

	assert.Equal(t, "node1", mgr.GetCurrentNodeID())
}

func TestStaticMembershipMgr_GetMemberCount(t *testing.T) {
	membership := &Membership{
		AllMembers: []NodeAddr{
			{NodeID: "node1", NodeURI: "localhost:5001"},
			{NodeID: "node2", NodeURI: "localhost:5002"},
			{NodeID: "node3", NodeURI: "localhost:5003"},
		},
	}
	mgr := &StaticMembershipMgr{membership: membership}

	assert.Equal(t, 3, mgr.GetMemberCount())
}

func TestStaticMembershipMgr_GetAllPeerClients(t *testing.T) {
	membership := &Membership{
		CurrentNodeID: "node1",
		AllMembers: []NodeAddr{
			{NodeID: "node1", NodeURI: "localhost:5001"},
			{NodeID: "node2", NodeURI: "localhost:5002"},
			{NodeID: "node3", NodeURI: "localhost:5003"},
		},
	}
	mgr := &StaticMembershipMgr{
		membership:    membership,
		peerAddrs:     map[string]string{"node2": "localhost:5002", "node3": "localhost:5003"},
		peerInitLocks: map[string]*sync.Mutex{"node2": &sync.Mutex{}, "node3": &sync.Mutex{}},
		clients:       &sync.Map{},
		// conns:         &sync.Map{},
	}

	ctrl := gomock.NewController(t)
	mockClient := rpc.NewMockInternalClientIface(ctrl)
	mgr.clients.Store("node2", mockClient)
	mgr.clients.Store("node3", mockClient)

	clients, err := mgr.GetAllPeerClients()
	assert.NoError(t, err)
	assert.Len(t, clients, 2)
}

func TestStaticMembershipMgr_getPeerClient(t *testing.T) {
	membership := &Membership{
		CurrentNodeID: "node1",
		AllMembers: []NodeAddr{
			{NodeID: "node1", NodeURI: "localhost:5001"},
			{NodeID: "node2", NodeURI: "localhost:5002"},
		},
	}
	mgr := &StaticMembershipMgr{
		membership:    membership,
		peerAddrs:     map[string]string{"node2": "localhost:5002"},
		peerInitLocks: map[string]*sync.Mutex{"node2": &sync.Mutex{}},
		clients:       &sync.Map{},
		// conns:         &sync.Map{},
	}

	// Mock gRPC connection and client creation
	// mockConn := &grpc.ClientConn{}
	ctrl := gomock.NewController(t)
	mockClient := rpc.NewMockInternalClientIface(ctrl)
	// mgr.conns.Store("node2", mockConn)
	mgr.clients.Store("node2", mockClient)

	client, err := mgr.getPeerClient("node2")
	assert.NoError(t, err)
	assert.Equal(t, mockClient, client)
}
