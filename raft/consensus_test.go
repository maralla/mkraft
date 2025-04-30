package raft

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/maki3cat/mkraft/rpc"
	gomock "go.uber.org/mock/gomock"
)

type DummyIface interface {
	Dummay()
}

type DummyImpl struct {
}

func (d *DummyImpl) Dummay() {
	fmt.Println("DummyImpl Dummay called")
}

func TestPrintIface(t *testing.T) {
	var iface DummyIface = &DummyImpl{}
	fmt.Println(iface)
}
func TestCalculateIfMajorityMet(t *testing.T) {
	tests := []struct {
		total               int
		peerVoteAccumulated int
		expected            bool
	}{
		{total: 5, peerVoteAccumulated: 3, expected: true},
		{total: 5, peerVoteAccumulated: 2, expected: false},
		{total: 3, peerVoteAccumulated: 2, expected: true},
		{total: 3, peerVoteAccumulated: 1, expected: false},
	}

	for _, test := range tests {
		result := calculateIfMajorityMet(test.total, test.peerVoteAccumulated)
		if result != test.expected {
			t.Errorf("calculateIfMajorityMet(%d, %d) = %v; want %v", test.total, test.peerVoteAccumulated, result, test.expected)
		}
	}
}

func TestCalculateIfAlreadyFail(t *testing.T) {
	tests := []struct {
		total               int
		peersCount          int
		peerVoteAccumulated int
		voteFailed          int
		expected            bool
	}{
		{total: 5, peersCount: 4, peerVoteAccumulated: 2, voteFailed: 1, expected: false},
		{total: 5, peersCount: 4, peerVoteAccumulated: 1, voteFailed: 2, expected: false},
		{total: 5, peersCount: 4, peerVoteAccumulated: 1, voteFailed: 3, expected: true},

		{total: 3, peersCount: 1, peerVoteAccumulated: 0, voteFailed: 1, expected: true},
		{total: 3, peersCount: 2, peerVoteAccumulated: 1, voteFailed: 0, expected: false},
	}

	for _, test := range tests {
		result := calculateIfAlreadyFail(test.total, test.peersCount, test.peerVoteAccumulated, test.voteFailed)
		if result != test.expected {
			t.Errorf("calculateIfAlreadyFail(%d, %d, %d, %d) = %v; want %v", test.total, test.peersCount, test.peerVoteAccumulated, test.voteFailed, result, test.expected)
		}
	}
}
func TestRequestVoteSendForConsensus(t *testing.T) {
	ctrl := gomock.NewController(t)

	// Mock dependencies
	mockMemberMgr := NewMockMembershipMgrIface(ctrl)
	memberMgr = mockMemberMgr
	mockClient1 := rpc.NewMockInternalClientIface(ctrl)
	mockClient2 := rpc.NewMockInternalClientIface(ctrl)
	peerClients := []rpc.InternalClientIface{mockClient1, mockClient2}
	// majorityClients := []rpc.InternalClientIface{mockClient1}

	// Removed unused variables allPeerClients and notEnoughPeerClients

	tests := []struct {
		name         string
		mockSetup    func()
		request      *rpc.RequestVoteRequest
		expectedResp *MajorityRequestVoteResp
		expectedErr  error
	}{
		{
			name: "Error getting peer clients",
			mockSetup: func() {
				mockMemberMgr.EXPECT().GetMemberCount().Return(3)
				mockMemberMgr.EXPECT().GetAllPeerClients().Return(nil, errors.New("mock error"))
			},
			request:      &rpc.RequestVoteRequest{Term: 1},
			expectedResp: nil,
			expectedErr:  errors.New("mock error"),
		},
		{
			name: "Not enough peer clients",
			mockSetup: func() {
				mockMemberMgr.EXPECT().GetMemberCount().Return(3)
				mockMemberMgr.EXPECT().GetAllPeerClients().Return(nil, nil) // Fixed undefined peerClients
			},
			request:      &rpc.RequestVoteRequest{Term: 1},
			expectedResp: nil,
			expectedErr:  errors.New("no member clients found"),
		},
		{
			name: "Majority vote granted",
			mockSetup: func() {
				mockMemberMgr.EXPECT().GetAllPeerClients().Return(peerClients, nil)
				mockMemberMgr.EXPECT().GetMemberCount().Return(3)
			},
			request: &rpc.RequestVoteRequest{Term: 1},
			expectedResp: &MajorityRequestVoteResp{
				Term:        1,
				VoteGranted: true,
			},
			expectedErr: nil,
		},
		// {
		// 	name: "Higher term received",
		// 	mockSetup: func() {
		// 		mockMemberMgr.EXPECT().GetAllPeerClients().Return(majorityClients, nil)
		// 		mockMemberMgr.EXPECT().GetMemberCount().Return(3)
		// 	},
		// 	request: &rpc.RequestVoteRequest{Term: 1},
		// 	expectedResp: &MajorityRequestVoteResp{
		// 		Term:        2,
		// 		VoteGranted: false,
		// 	},
		// 	expectedErr: nil,
		// },
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Setup mocks
			test.mockSetup()

			// Call the function
			ctx := context.Background()
			resp, err := RequestVoteSendForConsensus(ctx, test.request)

			// Validate results
			if resp != nil && test.expectedResp != nil {
				if resp.Term != test.expectedResp.Term || resp.VoteGranted != test.expectedResp.VoteGranted {
					t.Errorf("unexpected response: got %+v, want %+v", resp, test.expectedResp)
				}
			} else if resp != test.expectedResp {
				t.Errorf("unexpected response: got %+v, want %+v", resp, test.expectedResp)
			}

			if (err != nil && test.expectedErr == nil) || (err == nil && test.expectedErr != nil) || (err != nil && test.expectedErr != nil && err.Error() != test.expectedErr.Error()) {
				t.Errorf("unexpected error: got %v, want %v", err, test.expectedErr)
			}
		})
	}
}
