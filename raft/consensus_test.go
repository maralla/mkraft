package raft

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/maki3cat/mkraft/rpc"
	"github.com/maki3cat/mkraft/util"
	gomock "go.uber.org/mock/gomock"
)

func TestCalculateIfMajorityMet(t *testing.T) {
	tests := []struct {
		total               int
		peerVoteAccumulated int
		expected            bool
	}{
		{total: 5, peerVoteAccumulated: 3, expected: true},
		{total: 5, peerVoteAccumulated: 2, expected: true},
		{total: 5, peerVoteAccumulated: 1, expected: false},
		{total: 3, peerVoteAccumulated: 2, expected: true},
		{total: 3, peerVoteAccumulated: 1, expected: true},
		{total: 3, peerVoteAccumulated: 0, expected: false},
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
		{total: 5, peersCount: 3, peerVoteAccumulated: 2, voteFailed: 1, expected: false},
		{total: 5, peersCount: 3, peerVoteAccumulated: 1, voteFailed: 2, expected: true},
		{total: 5, peersCount: 4, peerVoteAccumulated: 1, voteFailed: 3, expected: true},
		{total: 5, peersCount: 4, peerVoteAccumulated: 1, voteFailed: 2, expected: false},

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

	mockConfig := util.NewMockConfigIface(ctrl)
	mockConfig.EXPECT().GetElectionTimeout().Return(1000 * time.Millisecond).AnyTimes()
	mockMemberMgr := NewMockMembershipMgrIface(ctrl)
	memberMgr = mockMemberMgr

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
				mockMemberMgr.EXPECT().GetMemberCount().Return(3).Times(1)
				mockMemberMgr.EXPECT().GetAllPeerClients().Return(nil, errors.New("mock error")).Times(1)
			},
			request:      &rpc.RequestVoteRequest{Term: 1},
			expectedResp: nil,
			expectedErr:  errors.New("mock error"),
		},
		{
			name: "Not enough peer clients",
			mockSetup: func() {
				mockMemberMgr.EXPECT().GetMemberCount().Return(3).Times(1)
				mockMemberMgr.EXPECT().GetAllPeerClients().Return(nil, nil).Times(1) // Fixed undefined peerClients
			},
			request:      &rpc.RequestVoteRequest{Term: 1},
			expectedResp: nil,
			expectedErr:  errors.New("no member clients found"),
		},
		{
			name: "Majority vote granted",
			mockSetup: func() {

				respChan := make(chan rpc.RPCRespWrapper[*rpc.RequestVoteResponse], 1)
				respChan <- rpc.RPCRespWrapper[*rpc.RequestVoteResponse]{
					Err:  nil,
					Resp: &rpc.RequestVoteResponse{Term: 1, VoteGranted: true},
				}
				mockClient1 := rpc.NewMockInternalClientIface(ctrl)
				mockClient1.EXPECT().SendRequestVoteWithRetries(
					gomock.Any(), gomock.Any()).Return(respChan).Times(1)

				peerClients := make([]rpc.InternalClientIface, 1)
				peerClients[0] = mockClient1
				fmt.Printf("%d\n", len(peerClients))

				mockMemberMgr.EXPECT().GetAllPeerClients().Return(peerClients, nil).Times(1)
				mockMemberMgr.EXPECT().GetMemberCount().Return(3).Times(1)
			},
			request: &rpc.RequestVoteRequest{Term: 1},
			expectedResp: &MajorityRequestVoteResp{
				Term:        1,
				VoteGranted: true,
			},
			expectedErr: nil,
		},
		{
			name: "Higher term received",
			mockSetup: func() {
				respChan := make(chan rpc.RPCRespWrapper[*rpc.RequestVoteResponse], 1)
				respChan <- rpc.RPCRespWrapper[*rpc.RequestVoteResponse]{
					Err:  nil,
					Resp: &rpc.RequestVoteResponse{Term: 2, VoteGranted: false},
				}
				mockClient1 := rpc.NewMockInternalClientIface(ctrl)
				mockClient1.EXPECT().SendRequestVoteWithRetries(
					gomock.Any(), gomock.Any()).Return(respChan).Times(1)

				peerClients := make([]rpc.InternalClientIface, 1)
				peerClients[0] = mockClient1
				fmt.Printf("%d\n", len(peerClients))

				mockMemberMgr.EXPECT().GetAllPeerClients().Return(peerClients, nil)
				mockMemberMgr.EXPECT().GetMemberCount().Return(3)
			},
			request: &rpc.RequestVoteRequest{Term: 1},
			expectedResp: &MajorityRequestVoteResp{
				Term:        2,
				VoteGranted: false,
			},
			expectedErr: nil,
		},
		{
			name: "Majority failed to respond",
			mockSetup: func() {
				respChan := make(chan rpc.RPCRespWrapper[*rpc.RequestVoteResponse], 1)
				respChan <- rpc.RPCRespWrapper[*rpc.RequestVoteResponse]{
					Err:  errors.New("mock error"),
					Resp: nil,
				}
				mockClient1 := rpc.NewMockInternalClientIface(ctrl)
				mockClient1.EXPECT().SendRequestVoteWithRetries(
					gomock.Any(), gomock.Any()).Return(respChan).Times(1)

				peerClients := make([]rpc.InternalClientIface, 1)
				peerClients[0] = mockClient1
				fmt.Printf("%d\n", len(peerClients))

				mockMemberMgr.EXPECT().GetAllPeerClients().Return(peerClients, nil)
				mockMemberMgr.EXPECT().GetMemberCount().Return(3)
			},
			request:      &rpc.RequestVoteRequest{Term: 1},
			expectedResp: nil,
			expectedErr:  errors.New("majority of nodes failed to respond"),
		},
		{
			name: "Context timeout",
			mockSetup: func() {
				respChan := make(chan rpc.RPCRespWrapper[*rpc.RequestVoteResponse], 1)
				mockClient1 := rpc.NewMockInternalClientIface(ctrl)
				mockClient1.EXPECT().SendRequestVoteWithRetries(
					gomock.Any(), gomock.Any()).Return(respChan).Times(1)

				peerClients := make([]rpc.InternalClientIface, 1)
				peerClients[0] = mockClient1
				fmt.Printf("%d\n", len(peerClients))

				mockMemberMgr.EXPECT().GetAllPeerClients().Return(peerClients, nil)
				mockMemberMgr.EXPECT().GetMemberCount().Return(3)
			},
			request:      &rpc.RequestVoteRequest{Term: 1},
			expectedResp: nil,
			expectedErr:  errors.New("context done"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Setup mocks
			test.mockSetup()
			ctx, _ := context.WithTimeout(context.Background(), 300*time.Millisecond)
			// ctx := context.Background()
			resp, err := consensus.RequestVoteSendForConsensus(ctx, test.request)

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

func TestAppendEntriesSendForConsensus(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockConfig := util.NewMockConfigIface(ctrl)
	mockConfig.EXPECT().GetElectionTimeout().Return(1000 * time.Millisecond).AnyTimes()
	mockMemberMgr := NewMockMembershipMgrIface(ctrl)
	memberMgr = mockMemberMgr

	tests := []struct {
		name         string
		mockSetup    func()
		request      *rpc.AppendEntriesRequest
		expectedResp *AppendEntriesConsensusResp
		expectedErr  error
	}{
		{
			name: "Error getting peer clients",
			mockSetup: func() {
				mockMemberMgr.EXPECT().GetMemberCount().Return(3).Times(1)
				mockMemberMgr.EXPECT().GetAllPeerClients().Return(nil, errors.New("mock error")).Times(1)
			},
			request:      &rpc.AppendEntriesRequest{Term: 1},
			expectedResp: nil,
			expectedErr:  errors.New("mock error"),
		},
		{
			name: "Not enough peer clients",
			mockSetup: func() {
				mockMemberMgr.EXPECT().GetMemberCount().Return(3).Times(1)
				mockMemberMgr.EXPECT().GetAllPeerClients().Return(nil, nil).Times(1)
			},
			request:      &rpc.AppendEntriesRequest{Term: 1},
			expectedResp: nil,
			expectedErr:  errors.New("not enough peer clients found"),
		},
		{
			name: "Majority success",
			mockSetup: func() {
				var term int32 = 3
				mockClient1 := mockSendAppendEntries(ctrl, term)
				mockClient2 := mockSendAppendEntriesError(ctrl)
				mockClient3 := mockSendAppendEntries(ctrl, term)

				peerClients := []rpc.InternalClientIface{mockClient1, mockClient2, mockClient3}

				mockMemberMgr.EXPECT().GetAllPeerClients().Return(peerClients, nil).Times(1)
				mockMemberMgr.EXPECT().GetMemberCount().Return(5).Times(1)
			},
			request: &rpc.AppendEntriesRequest{Term: 3},
			expectedResp: &AppendEntriesConsensusResp{
				Term:    3,
				Success: true,
			},
			expectedErr: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Setup mocks
			test.mockSetup()
			ctx, _ := context.WithTimeout(context.Background(), 300*time.Millisecond)
			resp, err := consensus.AppendEntriesSendForConsensus(ctx, test.request)

			// Validate results
			if resp != nil && test.expectedResp != nil {
				if resp.Term != test.expectedResp.Term || resp.Success != test.expectedResp.Success {
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

func mockSendAppendEntries(ctrl *gomock.Controller, term int32) *rpc.MockInternalClientIface {
	rpcWrapper := rpc.RPCRespWrapper[*rpc.AppendEntriesResponse]{
		Err:  nil,
		Resp: &rpc.AppendEntriesResponse{Term: term, Success: true},
	}
	mockClient1 := rpc.NewMockInternalClientIface(ctrl)
	mockClient1.EXPECT().SendAppendEntries(
		gomock.Any(), gomock.Any()).Return(rpcWrapper).Times(1)
	return mockClient1
}

func mockSendAppendEntriesError(ctrl *gomock.Controller) *rpc.MockInternalClientIface {
	rpcWrapper := rpc.RPCRespWrapper[*rpc.AppendEntriesResponse]{
		Err:  errors.New("mock error"),
		Resp: nil,
	}
	mockClient1 := rpc.NewMockInternalClientIface(ctrl)
	mockClient1.EXPECT().SendAppendEntries(
		gomock.Any(), gomock.Any()).Return(rpcWrapper).AnyTimes()
	return mockClient1
}
