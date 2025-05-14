package raft

import (
	"context"
	"testing"

	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func TestHandlers_SayHello(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockNode := NewMockNodeIface(ctrl)
	logger := zap.NewNop()
	handlers := NewHandlers(logger, mockNode)

	req := &rpc.HelloRequest{Name: "World"}
	resp, err := handlers.SayHello(context.Background(), req)
	if err != nil {
		t.Fatalf("SayHello() error = %v", err)
	}
	expectedMessage := "Hello World"
	if resp.Message != expectedMessage {
		t.Errorf("SayHello() got = %v, want %v", resp.Message, expectedMessage)
	}
}

func TestHandlers_RequestVote(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockNode := NewMockNodeIface(ctrl)
	logger := zap.NewNop()
	handlers := NewHandlers(logger, mockNode)

	req := &rpc.RequestVoteRequest{}
	respChan := make(chan *rpc.RPCRespWrapper[*rpc.RequestVoteResponse], 1)
	expectedResp := &rpc.RequestVoteResponse{VoteGranted: true}
	respChan <- &rpc.RPCRespWrapper[*rpc.RequestVoteResponse]{Resp: expectedResp}

	mockNode.EXPECT().VoteRequest(gomock.Any()).DoAndReturn(func(req *RequestVoteInternalReq) {
		req.RespWraper <- &rpc.RPCRespWrapper[*rpc.RequestVoteResponse]{Resp: expectedResp}
	})

	resp, err := handlers.RequestVote(context.Background(), req)
	if err != nil {
		t.Fatalf("RequestVote() error = %v", err)
	}
	if resp.VoteGranted != expectedResp.VoteGranted {
		t.Errorf("RequestVote() got = %v, want %v", resp.VoteGranted, expectedResp.VoteGranted)
	}
}

func TestHandlers_AppendEntries(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockNode := NewMockNodeIface(ctrl)
	logger := zap.NewNop()
	handlers := NewHandlers(logger, mockNode)

	req := &rpc.AppendEntriesRequest{}
	respChan := make(chan *rpc.RPCRespWrapper[*rpc.AppendEntriesResponse], 1)
	expectedResp := &rpc.AppendEntriesResponse{Success: true}
	respChan <- &rpc.RPCRespWrapper[*rpc.AppendEntriesResponse]{Resp: expectedResp}

	mockNode.EXPECT().AppendEntryRequest(gomock.Any()).DoAndReturn(func(req *AppendEntriesInternalReq) {
		req.RespWraper <- &rpc.RPCRespWrapper[*rpc.AppendEntriesResponse]{Resp: expectedResp}
	})

	resp, err := handlers.AppendEntries(context.Background(), req)
	if err != nil {
		t.Fatalf("AppendEntries() error = %v", err)
	}
	if resp.Success != expectedResp.Success {
		t.Errorf("AppendEntries() got = %v, want %v", resp.Success, expectedResp.Success)
	}
}
