package main

import (
	"context"
	"fmt"
	"time"

	"github.com/maki3cat/mkraft/raft"
	pb "github.com/maki3cat/mkraft/rpc"
	"github.com/maki3cat/mkraft/util"
)

type Server struct {
	pb.UnimplementedRaftServiceServer
}

func (s *Server) SayHello(_ context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	logger.Infof("Received: %v", in)
	return &pb.HelloReply{Message: "Hello " + in.GetName()}, nil
}

// timeout handling guideline: https://grpc.io/docs/guides/deadlines/
func (s *Server) RequestVote(_ context.Context, in *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	logger.Infof("RPC Server RequestVote received: %v", in)
	respChan := make(chan *pb.RPCRespWrapper[*pb.RequestVoteResponse], 1)
	internalReq := &raft.RequestVoteInternal{
		Request:    in,
		RespWraper: respChan,
	}

	raft.GetRaftNode().VoteRequest(internalReq)
	timeout := util.GetConfig().GetRPCRequestTimeout()
	timeoutTimer := time.NewTimer(timeout)
	select {
	case <-timeoutTimer.C:
		logger.Error("vote request, timeout getting response from raft server")
		internalReq.IsTimeout.Store(true)
		return nil, fmt.Errorf("server timeout")
	case resp := <-respChan:
		if resp.Err != nil {
			logger.Error("error in getting response from raft server", resp.Err)
			return nil, resp.Err
		}
		logger.Infof("RPC Server RequestVote respond: %v", resp.Resp)
		return resp.Resp, nil
	}
}

func (s *Server) AppendEntries(_ context.Context, in *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	logger.Infof("RPC Server, AppendEntries, Received: %v", in)
	respChan := make(chan *pb.RPCRespWrapper[*pb.AppendEntriesResponse], 1)
	internalReq := &raft.AppendEntriesInternal{
		Request:    in,
		RespWraper: respChan,
	}
	node := raft.GetRaftNode()
	node.AppendEntryRequest(internalReq)

	timeout := util.GetConfig().GetRPCRequestTimeout()
	timeoutTimer := time.NewTimer(timeout)
	select {
	case <-timeoutTimer.C:
		logger.Error("appendEntries, timeout getting response from raft server")
		internalReq.IsTimeout.Store(true)
		return nil, fmt.Errorf("server timeout")
	case resp := <-respChan:
		if resp.Err != nil {
			logger.Error("error in getting response from raft server", resp.Err)
			return nil, resp.Err
		}
		logger.Infof("RPC Server, AppendEntries, Respond: %v", resp.Resp)
		return resp.Resp, nil
	}
}
