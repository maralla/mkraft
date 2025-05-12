package raft

import (
	"context"

	pb "github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
)

type Handlers struct {
	pb.UnimplementedRaftServiceServer
	logger *zap.Logger
	node   NodeIface
}

func NewHandlers(logger *zap.Logger, node NodeIface) *Handlers {
	return &Handlers{
		logger: logger,
		node:   node,
	}
}

func (h *Handlers) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	return &pb.HelloReply{Message: "Hello " + in.GetName()}, nil
}

func (h *Handlers) RequestVote(ctx context.Context, in *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	respChan := make(chan *pb.RPCRespWrapper[*pb.RequestVoteResponse], 1)
	internalReq := &RequestVoteInternal{
		Request:    in,
		RespWraper: respChan,
	}
	// todo: should send the ctx into raft server so that it can notice the context is done
	h.node.VoteRequest(internalReq)
	resp := <-respChan
	if resp.Err != nil {
		h.logger.Error("error in getting response from raft server", zap.Error(resp.Err))
		return nil, resp.Err
	}
	h.logger.Info("RPC Server RequestVote respond", zap.Any("response", resp.Resp))
	return resp.Resp, nil
}

func (h *Handlers) AppendEntries(ctx context.Context, in *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	respChan := make(chan *pb.RPCRespWrapper[*pb.AppendEntriesResponse], 1)
	internalReq := &AppendEntriesInternal{
		Request:    in,
		RespWraper: respChan,
	}
	h.node.AppendEntryRequest(internalReq)

	// todo: should send the ctx into raft server so that it can notice the context is done
	resp := <-respChan
	if resp.Err != nil {
		h.logger.Error("error in getting response from raft server", zap.Error(resp.Err.(error)))
		return nil, resp.Err
	}
	h.logger.Info("RPC Server, AppendEntries, Respond", zap.Any("response", resp.Resp))
	return resp.Resp, nil
}
