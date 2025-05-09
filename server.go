package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/maki3cat/mkraft/raft"
	pb "github.com/maki3cat/mkraft/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	pb.UnimplementedRaftServiceServer
}

func (s *Server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	return &pb.HelloReply{Message: "Hello " + in.GetName()}, nil
}

func (s *Server) RequestVote(ctx context.Context, in *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	respChan := make(chan *pb.RPCRespWrapper[*pb.RequestVoteResponse], 1)
	internalReq := &raft.RequestVoteInternal{
		Request:    in,
		RespWraper: respChan,
	}
	// todo: should send the ctx into raft server so that it can notice the context is done
	raft.GetRaftNode().VoteRequest(internalReq)
	resp := <-respChan
	if resp.Err != nil {
		logger.Error("error in getting response from raft server", resp.Err)
		return nil, resp.Err
	}
	logger.Infof("RPC Server RequestVote respond: %v", resp.Resp)
	return resp.Resp, nil
}

func (s *Server) AppendEntries(ctx context.Context, in *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	respChan := make(chan *pb.RPCRespWrapper[*pb.AppendEntriesResponse], 1)
	internalReq := &raft.AppendEntriesInternal{
		Request:    in,
		RespWraper: respChan,
	}
	node := raft.GetRaftNode()
	node.AppendEntryRequest(internalReq)

	// todo: should send the ctx into raft server so that it can notice the context is done
	resp := <-respChan
	if resp.Err != nil {
		logger.Error("error in getting response from raft server", resp.Err)
		return nil, resp.Err
	}
	logger.Infof("RPC Server, AppendEntries, Respond: %v", resp.Resp)
	return resp.Resp, nil
}

func contextCheckInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.New(codes.Canceled, "context done").Err()
	}
	return handler(ctx, req)
}

func loggerInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	logger.Infof("gRPC request: %v", req)
	resp, err := handler(ctx, req)
	if err != nil {
		logger.Errorf("gRPC response error: %v", err)
	} else {
		logger.Infof("gRPC response: %v", resp)
	}
	return resp, err
}

func NewServer() *grpc.Server {
	serverOptions := grpc.ChainUnaryInterceptor(
		contextCheckInterceptor,
		loggerInterceptor)
	grpcServer := grpc.NewServer(serverOptions)
	pb.RegisterRaftServiceServer(grpcServer, &Server{})
	return grpcServer
}

func ServerGracefulShutdown(s *grpc.Server, waitingDuration time.Duration) {
	log.Println("Initiating grpc server graceful shutdown...")
	timer := time.AfterFunc(waitingDuration, func() {
		logger.Info("Server couldn't stop gracefully in time. Doing force stop.")
		s.Stop()
	})
	defer timer.Stop()
	s.GracefulStop()
	log.Println("Server stopped gracefully.")
}

func ServerStart(ctx context.Context, port int) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		logger.Fatalw("failed to listen", "error", err)
	}

	s := NewServer()
	go func() {
		logger.Infof("serving gRPC at %v...", port)
		if err := s.Serve(lis); err != nil {
			if errors.Is(err, grpc.ErrServerStopped) {
				logger.Info("gRPC server stopped")
				return
			} else {
				logger.Errorw("failed to serve", "error", err)
				panic(err)
			}
		}
	}()

	go func() {
		logger.Info("waiting for context cancellation or server quit...")
		<-ctx.Done()
		ServerGracefulShutdown(s, 5*time.Second)
		logger.Info("context canceled, stopping gRPC server...")
	}()
}
