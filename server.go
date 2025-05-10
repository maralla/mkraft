package mkraft

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"go.uber.org/zap"
	_ "google.golang.org/grpc/encoding/gzip"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/raft"
	pb "github.com/maki3cat/mkraft/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func NewServer(cfg common.ConfigIface, logger *zap.Logger) *Server {
	server := &Server{
		logger: logger,
		cfg:    cfg,
	}
	serverOptions := grpc.ChainUnaryInterceptor(
		server.contextCheckInterceptor,
		server.loggerInterceptor)
	server.grpcServer = grpc.NewServer(serverOptions)

	pb.RegisterRaftServiceServer(server.grpcServer, server.handler)
	return server
}

type Server struct {
	logger     *zap.Logger
	cfg        common.ConfigIface
	grpcServer *grpc.Server
	handler    *raft.Handlers
	node       *raft.Node
}

func (s *Server) contextCheckInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.New(codes.Canceled, "context done").Err()
	}
	return handler(ctx, req)
}

func (s *Server) loggerInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	s.logger.Info("gRPC request", zap.Any("request", req))
	resp, err := handler(ctx, req)
	if err != nil {
		s.logger.Error("gRPC response error", zap.Error(err))
	} else {
		s.logger.Info("gRPC response", zap.Any("response", resp))
	}
	return resp, err
}

func (s *Server) Stop() {
	waitingDuration := s.cfg.GetGracefulShutdownTimeout()
	timer := time.AfterFunc(waitingDuration, func() {
		s.grpcServer.Stop()
	})
	defer timer.Stop()
	s.grpcServer.GracefulStop()
}

func (s *Server) Start(ctx context.Context) error {
	port := s.cfg.GetMembership().CurrentPort
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	go func() {
		if err := s.grpcServer.Serve(lis); err != nil {
			if errors.Is(err, grpc.ErrServerStopped) {
				s.logger.Info("gRPC server stopped")
				return
			} else {
				s.logger.Error("failed to serve", zap.Error(err))
				panic(err)
			}
		}
	}()

	go func() {
		s.logger.Info("waiting for context cancellation or server quit...")
		<-ctx.Done()
		s.Stop()
		s.logger.Info("context canceled, stopping gRPC server...")
	}()
	return nil
}
