package rpc

import (
	"context"
	"fmt"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	util "github.com/maki3cat/mkraft/util"
)

var logger = util.GetSugarLogger()

type RPCRespWrapper[T RPCResponse] struct {
	Err  error
	Resp T
}

type RPCResponse interface {
	*AppendEntriesResponse | *RequestVoteResponse
}

type InternalClientIface interface {

	// send request vote is keep retrying until the context is done or the response is received
	SendRequestVoteWithRetries(ctx context.Context, req *RequestVoteRequest) chan RPCRespWrapper[*RequestVoteResponse]

	// send append entries is one simple sync rpc call with rpc timeout
	SendAppendEntries(ctx context.Context, req *AppendEntriesRequest) RPCRespWrapper[*AppendEntriesResponse]

	SayHello(ctx context.Context, req *HelloRequest) (*HelloReply, error)

	String() string

	Close() error
}

type InternalClientImpl struct {
	nodeId    string
	nodeAddr  string
	rawClient RaftServiceClient
	conn      *grpc.ClientConn
}

func NewInternalClient(nodeID, nodeAddr string) (InternalClientIface, error) {
	conn, err := NewClientConn(&nodeAddr)
	if err != nil {
		logger.Errorw("failed to create gRPC connection", "nodeID", nodeID, "error", err)
		return nil, err
	}
	raftServiceClient := NewRaftServiceClient(conn)
	return &InternalClientImpl{
		nodeId:    nodeID,
		nodeAddr:  nodeAddr,
		rawClient: raftServiceClient,
		conn:      conn,
	}, nil
}

func (rc *InternalClientImpl) Close() error {
	if rc.conn != nil {
		err := rc.conn.Close()
		if err != nil {
			logger.Errorw("failed to close gRPC connection", "nodeID", rc.nodeId, "error", err)
		}
		return err
	}
	logger.Warn("gRPC connection is nil, cannot close")
	return nil
}

func (rc *InternalClientImpl) String() string {
	return fmt.Sprintf("InternalClientImpl: %s, %s", rc.nodeId, rc.nodeAddr)
}

func (rc *InternalClientImpl) SayHello(ctx context.Context, req *HelloRequest) (*HelloReply, error) {
	return rc.rawClient.SayHello(ctx, req)
}

func (rc *InternalClientImpl) SendAppendEntries(ctx context.Context, req *AppendEntriesRequest) RPCRespWrapper[*AppendEntriesResponse] {
	resp, err := rc.syncCallAppendEntries(ctx, req)
	wrapper := RPCRespWrapper[*AppendEntriesResponse]{
		Resp: resp,
		Err:  err,
	}
	return wrapper
}

// the context shall be timed out in election timeout period
func (rc *InternalClientImpl) SendRequestVoteWithRetries(ctx context.Context, req *RequestVoteRequest) chan RPCRespWrapper[*RequestVoteResponse] {
	logger.Debugw("send SendRequestVote", "req", req)
	out := make(chan RPCRespWrapper[*RequestVoteResponse], 1)

	retriedRPC := func() {
		singleResChan := rc.asyncCallRequestVote(ctx, req)
		for {
			select {
			case <-ctx.Done():
				out <- RPCRespWrapper[*RequestVoteResponse]{
					Err: fmt.Errorf("%s", "election timeout to receive any non-error response")}
				return
			case resp := <-singleResChan:
				if resp.Err != nil {
					deadline, ok := ctx.Deadline()
					logger.Debugw("retrying RPC, deadline:", "deadline", deadline)
					if ok && time.Until(deadline) < util.GetConfig().GetMinRemainingTimeForRPC() {
						out <- RPCRespWrapper[*RequestVoteResponse]{
							Err: fmt.Errorf("%s", "election timeout to receive any non-error response")}
						return
					} else {
						logger.Errorw("need retry, RPC error:", "to", rc, "error", resp.Err)
						singleResChan = rc.asyncCallRequestVote(ctx, req)
						continue
					}
				} else {
					out <- resp
					return
				}
			}
		}
	}
	go retriedRPC()
	return out
}

func (rc *InternalClientImpl) asyncCallRequestVote(ctx context.Context, req *RequestVoteRequest) chan RPCRespWrapper[*RequestVoteResponse] {
	singleResChan := make(chan RPCRespWrapper[*RequestVoteResponse], 1) // must be buffered
	go func() {
		resp, err := rc.syncCallRequestVote(ctx, req)
		wrapper := RPCRespWrapper[*RequestVoteResponse]{
			Resp: resp,
			Err:  err,
		}
		singleResChan <- wrapper
	}()
	return singleResChan
}

// should be called when ctx.deadline can handle the rpc timeout
func (rc *InternalClientImpl) syncCallAppendEntries(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	rpcTimeout := util.GetConfig().GetRPCRequestTimeout()
	singleCallCtx, singleCallCancel := context.WithTimeout(ctx, rpcTimeout)
	defer singleCallCancel()

	resp, err := rc.rawClient.AppendEntries(singleCallCtx, req)
	if err != nil {
		code := status.Code(err)
		logger.Errorw("single RPC error in SendAppendEntries:", "to", rc.rawClient, "error", err, "code", code)
	} else {
		logger.Debugw("single RPC SendAppendEntries response:", "member", rc, "response", resp)
	}
	return resp, err
}

// should be called when ctx.deadline can handle the rpc timeout
func (rc *InternalClientImpl) syncCallRequestVote(ctx context.Context, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	rpcTimeout := util.GetConfig().GetRPCRequestTimeout()
	singleCallCtx, singleCallCancel := context.WithTimeout(ctx, rpcTimeout)
	defer singleCallCancel()

	resp, err := rc.rawClient.RequestVote(singleCallCtx, req)
	if err != nil {
		code := status.Code(err)
		logger.Errorw("single RPC error in SendAppendEntries:", "to", rc.rawClient, "error", err, "code", code)
	} else {
		logger.Debugw("single RPC response:", "member", rc, "response", resp)
	}
	return resp, err
}

func loggerClientInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	logger.Debugf("RPC call: %s, request: %v", method, req)
	start := time.Now()
	err := invoker(ctx, method, req, reply, cc, opts...)
	end := time.Now()
	logger.Infof("RPC call: %s, duration: %v, error: %v", method, end.Sub(start), err)
	return err
}

func timeoutClientInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	rpcTimeout := util.GetConfig().GetRPCRequestTimeout()
	singleCallCtx, singleCallCancel := context.WithTimeout(ctx, rpcTimeout)
	defer singleCallCancel()
	err := invoker(singleCallCtx, method, req, reply, cc, opts...)
	return err
}

func NewClientConn(addr *string) (*grpc.ClientConn, error) {
	clientOptions := []grpc.DialOption{
		// todo: add creds
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(loggerClientInterceptor),
		grpc.WithUnaryInterceptor(timeoutClientInterceptor),
	}
	conn, err := grpc.NewClient(*addr, clientOptions...)
	if err != nil {
		logger.Errorw("failed to connect to gRPC server", "target", addr, "error", err)
		return nil, err
	}
	return conn, nil
}
