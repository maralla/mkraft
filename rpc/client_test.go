package rpc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/maki3cat/mkraft/util"
	"github.com/stretchr/testify/assert"
	gomock "go.uber.org/mock/gomock"
)

func TestInternalClientImpl_SayHello(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := NewMockRaftServiceClient(ctrl)
	client := NewInternalClient(mockClient, "node1", "localhost:8080")

	req := &HelloRequest{}
	expectedResp := &HelloReply{Message: "mkraft it is!"}
	mockClient.EXPECT().
		SayHello(gomock.Any(), req).Return(expectedResp, nil).Times(1)
	resp, err := client.SayHello(context.Background(), req)
	assert.NoError(t, err)
	assert.Equal(t, expectedResp, resp)
}
func TestInternalClientImpl_SendAppendEntries(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := NewMockRaftServiceClient(ctrl)
	client := NewInternalClient(mockClient, "node1", "localhost:8080")

	req := &AppendEntriesRequest{}
	expectedResp := &AppendEntriesResponse{}
	mockClient.EXPECT().
		AppendEntries(gomock.Any(), req).Return(expectedResp, nil).Times(1)

	respWrapper := client.SendAppendEntries(context.Background(), req)

	assert.NoError(t, respWrapper.Err)
	assert.Equal(t, expectedResp, respWrapper.Resp)
}

func TestInternalClientImpl_SendAppendEntries_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := NewMockRaftServiceClient(ctrl)
	client := NewInternalClient(mockClient, "node1", "localhost:8080")

	req := &AppendEntriesRequest{}
	expectedErr := fmt.Errorf("RPC error")
	mockClient.EXPECT().
		AppendEntries(gomock.Any(), req).Return(nil, expectedErr).Times(1)

	respWrapper := client.SendAppendEntries(context.Background(), req)

	assert.Error(t, respWrapper.Err)
	assert.Nil(t, respWrapper.Resp)
	assert.Equal(t, expectedErr, respWrapper.Err)
}
func TestInternalClientImpl_SendRequestVoteWithRetries_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := NewMockRaftServiceClient(ctrl)
	client := NewInternalClient(mockClient, "node1", "localhost:8080")

	req := &RequestVoteRequest{}
	expectedResp := &RequestVoteResponse{}
	mockClient.EXPECT().
		RequestVote(gomock.Any(), req).Return(expectedResp, nil).Times(1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	respChan := client.SendRequestVoteWithRetries(ctx, req)
	respWrapper := <-respChan

	assert.NoError(t, respWrapper.Err)
	assert.Equal(t, expectedResp, respWrapper.Resp)
}

func TestInternalClientImpl_SendRequestVoteWithRetries_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := NewMockRaftServiceClient(ctrl)
	client := NewInternalClient(mockClient, "node1", "localhost:8080")

	minTime := util.GetConfig().GetMinRemainingTimeForRPC()

	req := &RequestVoteRequest{}
	expectedErr := fmt.Errorf("RPC error")
	mockClient.EXPECT().
		RequestVote(gomock.Any(), req).DoAndReturn(
		func(ctx context.Context, req *RequestVoteRequest, opts ...interface{}) (*RequestVoteResponse, error) {
			time.Sleep(time.Duration(float64(minTime) / 2))
			return nil, expectedErr
		},
	).MinTimes(2)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(float64(minTime)*2))
	defer cancel()

	respChan := client.SendRequestVoteWithRetries(ctx, req)
	respWrapper := <-respChan

	assert.Error(t, respWrapper.Err)
	assert.Nil(t, respWrapper.Resp)
}

func TestInternalClientImpl_SendRequestVoteWithRetries_RetrySuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := NewMockRaftServiceClient(ctrl)
	client := NewInternalClient(mockClient, "node1", "localhost:8080")

	req := &RequestVoteRequest{}
	expectedErr := fmt.Errorf("RPC error")
	expectedResp := &RequestVoteResponse{}

	gomock.InOrder(
		mockClient.EXPECT().
			RequestVote(gomock.Any(), req).Return(nil, expectedErr).Times(2),
		mockClient.EXPECT().
			RequestVote(gomock.Any(), req).Return(expectedResp, nil).Times(1),
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	respChan := client.SendRequestVoteWithRetries(ctx, req)
	respWrapper := <-respChan

	assert.NoError(t, respWrapper.Err)
	assert.Equal(t, expectedResp, respWrapper.Resp)
}
