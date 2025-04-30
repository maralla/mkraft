package rpc

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	gomock "go.uber.org/mock/gomock"
)

func TestSyncCallRequestVote_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := NewMockRaftServiceClient(ctrl)

	ctx := context.Background()
	req := &RequestVoteRequest{
		// fill necessary fields if needed
	}
	expectedResp := &RequestVoteResponse{
		Term:        int32(rand.Int()),
		VoteGranted: true,
	}

	// Set up expectation
	mockClient.EXPECT().
		RequestVote(gomock.Any(), req).
		Return(expectedResp, nil)

	resp, err := SyncCallRequestVote(ctx, mockClient, req)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.VoteGranted)
}

func TestSyncCallRequestVote_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := NewMockRaftServiceClient(ctrl)

	ctx := context.Background()
	req := &RequestVoteRequest{
		// fill necessary fields if needed
	}
	expectedErr := errors.New("RPC failed")

	// Set up expectation
	mockClient.EXPECT().
		RequestVote(gomock.Any(), req).
		Return(nil, expectedErr)

	resp, err := SyncCallRequestVote(ctx, mockClient, req)

	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, expectedErr, err)
}

func TestAsyncCallRequestVote_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := NewMockRaftServiceClient(ctrl)

	ctx := context.Background()
	req := &RequestVoteRequest{}
	expectedResp := &RequestVoteResponse{
		Term:        int32(rand.Int()),
		VoteGranted: true,
	}

	// Expect the SyncCallRequestVote to call RequestVote
	mockClient.EXPECT().
		RequestVote(gomock.Any(), req).
		Return(expectedResp, nil)

	respChan := AsyncCallRequestVote(ctx, mockClient, req)

	select {
	case wrapper := <-respChan:
		assert.NoError(t, wrapper.Err)
		assert.NotNil(t, wrapper.Resp)
		assert.True(t, wrapper.Resp.VoteGranted)
		assert.Equal(t, wrapper.Resp.Term, expectedResp.Term)
	case <-time.After(time.Second): // 1s timeout just in case of deadlock
		t.Fatal("Timeout waiting for AsyncCallRequestVote response")
	}
}

func TestAsyncCallRequestVote_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := NewMockRaftServiceClient(ctrl)

	ctx := context.Background()
	req := &RequestVoteRequest{}
	expectedErr := errors.New("RPC failed")

	mockClient.EXPECT().
		RequestVote(gomock.Any(), req).
		Return(nil, expectedErr)

	respChan := AsyncCallRequestVote(ctx, mockClient, req)

	select {
	case wrapper := <-respChan:
		assert.Error(t, wrapper.Err)
		assert.Nil(t, wrapper.Resp)
		assert.Equal(t, expectedErr, wrapper.Err)
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for AsyncCallRequestVote response")
	}
}

func TestSendRequestVote_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := NewMockRaftServiceClient(ctrl)

	cli := &InternalClientImpl{
		rawClient: mockClient,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	req := &RequestVoteRequest{}
	expectedResp := &RequestVoteResponse{
		VoteGranted: true,
		Term:        int32(rand.Int()),
	}

	// Expect first call to succeed
	mockClient.EXPECT().
		RequestVote(gomock.Any(), req).
		Return(expectedResp, nil)

	respChan := cli.SendRequestVote(ctx, req)

	select {
	case wrapper := <-respChan:
		assert.NoError(t, wrapper.Err)
		assert.NotNil(t, wrapper.Resp)
		assert.True(t, wrapper.Resp.VoteGranted)
		assert.Equal(t, wrapper.Resp.Term, expectedResp.Term)
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for SendRequestVote response")
	}
}

func TestSendRequestVote_RetrySuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := NewMockRaftServiceClient(ctrl)

	cli := &InternalClientImpl{
		rawClient: mockClient,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	req := &RequestVoteRequest{}
	expectedResp := &RequestVoteResponse{
		Term:        int32(rand.Int()),
		VoteGranted: true}

	// First call fails, second call succeeds
	gomock.InOrder(
		mockClient.EXPECT().
			RequestVote(gomock.Any(), req).
			Return(nil, errors.New("temporary RPC error")),
		mockClient.EXPECT().
			RequestVote(gomock.Any(), req).
			Return(expectedResp, nil),
	)

	respChan := cli.SendRequestVote(ctx, req)

	select {
	case wrapper := <-respChan:
		assert.NoError(t, wrapper.Err)
		assert.NotNil(t, wrapper.Resp)
		assert.True(t, wrapper.Resp.VoteGranted)
		assert.Equal(t, wrapper.Resp.Term, expectedResp.Term)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for SendRequestVote response")
	}
}

func TestSendRequestVote_ContextTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := NewMockRaftServiceClient(ctrl)

	cli := &InternalClientImpl{
		rawClient: mockClient,
	}

	// Very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	req := &RequestVoteRequest{}

	// Make the mock delay to simulate slowness
	mockClient.EXPECT().
		RequestVote(gomock.Any(), req).
		DoAndReturn(func(ctx context.Context, req *RequestVoteRequest, opts ...interface{}) (*RequestVoteResponse, error) {
			time.Sleep(50 * time.Millisecond) // sleep longer than ctx timeout
			return nil, context.DeadlineExceeded
		})

	respChan := cli.SendRequestVote(ctx, req)

	select {
	case wrapper := <-respChan:
		assert.Error(t, wrapper.Err)
		assert.Nil(t, wrapper.Resp)
		assert.Contains(t, wrapper.Err.Error(), "election timeout")
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for SendRequestVote response")
	}
}
