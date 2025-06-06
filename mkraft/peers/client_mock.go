// Code generated by MockGen. DO NOT EDIT.
// Source: mkraft/peers/client.go
//
// Generated by this command:
//
//	mockgen -source=mkraft/peers/client.go -destination=./mkraft/peers/client_mock.go -package peers
//

// Package peers is a generated GoMock package.
package peers

import (
	context "context"
	reflect "reflect"

	utils "github.com/maki3cat/mkraft/mkraft/utils"
	rpc "github.com/maki3cat/mkraft/rpc"
	gomock "go.uber.org/mock/gomock"
)

// MockInternalClientIface is a mock of InternalClientIface interface.
type MockInternalClientIface struct {
	ctrl     *gomock.Controller
	recorder *MockInternalClientIfaceMockRecorder
	isgomock struct{}
}

// MockInternalClientIfaceMockRecorder is the mock recorder for MockInternalClientIface.
type MockInternalClientIfaceMockRecorder struct {
	mock *MockInternalClientIface
}

// NewMockInternalClientIface creates a new mock instance.
func NewMockInternalClientIface(ctrl *gomock.Controller) *MockInternalClientIface {
	mock := &MockInternalClientIface{ctrl: ctrl}
	mock.recorder = &MockInternalClientIfaceMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockInternalClientIface) EXPECT() *MockInternalClientIfaceMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockInternalClientIface) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockInternalClientIfaceMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockInternalClientIface)(nil).Close))
}

// SayHello mocks base method.
func (m *MockInternalClientIface) SayHello(ctx context.Context, req *rpc.HelloRequest) (*rpc.HelloReply, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SayHello", ctx, req)
	ret0, _ := ret[0].(*rpc.HelloReply)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SayHello indicates an expected call of SayHello.
func (mr *MockInternalClientIfaceMockRecorder) SayHello(ctx, req any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SayHello", reflect.TypeOf((*MockInternalClientIface)(nil).SayHello), ctx, req)
}

// SendAppendEntries mocks base method.
func (m *MockInternalClientIface) SendAppendEntries(ctx context.Context, req *rpc.AppendEntriesRequest) utils.RPCRespWrapper[*rpc.AppendEntriesResponse] {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SendAppendEntries", ctx, req)
	ret0, _ := ret[0].(utils.RPCRespWrapper[*rpc.AppendEntriesResponse])
	return ret0
}

// SendAppendEntries indicates an expected call of SendAppendEntries.
func (mr *MockInternalClientIfaceMockRecorder) SendAppendEntries(ctx, req any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SendAppendEntries", reflect.TypeOf((*MockInternalClientIface)(nil).SendAppendEntries), ctx, req)
}

// SendRequestVoteWithRetries mocks base method.
func (m *MockInternalClientIface) SendRequestVoteWithRetries(ctx context.Context, req *rpc.RequestVoteRequest) chan utils.RPCRespWrapper[*rpc.RequestVoteResponse] {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SendRequestVoteWithRetries", ctx, req)
	ret0, _ := ret[0].(chan utils.RPCRespWrapper[*rpc.RequestVoteResponse])
	return ret0
}

// SendRequestVoteWithRetries indicates an expected call of SendRequestVoteWithRetries.
func (mr *MockInternalClientIfaceMockRecorder) SendRequestVoteWithRetries(ctx, req any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SendRequestVoteWithRetries", reflect.TypeOf((*MockInternalClientIface)(nil).SendRequestVoteWithRetries), ctx, req)
}

// String mocks base method.
func (m *MockInternalClientIface) String() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "String")
	ret0, _ := ret[0].(string)
	return ret0
}

// String indicates an expected call of String.
func (mr *MockInternalClientIfaceMockRecorder) String() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "String", reflect.TypeOf((*MockInternalClientIface)(nil).String))
}
