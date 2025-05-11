package raft

import (
	"context"
	"errors"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
)

var _ ConsensusIface = (*ConsensusImpl)(nil)

func NewConsensus(logger *zap.Logger, membershipMgr MembershipMgrIface, cfg common.ConfigIface) ConsensusIface {
	return &ConsensusImpl{
		logger:        logger,
		membershipMgr: membershipMgr,
		cfg:           cfg,
	}
}

type AppendEntriesConsensusResp struct {
	Term    int32
	Success bool
}

type MajorityRequestVoteResp struct {
	Term        int32
	VoteGranted bool
}

type ConsensusIface interface {
	RequestVoteSendForConsensus(ctx context.Context, request *rpc.RequestVoteRequest) (*MajorityRequestVoteResp, error)
	AppendEntriesSendForConsensus(ctx context.Context, request *rpc.AppendEntriesRequest) (*AppendEntriesConsensusResp, error)
}

type ConsensusImpl struct {
	logger        *zap.Logger
	membershipMgr MembershipMgrIface
	cfg           common.ConfigIface
}

func (c *ConsensusImpl) RequestVoteSendForConsensus(ctx context.Context, request *rpc.RequestVoteRequest) (*MajorityRequestVoteResp, error) {

	total := c.membershipMgr.GetMemberCount()
	peerClients, err := c.membershipMgr.GetAllPeerClients()
	if err != nil {
		c.logger.Error("error in getting all peer clients", zap.Error(err))
		return nil, err
	}
	if !calculateIfMajorityMet(total, len(peerClients)) {
		return nil, errors.New("no member clients found")
	}

	peersCount := len(peerClients)
	resChan := make(chan rpc.RPCRespWrapper[*rpc.RequestVoteResponse], peersCount) // buffered with len(members) to prevent goroutine leak
	for _, member := range peerClients {
		// FAN-OUT
		// maki: todo topic for go gynastics
		go func() {
			memberHandle := member
			timeout := c.cfg.GetElectionTimeout()
			ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			// FAN-IN
			resChan <- <-memberHandle.SendRequestVoteWithRetries(ctxWithTimeout, request)
		}()
	}

	// FAN-IN WITH STOPPING SHORT
	peerVoteAccumulated := 0 // the node itself is counted as a vote
	voteFailed := 0
	for range peersCount {
		select {
		case res := <-resChan:
			if err := res.Err; err != nil {
				voteFailed++
				c.logger.Error("error in sending request vote to one node", zap.Error(err))
				if calculateIfAlreadyFail(total, peersCount, peerVoteAccumulated, voteFailed) {
					return nil, errors.New("majority of nodes failed to respond")
				} else {
					continue
				}
			} else {
				resp := res.Resp
				// if someone responds with a term greater than the current term
				if resp.Term > request.Term {
					c.logger.Info("peer's term is greater than the node's current term")
					return &MajorityRequestVoteResp{
						Term:        resp.Term,
						VoteGranted: false,
					}, nil
				}
				if resp.Term == request.Term {
					if resp.VoteGranted {
						// won the election
						peerVoteAccumulated++
						if calculateIfMajorityMet(total, peerVoteAccumulated) {
							return &MajorityRequestVoteResp{
								Term:        request.Term,
								VoteGranted: true,
							}, nil
						}
					} else {
						voteFailed++
						if calculateIfAlreadyFail(total, peersCount, peerVoteAccumulated, voteFailed) {
							return nil, errors.New("majority of nodes failed to respond")
						}
					}
				}
				if resp.Term < request.Term {
					c.logger.Error("invairant failed, smaller term is not overwritten by larger term")
					panic("this should not happen, the consensus algorithm is not implmented correctly")
				}
			}
		case <-ctx.Done():
			c.logger.Info("context done")
			return nil, errors.New("context done")
		}
	}
	return nil, errors.New("this should not happen, the consensus algorithm is not implmented correctly")
}

/*
* Synchronous API:
* contains async workers to call all peers appendEtnries
* and wait for the majority of them to respond
* the expected timeout is just simple one-round trip timeout configuration
 */
func (c *ConsensusImpl) AppendEntriesSendForConsensus(
	ctx context.Context, request *rpc.AppendEntriesRequest) (*AppendEntriesConsensusResp, error) {

	total := c.membershipMgr.GetMemberCount()
	peerClients, err := c.membershipMgr.GetAllPeerClients()
	if err != nil {
		c.logger.Error("error in getting all peer clients", zap.Error(err))
		return nil, err
	}
	if !calculateIfMajorityMet(total, len(peerClients)) {
		c.logger.Error("not enough peer clients found")
		return nil, errors.New("not enough peer clients found")
	}

	allRespChan := make(chan rpc.RPCRespWrapper[*rpc.AppendEntriesResponse], len(peerClients))
	for _, member := range peerClients {
		memberHandle := member
		// FAN-OUT
		go func() {
			ctxWithTimeout, cancel := context.WithTimeout(ctx, c.cfg.GetElectionTimeout())
			defer cancel()
			// FAN-IN
			allRespChan <- memberHandle.SendAppendEntries(ctxWithTimeout, request)
		}()
	}

	// STOPPING SHORT
	peerVoteAccumulated := 0
	failAccumulated := 0

	peersCount := len(peerClients)
	for range peersCount {
		select {
		case res := <-allRespChan:
			if err := res.Err; err != nil {
				c.logger.Warn("error returned from appendEntries", zap.Error(err))
				failAccumulated++
				continue
			} else {
				resp := res.Resp
				if resp.Term > request.Term {
					c.logger.Info("peer's term is greater than current term")
					return &AppendEntriesConsensusResp{
						Term:    resp.Term,
						Success: false,
					}, nil
				}
				if resp.Term == request.Term {
					if resp.Success {
						peerVoteAccumulated++
						if calculateIfMajorityMet(total, peerVoteAccumulated) {
							return &AppendEntriesConsensusResp{
								Term:    request.Term,
								Success: true,
							}, nil
						}
					} else {
						failAccumulated++
						if calculateIfAlreadyFail(total, peersCount, peerVoteAccumulated, failAccumulated) {
							c.logger.Warn("another node with same term becomes the leader")
							return &AppendEntriesConsensusResp{
								Term:    resp.Term,
								Success: false,
							}, nil
						}
					}
				}
				if resp.Term < request.Term {
					c.logger.Error(
						"invairant failed, smaller term is not overwritten by larger term",
						zap.String("request", request.String()),
						zap.String("response", resp.String()))
					panic("this should not happen, the consensus algorithm is not implmented correctly")
				}
			}
		case <-ctx.Done():
			c.logger.Info("context canceled")
			return nil, errors.New("context canceled")
		}
	}
	return nil, errors.New("this should not happen, the consensus algorithm is not implmented correctly")
}

func calculateIfMajorityMet(total, peerVoteAccumulated int) bool {
	return (peerVoteAccumulated + 1) >= total/2+1
}

func calculateIfAlreadyFail(total, peersCount, peerVoteAccumulated, voteFailed int) bool {
	majority := total/2 + 1
	majorityNeeded := majority - 1
	needed := majorityNeeded - peerVoteAccumulated
	possibleRespondant := peersCount - voteFailed - peerVoteAccumulated
	return possibleRespondant < needed
}
