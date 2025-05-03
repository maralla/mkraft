package raft

import (
	"context"
	"errors"

	"github.com/maki3cat/mkraft/rpc"
	"github.com/maki3cat/mkraft/util"
)

var logger = util.GetSugarLogger()

func calculateIfMajorityMet(total, peerVoteAccumulated int) bool {
	return (peerVoteAccumulated + 1) >= total/2+1
}

// assumes total > peersCount
// todo should make sure this is guaranteed somewhere else
func calculateIfAlreadyFail(total, peersCount, peerVoteAccumulated, voteFailed int) bool {
	majority := total/2 + 1
	majorityNeeded := majority - 1
	needed := majorityNeeded - peerVoteAccumulated
	possibleRespondant := peersCount - voteFailed - peerVoteAccumulated
	return possibleRespondant < needed
}

type AppendEntriesConsensusResp struct {
	Term    int32
	Success bool
}

type MajorityRequestVoteResp struct {
	Term        int32
	VoteGranted bool
}

var consensus ConsensusIface = &ConsensusImpl{}

type ConsensusIface interface {
	RequestVoteSendForConsensus(ctx context.Context, request *rpc.RequestVoteRequest) (*MajorityRequestVoteResp, error)
	AppendEntriesSendForConsensus(ctx context.Context, request *rpc.AppendEntriesRequest) (*AppendEntriesConsensusResp, error)
}

type ConsensusImpl struct {
}

func (c *ConsensusImpl) RequestVoteSendForConsensus(ctx context.Context, request *rpc.RequestVoteRequest) (*MajorityRequestVoteResp, error) {

	total := memberMgr.GetMemberCount()
	peerClients, err := memberMgr.GetAllPeerClients()
	if err != nil {
		logger.Error("error in getting all peer clients", err)
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
			timeout := util.GetConfig().GetElectionTimeout()
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
				logger.Errorf("error in sending request vote to one node: %v", err)
				if calculateIfAlreadyFail(total, peersCount, peerVoteAccumulated, voteFailed) {
					return nil, errors.New("majority of nodes failed to respond")
				} else {
					continue
				}
			} else {
				resp := res.Resp
				// if someone responds with a term greater than the current term
				if resp.Term > request.Term {
					logger.Info("peer's term is greater than the node's current term")
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
					logger.Error("invairant failed, smaller term is not overwritten by larger term")
					panic("this should not happen, the consensus algorithm is not implmented correctly")
				}
			}
		case <-ctx.Done():
			logger.Info("context done")
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

	total := memberMgr.GetMemberCount()
	peerClients, err := memberMgr.GetAllPeerClients()
	if err != nil {
		logger.Error("error in getting all peer clients", err)
		return nil, err
	}
	if !calculateIfMajorityMet(total, len(peerClients)) {
		logger.Error("not enough peer clients found")
		return nil, errors.New("not enough peer clients found")
	}

	allRespChan := make(chan rpc.RPCRespWrapper[*rpc.AppendEntriesResponse], len(peerClients))
	for _, member := range peerClients {
		memberHandle := member
		// FAN-OUT
		go func() {
			ctxWithTimeout, cancel := context.WithTimeout(ctx, util.GetConfig().GetElectionTimeout())
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
				logger.Warn("error returned from appendEntries", err)
				failAccumulated++
				continue
			} else {
				resp := res.Resp
				if resp.Term > request.Term {
					logger.Info("peer's term is greater than current term")
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
							logger.Warn("another node with same term becomes the leader")
							return &AppendEntriesConsensusResp{
								Term:    resp.Term,
								Success: false,
							}, nil
						}
					}
				}
				if resp.Term < request.Term {
					logger.Errorw(
						"invairant failed, smaller term is not overwritten by larger term",
						"request", request,
						"response", resp)
					panic("this should not happen, the consensus algorithm is not implmented correctly")
				}
			}
		case <-ctx.Done():
			logger.Info("context canceled")
			return nil, errors.New("context canceled")
		}
	}
	return nil, errors.New("this should not happen, the consensus algorithm is not implmented correctly")
}
