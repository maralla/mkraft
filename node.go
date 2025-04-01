package main

import (
	"time"
)

// 1) THE STATES OF NODES
type NodeState int

const (
	StateFollower NodeState = iota
	StateCandidate
	StateLeader
)

func (state NodeState) String() string {
	switch state {
	case StateFollower:
		return "Follower"
	case StateCandidate:
		return "Candidate"
	case StateLeader:
		return "Leader"
	}
	// should never reach here
	return "Unknown State"
}

const TIMEOUT_IN_SEC = 5
const HEARTBEAT_IN_SEC = 4

type Node struct {
	heartbeatChannel     chan bool
	state                NodeState
	clientRequestChannel chan string // can be a batching of requests
}

func NewNode() *Node {
	return &Node{
		heartbeatChannel:     make(chan bool),
		state:                StateFollower,
		clientRequestChannel: make(chan string),
	}
}

func (node *Node) RunAsFollower() {
	for {
		// maki:I think here has a potential problem, because if the node is busy with accumulated heartbeats of previous time, it will
		// lost track of the election timeout
		timerForElection := time.NewTimer(time.Duration(TIMEOUT_IN_SEC) * time.Second)
		select {
		case <-node.heartbeatChannel:
			timerForElection.Stop()
			continue
		case <-timerForElection.C:
			// maki: Question, does the node still handles the heartbeat in the time of election?
			node.state = StateCandidate
			// todo: request vote RPC
			elected := false
			if elected {
				node.state = StateLeader
				go node.RunAsLeader()
				return
			} else {
				node.state = StateFollower
			}
		}
	}
}

func (node *Node) RunAsLeader() {
	for {
		// maki: here we need to send heartbeats to all the followers
		timerForHeartbeat := time.NewTimer(time.Duration(HEARTBEAT_IN_SEC) * time.Second)
		select {
		case request := <-node.clientRequestChannel:
			timerForHeartbeat.Stop()
			// send the request to all the followers,
			// and wait for the majority of them to respond
			appendLog(request)
			// todo: consensesus, not sure if not reached
			consensusReached := node.AppendEntries(request, getCurrentCommitID())
			if consensusReached {
				_ = commitLog(request)
			} else {
				resetLog()
			}
		case <-timerForHeartbeat.C:
			node.AppendEntries("", getCurrentCommitID())
		}
	}
}

func (node *Node) ReceiveHeartbeat() {
	node.heartbeatChannel <- true
}

// together with heartbeat
func (node *Node) AppendEntries(data string, lastCommitID int) bool {
	// maki: send heartbeat to all the others
	// maki: how to handle error in this case?
	return true
}

// gracefully stop the node
func (node *Node) Stop() {
	close(node.heartbeatChannel)
	close(node.clientRequestChannel)
}

func (node *Node) Start() {
	switch node.state {
	case StateFollower:
		go node.RunAsFollower()
	case StateLeader:
		go node.RunAsLeader()
	}
}
