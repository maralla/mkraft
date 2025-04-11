package util

import (
	"math/rand"
	"syscall"
	"time"
)

const LEADER_HEARTBEAT_PERIOD_IN_MS = 100
const REUQEST_TIMEOUT_IN_MS = 200
const RPC_REUQEST_TIMEOUT_IN_MS = 200

// This non-determinisim is by design
// because sometimes randomness brings simplicity
// here it minimizes the possiblity of contention of leader and split vote
const ELECTION_TIMEOUT_MIN_IN_MS = 150
const ELECTION_TIMEOUT_MAX_IN_MS = 350

func GetRandomElectionTimeout() time.Duration {
	diff := ELECTION_TIMEOUT_MAX_IN_MS - ELECTION_TIMEOUT_MIN_IN_MS
	randomMs := rand.Intn(diff) + ELECTION_TIMEOUT_MIN_IN_MS
	return time.Duration(randomMs) * time.Millisecond
}

const HANDLE_CLIENT_COMMAND_BATCH = 10
const HANDLE_CLIENT_COMMAND_BUFFER = 1000

// leader configuration
const LEADER_BUFFER_SIZE = 1000

type Configuration struct {
	RPCRequestTimeout     time.Duration `json:"rpc_request_timeout"`
	RPCRequestTimeoutInMs int           `json:"rpc_request_timeout_in_ms"`
	// Election
	ElectionTimeout     time.Duration `json:"election_timeout"`
	ElectionTimeoutInMs int           `json:"election_timeout_in_ms"`
	// Leader
	LeaderHeartbeatPeriod     time.Duration `json:"leader_heartbeat_period"`
	LeaderHeartbeatPeriodInMs int           `json:"leader_heartbeat_period_in_ms"`
	LeaderBufferSize          int           `json:"leader_buffer_size"`
	// Client
	ClientCommandBufferSize int `json:"client_command_buffer_size"`
	// Node Meta
	NodeID string `json:"node_id"`
}

var (
	defaultConfig Configuration
	Config        Configuration
)

func init() {
	// read nodeID from env
	nodeID, found := syscall.Getenv("NODE_ID")
	if !found {
		panic("NODE_ID not found")
	}

	// todo: add configuration loading from env
	defaultConfig = Configuration{
		RPCRequestTimeout:         time.Duration(RPC_REUQEST_TIMEOUT_IN_MS) * time.Millisecond,
		RPCRequestTimeoutInMs:     RPC_REUQEST_TIMEOUT_IN_MS,
		ElectionTimeoutInMs:       ELECTION_TIMEOUT_MIN_IN_MS,
		LeaderHeartbeatPeriod:     time.Duration(LEADER_HEARTBEAT_PERIOD_IN_MS) * time.Millisecond,
		LeaderHeartbeatPeriodInMs: LEADER_HEARTBEAT_PERIOD_IN_MS,
		ClientCommandBufferSize:   HANDLE_CLIENT_COMMAND_BUFFER,
		LeaderBufferSize:          LEADER_BUFFER_SIZE,
		// todo: here seems to dynamic ?
		// NodeID: uuid.New().String(),
		NodeID: nodeID,
	}
	Config = defaultConfig
}

func GetConfig() Configuration {
	return Config
}
