package util

import (
	"encoding/json"
	"math/rand"
	"time"
)

var (
	theConf *Config
)

// maki: haven't found easy way to lock the writing of theConf without copying the value
// so careful coding is needed
func GetConfig() *Config {
	return theConf
}

// todo: in the near future, we need update membership dynamically;
// todo: in the remote future, we need to update the config dynamically
func InitConf(membership *MembershipConfig) {
	theConf = CreateDefaultConf()
	theConf.MembershipConfig = *membership
	sugarLogger.Infof("init the config to %s", theConf)
}

func CreateDefaultConf() *Config {
	return &Config{
		RPCRequestTimeoutInMs:     RPC_REUQEST_TIMEOUT_IN_MS,
		ElectionTimeoutMinInMs:    ELECTION_TIMEOUT_MIN_IN_MS,
		ElectionTimeoutMaxInMs:    ELECTION_TIMEOUT_MAX_IN_MS,
		LeaderHeartbeatPeriodInMs: LEADER_HEARTBEAT_PERIOD_IN_MS,
		LeaderBufferSize:          LEADER_BUFFER_SIZE,
		ClientCommandBufferSize:   CLIENT_COMMAND_BUFFER_SIZE,
		ClientCommandBatchSize:    CLIENT_COMMAND_BATCH_SIZE,
	}
}

// DEFAULT VALUES of configuration
const CLIENT_COMMAND_BATCH_SIZE = 10
const CLIENT_COMMAND_BUFFER_SIZE = 1000

const LEADER_BUFFER_SIZE = 1000
const LEADER_HEARTBEAT_PERIOD_IN_MS = 100

const RPC_REUQEST_TIMEOUT_IN_MS = 200

const ELECTION_TIMEOUT_MIN_IN_MS = 150
const ELECTION_TIMEOUT_MAX_IN_MS = 350

type Config struct {
	// RPC timeout
	RPCRequestTimeoutInMs int `json:"rpc_request_timeout_in_ms"`

	// Election timeout
	ElectionTimeoutMinInMs int `json:"election_timeout_min_in_ms"`
	ElectionTimeoutMaxInMs int `json:"election_timeout_max_in_ms"`

	// Leader
	LeaderHeartbeatPeriodInMs int `json:"leader_heartbeat_period_in_ms"`
	LeaderBufferSize          int `json:"leader_buffer_size"`

	// Client
	ClientCommandBufferSize int `json:"client_command_buffer_size"`
	ClientCommandBatchSize  int `json:"client_command_batch_size"`

	// Node Meta
	MembershipConfig
}

func (c *Config) GetRPCRequestTimeout() time.Duration {
	return time.Duration(c.RPCRequestTimeoutInMs) * time.Millisecond
}

func (c *Config) GetElectionTimeout() time.Duration {
	diff := c.ElectionTimeoutMaxInMs - c.ElectionTimeoutMinInMs
	randomMs := rand.Intn(diff) + ELECTION_TIMEOUT_MIN_IN_MS
	return time.Duration(randomMs) * time.Millisecond
}

func (c *Config) GetLeaderHeartbeatPeriod() time.Duration {
	return time.Duration(c.LeaderHeartbeatPeriodInMs) * time.Millisecond
}

func (c *Config) GetLeaderBufferSize() int {
	return c.LeaderBufferSize
}

func (c *Config) GetClientCommandBufferSize() int {
	return c.ClientCommandBufferSize
}

func (c *Config) GetClientCommandBatchSize() int {
	return c.ClientCommandBatchSize
}

func (c *Config) String() string {
	jsonStr, _ := json.Marshal(c)
	return string(jsonStr)
}

type MembershipConfig struct {
	NodeID     string       `json:"node_id"`
	Membership []NodeConfig `json:"membership"`
}

type NodeConfig struct {
	NodeID  string `json:"node_id"`
	NodeURI string `json:"node_uri"`
}
