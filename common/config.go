package common

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"

	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"
)

// current design: membership is not a part of the configuration
var (
	theConf ConfigIface
)

func InitConf() {
	theConf = CreateDefaultConf()
	sugarLogger.Infof("init the config to %s", theConf)
}

func GetConfig() ConfigIface {
	return theConf
}

func CreateDefaultConf() *Config {
	return &Config{
		RaftNodeRequestBufferSize:  RAFT_NODE_REQUEST_BUFFER_SIZE,
		RPCRequestTimeoutInMs:      RPC_REUQEST_TIMEOUT_IN_MS,
		ElectionTimeoutMinInMs:     ELECTION_TIMEOUT_MIN_IN_MS,
		ElectionTimeoutMaxInMs:     ELECTION_TIMEOUT_MAX_IN_MS,
		LeaderHeartbeatPeriodInMs:  LEADER_HEARTBEAT_PERIOD_IN_MS,
		ClientCommandBufferSize:    CLIENT_COMMAND_BUFFER_SIZE,
		ClientCommandBatchSize:     CLIENT_COMMAND_BATCH_SIZE,
		MinRemainingTimeForRPCInMs: MIN_REMAINING_TIME_FOR_RPC_IN_MS,
	}
}

// DEFAULT VALUES of configuration
const RAFT_NODE_REQUEST_BUFFER_SIZE = 500
const CLIENT_COMMAND_BATCH_SIZE = 10
const CLIENT_COMMAND_BUFFER_SIZE = 1000

const LEADER_BUFFER_SIZE = 1000
const LEADER_HEARTBEAT_PERIOD_IN_MS = 100

const RPC_REUQEST_TIMEOUT_IN_MS = 200

const ELECTION_TIMEOUT_MIN_IN_MS = 350
const ELECTION_TIMEOUT_MAX_IN_MS = 550

const MIN_REMAINING_TIME_FOR_RPC_IN_MS = 50

type ConfigIface interface {
	GetRPCRequestTimeout() time.Duration
	GetElectionTimeout() time.Duration

	GetLeaderHeartbeatPeriod() time.Duration
	GetRaftNodeRequestBufferSize() int

	GetClientCommandBufferSize() int
	GetClientCommandBatchSize() int
	String() string

	GetMinRemainingTimeForRPC() time.Duration
}

type Config struct {
	RaftNodeRequestBufferSize int `json:"raft_node_request_buffer_size"`

	// RPC timeout
	RPCRequestTimeoutInMs int `json:"rpc_request_timeout_in_ms"`

	// Election timeout
	ElectionTimeoutMinInMs int `json:"election_timeout_min_in_ms"`
	ElectionTimeoutMaxInMs int `json:"election_timeout_max_in_ms"`

	// Leader
	LeaderHeartbeatPeriodInMs int `json:"leader_heartbeat_period_in_ms"`

	// Client
	ClientCommandBufferSize int `json:"client_command_buffer_size"`
	ClientCommandBatchSize  int `json:"client_command_batch_size"`

	// internal
	MinRemainingTimeForRPCInMs int `json:"min_remaining_time_for_rpc_in_ms"`
}

func (c *Config) GetMinRemainingTimeForRPC() time.Duration {
	return time.Duration(c.MinRemainingTimeForRPCInMs) * time.Millisecond
}

func (c *Config) GetRPCRequestTimeout() time.Duration {
	return time.Duration(c.RPCRequestTimeoutInMs) * time.Millisecond
}

func (c *Config) GetElectionTimeout() time.Duration {
	diff := c.ElectionTimeoutMaxInMs - c.ElectionTimeoutMinInMs
	randomMs := rand.Intn(diff) + ELECTION_TIMEOUT_MIN_IN_MS
	return time.Duration(randomMs) * time.Millisecond
}

func (c *Config) GetRaftNodeRequestBufferSize() int {
	return c.RaftNodeRequestBufferSize
}

func (c *Config) GetLeaderHeartbeatPeriod() time.Duration {
	return time.Duration(c.LeaderHeartbeatPeriodInMs) * time.Millisecond
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

// GrpcServiceConfigDialOptionFromYAML reads a YAML file, extracts the grpc block,
// and returns a grpc.DialOption with WithDefaultServiceConfig.
func GrpcServiceConfigDialOptionFromYAML(filePath string) (grpc.DialOption, error) {
	type config struct {
		GRPC map[string]interface{} `yaml:"grpc"`
	}

	yamlData, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read YAML file: %w", err)
	}

	var cfg config
	if err := yaml.Unmarshal(yamlData, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}
	sugarLogger.Debug("grpc config: %s", cfg.GRPC)

	grpcJSON, err := json.Marshal(cfg.GRPC)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal grpc config to JSON: %w", err)
	}

	return grpc.WithDefaultServiceConfig(string(grpcJSON)), nil
}

type (
	ConfigV2 struct {
		Membership Membership ``
	}
)

type Membership struct {
	CurrentNodeID   string     `json:"current_node_id" yaml:"current_node_id"`
	CurrentPort     int        `json:"current_port" yaml:"current_port"`
	CurrentNodeAddr string     `json:"current_node_addr" yaml:"current_node_addr"`
	AllMembers      []NodeAddr `json:"all_members" yaml:"all_members"`
}

type NodeAddr struct {
	NodeID  string `json:"node_id" yaml:"node_id"`
	NodeURI string `json:"node_uri" yaml:"node_uri"`
}
