package util

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var nodeBasicInfo *MembershipBasicInfo

func init() {
	nodeBasicInfo = &MembershipBasicInfo{
		NodeID: "node1",
		Membership: []NodeConfigruation{
			{
				NodeID:  "node1",
				NodeURI: "localhost:18080",
			},
			{
				NodeID:  "node2",
				NodeURI: "localhost:18081",
			},
			{
				NodeID:  "node3",
				NodeURI: "localhost:18082",
			},
			{
				NodeID:  "node4",
				NodeURI: "localhost:18083",
			},
			{
				NodeID:  "node5",
				NodeURI: "localhost:18084",
			},
		},
	}
}

func TestMembershipBasicInfo(t *testing.T) {
	jsonStr, err := json.Marshal(nodeBasicInfo)
	if err != nil {
		t.Errorf("json marshal error: %v", err)
	} else {
		fmt.Printf("json string: %s\n", jsonStr)
		t.Logf("json string: %s", jsonStr)
	}
}

func TestConfig(t *testing.T) {
	config := GetDefaultConfig()
	config.MembershipBasicInfo = *nodeBasicInfo
	assert.Equal(t, config.MembershipBasicInfo.NodeID, nodeBasicInfo.NodeID)
	assert.ElementsMatch(t, config.MembershipBasicInfo.Membership, nodeBasicInfo.Membership)
	assert.Equal(t, config.RPCRequestTimeoutInMs, RPC_REUQEST_TIMEOUT_IN_MS)
}
