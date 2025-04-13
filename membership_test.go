package main

import (
	"encoding/json"
	"fmt"
	"testing"
)

var nodeBasicInfo *Membership

func init() {
	nodeBasicInfo = &Membership{
		CurrentNodeID: "node1",
		AllMembers: []NodeAddr{
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
