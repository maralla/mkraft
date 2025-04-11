package main

import (
	"fmt"
	"sync"

	pb "github.com/maki3cat/mkraft/rpc"
	"github.com/maki3cat/mkraft/util"
	"google.golang.org/grpc"
)

// maki: this needs to be threadsafe, analyze this to gogynastics
var membership sync.Map

func initMembership() {
	membership.Store("node1", "localhost:50051")
	membership.Store("node2", "localhost:50052")
	membership.Store("node3", "localhost:50053")
	membership.Store("node4", "localhost:50054")
	membership.Store("node5", "localhost:50055")
}

func getMembershipSnapshot() map[string]string {
	membershipSnapshot := make(map[string]string)
	membership.Range(func(key, value interface{}) bool {
		membershipSnapshot[key.(string)] = value.(string)
		return true
	})
	return membershipSnapshot
}

var nodesConnecionts map[string]*grpc.ClientConn

// todo: should be one connection to one client?
var membershipClients map[string]*pb.RaftServiceClient

func create_connection(serverAddr string) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(serverAddr)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// TODO: CONNECTING TO OTHER NODES SHOULD HAPPEN AFTER THE CURRENT NODE IS UP
func init() {

	nodesConnecionts = make(map[string]*grpc.ClientConn)
	for nodeID, addr := range getMembershipSnapshot() {
		if nodeID == util.GetConfig().NodeID {
			continue
		}
		conn, err := create_connection(addr)
		if err != nil {
			util.GetSugarLogger().Errorw("failed to connect to server", "addr", addr, "error", err)
			continue
		}
		nodesConnecionts[addr] = conn
	}
	fmt.Println("connected to all servers")
}
