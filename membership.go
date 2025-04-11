package main

import (
	"fmt"
	"sync"

	"github.com/maki3cat/mkraft/rpc"
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

// maki: here is a topic
// todo: should be one connection to one client?
// todo: what is the best pattern of maintaing busy connections and clients
// todo: now these connections/clients are stored in map which is not thread safe
// todo: need to check problem of concurrency here
var nodesConnecionts map[string]*grpc.ClientConn

// maki: here is a topic for go gynastics
// interface cannot use pointer
// todo:
var membershipClients map[string]rpc.InternalClientIface

func createConn(serverAddr string) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(serverAddr)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func createClient(conn *grpc.ClientConn) rpc.InternalClientIface {
	client := rpc.NewRaftServiceClient(conn)
	return rpc.NewInternalClient(client)
}

// todo: and the pointer of the client will be send to the goroutinej
// todo: concurrency to be handled
func GetPeersInMembership() []rpc.InternalClientIface {
	peers := make([]rpc.InternalClientIface, 0)
	for key, client := range membershipClients {
		if key == util.GetConfig().NodeID {
			continue
		}
		peers = append(peers, client)
	}
	return peers
}

// TODO: CONNECTING TO OTHER NODES SHOULD HAPPEN AFTER THE CURRENT NODE IS UP
func init() {
	nodesConnecionts = make(map[string]*grpc.ClientConn)
	for nodeID, addr := range getMembershipSnapshot() {
		if nodeID == util.GetConfig().NodeID {
			continue
		}
		conn, err := createConn(addr)
		if err != nil {
			util.GetSugarLogger().Errorw("failed to connect to server", "addr", addr, "error", err)
			continue
		}
		nodesConnecionts[addr] = conn
		membershipClients[nodeID] = createClient(conn)
	}
	fmt.Println("connected to all servers")
}
