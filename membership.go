package main

import (
	"fmt"
	"sync"

	"github.com/maki3cat/mkraft/rpc"
	"github.com/maki3cat/mkraft/util"
	"google.golang.org/grpc"
)

var (
	membershipManagerInst *MembershipManager
)

type MembershipConfig struct {
	NodeID     string       `json:"node_id"`
	Membership []NodeConfig `json:"membership"`
}

type NodeConfig struct {
	NodeID  string `json:"node_id"`
	NodeURI string `json:"node_uri"`
}

func InitMembershipManager(conf *MembershipConfig) {
	membershipManagerInst = &MembershipManager{
		currentMembers: conf,
		connections:    make(map[string]*grpc.ClientConn),
		clients:        make(map[string]rpc.InternalClientIface),
		locks:          make(map[string]sync.Mutex),
	}
	for _, nodeInfo := range conf.Membership {
		membershipManagerInst.locks[nodeInfo.NodeID] = sync.Mutex{}
	}
}

// maki
// todo: right now we suppose membership list doesn't change after first set up
// they may be alive or dead, but the list is fixed
type MembershipManager struct {

	// todo: in the near future, we need update membership dynamically;
	// todo: in the remote future, we need to update the config dynamically
	currentMembers *util.MembershipConfig

	// maki: here is a topic
	// todo: should be one connection to one client?
	// todo: what is the best pattern of maintaing busy connections and clients
	// todo: now these connections/clients are stored in map which is not thread safe
	// todo: need to check problem of concurrency here
	connections map[string]*grpc.ClientConn

	// maki: here is a topic for go gynastics
	// interface cannot use pointer
	clients map[string]rpc.InternalClientIface
	locks   map[string]sync.Mutex
}

// lazily initialized for the first time
func (mgr *MembershipManager) GetClient(nodeID string) rpc.InternalClientIface {
	client, ok := mgr.clients[nodeID]
	if ok {
		// todo: if this client is thread safe for different goroutines?
		return client
	}
	if !ok {
		util.GetSugarLogger().Errorw("failed to get client", "nodeID", nodeID)
		return nil
	}
	return client
}

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

// clients shalle be lazily initialized
// if we start them all at when all servers boot, they fail at the same time
func InitClients() {
	nodesConnecionts = make(map[string]*grpc.ClientConn)
	otherNodes := util.GetConfig().MembershipConfig.Membership

	for _, nodeInfo := range otherNodes {
		nodeID := nodeInfo.NodeID
		nodeURI := nodeInfo.NodeURI
		if nodeID == util.GetConfig().NodeID {
			continue
		}
		conn, err := createConn(nodeURI)
		if err != nil {
			util.GetSugarLogger().Errorw("failed to connect to server", "nodeURI", nodeURI, "error", err)
			continue
		}
		nodesConnecionts[nodeURI] = conn
		membershipClients[nodeID] = createClient(conn)
	}
	fmt.Println("connected to all servers")
}
