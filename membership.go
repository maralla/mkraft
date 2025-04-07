package main

import "github.com/maki3cat/mkraft/rpc"

// to be added
// todo: this is a mock, we need to find the membership
func getMembership() []int {
	membership := []int{1, 2, 3, 4, 5}
	return membership
}

func getClientOfAllMembers() []rpc.InternalClientIface {
	members := getMembership()
	clients := make([]rpc.InternalClientIface, len(members))
	for i, _ := range members {
		clients[i] = rpc.NewInternalClient(rpc.NewRPCClient())
	}
	return clients
}
