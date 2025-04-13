package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"

	pb "github.com/maki3cat/mkraft/rpc"
	"google.golang.org/grpc"
)

// THE RPC SERVER, which is different from the RAFT SERVER
// server interface
// shall be implemented by a grpc server and try to make an abstract interface
type RPCServerIface interface {
	SendRequestVote(request pb.RequestVoteRequest) pb.RequestVoteResponse
	SendAppendEntries(request pb.AppendEntriesRequest) pb.AppendEntriesResponse
}

/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

// Package main implements a server for Greeter service.

var (
	port = flag.Int("port", 50051, "The server port")
)

// server is used to implement helloworld.GreeterServer.
type server struct {
	pb.UnimplementedRaftServiceServer
}

// SayHello implements helloworld.GreeterServer
func (s *server) SayHello(_ context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	log.Printf("Received: %v", in.GetName())
	return &pb.HelloReply{Message: "Hello " + in.GetName()}, nil
}

func (s *server) RequestVote(_ context.Context, in *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	log.Printf("Received: %v", in)
	return &pb.RequestVoteResponse{Term: 1, VoteGranted: true}, nil
}

func (s *server) AppendEntries(_ context.Context, in *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	log.Printf("Received: %v", in)
	return &pb.AppendEntriesResponse{Term: 1, Success: true}, nil
}

func main() {

	membershipStr := flag.String("m", "", "the json string of MembershipBasicInfo")
	flag.Parse()
	fmt.Printf("membership: %s\n", *membershipStr)
	if *membershipStr == "" {
		panic("please provide the membership json string")
	}

	membershipBasicInfo := Membership{}
	err := json.Unmarshal([]byte(*membershipStr), &membershipBasicInfo)
	if err != nil {
		panic("failed to parse membership json string " + *membershipStr + ": " + err.Error())
	}
	InitMembershipManager(&membershipBasicInfo)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterRaftServiceServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
