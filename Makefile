

all: clean build

test:
	go test -v ./...

run:
	echo "Running the main program..."
	go run main.go -c local/config1.yaml

protogen:
	protoc --go_out=. --go-grpc_out=. proto/mkraft/service.proto
	echo "Protocol buffer files generated successfully."

mockgen:
	mockgen -source=raft/membership.go -destination=./raft/membership_mock.go -package raft
	mockgen -source=rpc/service_grpc.pb.go -destination=./rpc/service_mock.go -package rpc
	mockgen -source=rpc/client.go -destination=./rpc/client_mock.go -package rpc
	mockgen -source=common/config.go -destination=./common/config_mock.go -package common
	mockgen -source=raft/consensus.go -destination=./raft/consensus_mock.go -package raft
	mockgen -source=raft/node.go -destination=./raft/node_mock.go -package raft

build:
	echo "Building the project..."
	go build -o bin/mkraft cmd/main.go

clean:
	rm bin/*
	rm *.log *.pid

test-nodes: build
	echo "Starting mkraft nodes..."
	./bin/mkraft -c ./config/local/base.yaml > node1.log 2>&1 & echo $$! > node1.pid
	./bin/mkraft -c ./config/local/node2.yaml > node2.log 2>&1 & echo $$! > node2.pid
	./bin/mkraft -c ./config/local/node3.yaml > node3.log 2>&1 & echo $$! > node3.pid
	echo "Nodes running for 30 seconds..."
	sleep 30
	echo "Stopping nodes..."
	-kill -15 $$(cat node1.pid)
	-kill -15 $$(cat node2.pid) 
	-kill -15 $$(cat node3.pid)
	sleep 10
	rm -f node1.pid node2.pid node3.pid
	@ps aux | grep "mkraft"
	echo "All nodes stopped"
