

all: clean build

test:
	@go test -v ./...

run:
	@echo "Running the main program..."
	@go run main.go -c local/config1.yaml

protogen:
	@protoc --go_out=. --go-grpc_out=. proto/mkraft/service.proto
	@echo "Protocol buffer files generated successfully."

mockgen:
	@mockgen -source=raft/membership.go -destination=./raft/membership_mock.go -package raft
	@mockgen -source=rpc/service_grpc.pb.go -destination=./rpc/service_mock.go -package rpc
	@mockgen -source=rpc/client.go -destination=./rpc/client_mock.go -package rpc
	@mockgen -source=util/config.go -destination=./util/config_mock.go -package util
	@mockgen -source=raft/consensus.go -destination=./raft/consensus_mock.go -package raft

build:
	@echo "Building the project..."
	@go build -o bin/mkraft

clean:
	@rm bin/*


#  ./bin/mkraft -c config/config1.yaml > output1.log 2>&1
