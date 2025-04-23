

all: build test

test:
	@go test -v ./...

run:
	@echo "Running the main program..."
	@go run main.go -c local/config1.yaml

protogen:
	@protoc --go_out=. --go-grpc_out=. proto/mkraft/service.proto
	@echo "Protocol buffer files generated successfully."

mockgen:
	@mockgen -source=rpc/service_grpc.pb.go -destination=./rpc/mock_client.go -package rpc

build:
	@echo "Building the project..."
	@go build -o bin/mkraft

clean:
	@rm bin/*
