
MEMBERSHIP_CONFIG='{"current_node_id":"node1","current_port":18080,"current_node_addr":"localhost:18080","all_members":[{"node_id":"node1","node_uri":"localhost:18080"},{"node_id":"node2","node_uri":"localhost:18081"},{"node_id":"node3","node_uri":"localhost:18082"},{"node_id":"node4","node_uri":"localhost:18083"},{"node_id":"node5","node_uri":"localhost:18084"}]}'

default:
	@go test -v ./...

run:
	@echo "Running the main program..."
	@go run main.go -m $(MEMBERSHIP_CONFIG)

protogen:
	@protoc --go_out=. --go-grpc_out=. proto/mkraft/service.proto
	@echo "Protocol buffer files generated successfully."

