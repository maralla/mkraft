
protogen:
	@protoc --go_out=. --go-grpc_out=. proto/mkraft/service.proto
	@echo "Protocol buffer files generated successfully."