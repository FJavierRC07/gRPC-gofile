.PHONY: generate tidy build test

PROTOC_INCLUDE ?= $(shell dirname $(shell which protoc 2>/dev/null))/../include
PROTO_SRC      := proto/fileservice/v1/fileservice.proto
GEN_DIR        := gen

# Regenerate Go and gRPC stubs from the proto definition.
# Requires:
#   protoc                   - https://grpc.io/docs/protoc-installation/
#   protoc-gen-go            - go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   protoc-gen-go-grpc       - go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
generate:
	mkdir -p $(GEN_DIR)/fileservice/v1
	protoc \
		--go_out=$(GEN_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR) --go-grpc_opt=paths=source_relative \
		-I proto -I $(PROTOC_INCLUDE) \
		$(PROTO_SRC)

# Download and tidy Go module dependencies.
tidy:
	go mod tidy

# Build the server binary.
build: generate
	go build -o file-service ./cmd/server

# Run all tests.
test:
	go test ./...
