.PHONY: test race bench build

BIN_DIR := bin

build:
	mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/keelstone ./cmd/keelstone
	go build -o $(BIN_DIR)/bench ./cmd/bench

test:
	go test -count=1 -timeout 20m ./...

race:
	go test -race -count=1 -timeout 25m ./...

bench: build
	./$(BIN_DIR)/bench -mode=load -spawn=3 -read-ratio=0 -duration=10s -clients=8 -base-port=23121
	./$(BIN_DIR)/bench -mode=load -spawn=3 -read-ratio=1 -duration=10s -clients=8 -base-port=23131
	./$(BIN_DIR)/bench -mode=load -spawn=5 -read-ratio=0 -duration=10s -clients=8 -base-port=23141
	./$(BIN_DIR)/bench -mode=load -spawn=5 -read-ratio=1 -duration=10s -clients=8 -base-port=23151
	./$(BIN_DIR)/bench -mode=failover -spawn=3 -base-port=23161
