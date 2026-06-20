.PHONY: build test lint clean run

BINARY_NAME=server
BUILD_DIR=bin

build:
	@echo "Building $(BINARY_NAME)..."
	@if not exist $(BUILD_DIR) mkdir $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY_NAME).exe ./cmd/server

test:
	go test -v -race -coverprofile=coverage.out ./...

lint:
	golangci-lint run ./...

clean:
	@if exist $(BUILD_DIR) rmdir /s /q $(BUILD_DIR)
	@if exist coverage.out del coverage.out

run: build
	.\$(BUILD_DIR)\$(BINARY_NAME).exe
