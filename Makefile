.PHONY: build test lint clean run

BINARY=bin/server
GO=go

build:
	$(GO) build -o $(BINARY) ./cmd/server

run:
	$(GO) run ./cmd/server

test:
	$(GO) test -race -cover ./...

lint:
	golangci-lint run ./...

clean:
	rm -rf bin/

tidy:
	$(GO) mod tidy
