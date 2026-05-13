.PHONY: build test lint vet tidy fmt clean run-worker ask ask-supervisor all

GO ?= go

all: fmt vet test build

build:
	$(GO) build ./...

test:
	$(GO) test -race -count=1 ./...

vet:
	$(GO) vet ./...

fmt:
	$(GO) fmt ./...

tidy:
	$(GO) mod tidy

# Optional: requires `go install honnef.co/go/tools/cmd/staticcheck@latest`
lint:
	staticcheck ./... || (echo "staticcheck not installed; skipping" && true)

run-worker:
	$(GO) run ./cmd/worker

ask:
	$(GO) run ./cmd/ask -q "$(Q)" -rounds 3

# Multi-agent supervisor: decomposes the question, fans out child
# convergence workflows in parallel, synthesizes the result.
# Try: make ask-supervisor Q="What is Go and how does it differ from Rust"
ask-supervisor:
	$(GO) run ./cmd/ask-supervisor -q "$(Q)" -rounds 3

clean:
	rm -f temporal.db temporal.db-*
	rm -rf bin/
