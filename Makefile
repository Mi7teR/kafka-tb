export CGO_ENABLED=1

.PHONY: build test lint bench integration proto

build:
	go build -o bin/kafkatb ./cmd/kafkatb

test:
	go test ./... -race -count=1

integration:
	go test ./test/integration/... -tags=integration -count=1 -timeout=15m

bench:
	go test ./... -run=^$$ -bench=. -benchmem

lint:
	golangci-lint run

proto:
	buf generate
