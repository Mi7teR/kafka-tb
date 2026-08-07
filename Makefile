export CGO_ENABLED=1

# The TigerBeetle client ships a prebuilt static library whose members are not
# 8-byte aligned, which Apple's current linker refuses to link:
#   ld: 64-bit mach-o member 'libtb_client.a.o' not 8-byte aligned
# The classic linker accepts it. Linux is unaffected.
ifeq ($(shell uname -s),Darwin)
GO_LDFLAGS := -ldflags=-extldflags=-Wl,-ld_classic
endif

.PHONY: build test lint bench integration proto

build:
	go build $(GO_LDFLAGS) -o bin/kafkatb ./cmd/kafkatb

test:
	go test ./... -race -count=1 $(GO_LDFLAGS)

integration:
	go test ./test/integration/... -tags=integration -count=1 -timeout=15m $(GO_LDFLAGS)

bench:
	go test ./... -run=^$$ -bench=. -benchmem $(GO_LDFLAGS)

lint:
	golangci-lint run

proto:
	buf generate
