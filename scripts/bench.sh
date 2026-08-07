#!/usr/bin/env bash
set -euo pipefail

# The TigerBeetle client ships a prebuilt static library that Apple's
# default linker refuses to link (see Makefile's GO_LDFLAGS comment); the
# classic linker accepts it. Linux is unaffected. Mirrors Makefile's
# GO_LDFLAGS so `go test -bench` links here the same way `make bench` does.
ldflags=()
if [[ "$(uname -s)" == "Darwin" ]]; then
  ldflags=(-ldflags=-extldflags=-Wl,-ld_classic)
fi

out="docs/benchmarks/$(git rev-parse --short HEAD).txt"
go test ./... -run='^$' -bench=. -benchmem -count=6 "${ldflags[@]}" | tee "$out"
echo "saved to $out"
echo "compare: benchstat docs/benchmarks/<old>.txt $out"
