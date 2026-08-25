#!/usr/bin/env bash
#
# Produces the project's coverage number: the unit suite and the integration
# suite merged into one profile, with generated code filtered out.
#
# Why the merge. cmd/kafkatb has no tests of its own -- it is a cobra command
# tree whose only honest exercise is being run as a subprocess over a real
# signal, which is exactly what test/integration/subcommand_test.go does.
# Measuring only the unit suite reports that binary as 0% and hides the fact
# that it is tested. So both suites emit Go's binary coverage format
# (GOCOVERDIR / -test.gocoverdir) and "go tool covdata" merges them: a
# statement counts as covered if any suite executed it.
#
# What is deliberately left out of the number, and why:
#
#   *_easyjson.go   Generated marshalers, ~780 statements. They are exercised
#                   through the real API by the hand-written codec, cdc and
#                   emit tests; the uncovered remainder is generated error
#                   handling for shapes the hand-written types cannot produce.
#                   Testing them directly would measure easyjson, not us.
#
#   cmd/loadgen     A synthetic load generator for benchmarking, not part of
#                   the shipped connector. It is untested (0%), and it is
#                   excluded because it is not the connector -- not because it
#                   is covered. Note this is also what Go's binary coverage
#                   format does on its own: a main package that no test binary
#                   imports emits no coverage metadata at all.
#
# Usage:
#   scripts/coverage.sh              unit + integration (needs Docker)
#   SKIP_INTEGRATION=1 scripts/coverage.sh   unit only, cmd/kafkatb absent
set -euo pipefail

cd "$(dirname "$0")/.."
root=$(pwd)

covdir=${COVDIR:-build/coverage}
out=${COVERPROFILE:-coverage.out}

# See the Makefile: Apple's current linker rejects the TigerBeetle client's
# prebuilt static library, and the classic linker accepts it.
ldflags=()
if [ "$(uname -s)" = "Darwin" ]; then
	ldflags=(-ldflags=-extldflags=-Wl,-ld_classic)
fi

rm -rf "$covdir"
mkdir -p "$covdir/unit" "$covdir/integration"

# -coverpkg over the whole module, so that a statement is credited to whichever
# suite ran it rather than only to its own package's tests.
echo "==> unit suite"
go test ./... -count=1 -covermode=atomic \
	-coverpkg=github.com/Mi7teR/kafka-tb/... "${ldflags[@]}" \
	-args -test.gocoverdir="$root/$covdir/unit"

inputs="$covdir/unit"
if [ "${SKIP_INTEGRATION:-0}" = "1" ]; then
	echo "==> integration suite SKIPPED (SKIP_INTEGRATION=1); cmd/kafkatb will be absent"
else
	echo "==> integration suite"
	# KAFKATB_COVERDIR tells the harness to build the kafkatb binary with
	# coverage and to point each subprocess's GOCOVERDIR here; it is a variable
	# of our own because "go test" overwrites GOCOVERDIR for the test process.
	# -test.gocoverdir collects the integration test binary's own counters into
	# the same directory, so covdata sees both as one input.
	KAFKATB_COVERDIR="$root/$covdir/integration" \
		go test ./test/integration/... -tags=integration -count=1 -timeout=15m \
		-covermode=atomic -coverpkg=github.com/Mi7teR/kafka-tb/... "${ldflags[@]}" \
		-args -test.gocoverdir="$root/$covdir/integration"
	inputs="$inputs,$covdir/integration"
fi

go tool covdata textfmt -i="$inputs" -o "$covdir/merged.txt"

# Keep the "mode:" header (it has no .go: in it) and drop generated files.
grep -v '_easyjson\.go:' "$covdir/merged.txt" >"$out"

echo
echo "==> merged, filtered coverage -> $out"
go tool cover -func="$out" | tail -1
