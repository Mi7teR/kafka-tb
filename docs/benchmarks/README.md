# Benchmarks

## Microbenchmarks

Raw output: [`99ebf66.txt`](./99ebf66.txt), produced by `./scripts/bench.sh`
(`go test ./... -bench=. -benchmem -count=6`) at commit `99ebf66`, on:

- Apple M4 Pro, macOS 26.5.2, go1.26.3 darwin/arm64

Headline numbers (median of 6 runs):

| Benchmark | ns/op | throughput | allocs/op |
|---|---|---|---|
| `BenchmarkDecodeJSON/n=1` | ~1990 | ~135 MB/s | 28 |
| `BenchmarkDecodeJSON/n=10` | ~15,720 | ~145 MB/s | 124 |
| `BenchmarkDecodeJSON/n=100` | ~148,600 | ~150 MB/s | 1030 |
| `BenchmarkParseAmount` | ~94 | — | 3 |
| `BenchmarkFormatAmount` | ~193 | — | 11 |
| `BenchmarkParseID` | ~13.4 | — | 0 |
| `BenchmarkMapResults` | ~106,000 | — | 83 |
| `BenchmarkBatcherAssemble` | ~112,800 | — | 5 |

To compare against a later commit: `./scripts/bench.sh && benchstat docs/benchmarks/99ebf66.txt docs/benchmarks/<new>.txt`.

## End-to-end load scenarios

**What was actually measured:** all six scenarios below were run, but scaled
down from the brief's `-count=1000000`/`-count=200000` to counts that finish
in well under a minute each (see "Actual count" column) — this is an honest
first baseline, not a definitive performance study. Every number in this
table comes from a real run against containerized Redpanda
(`docker.redpanda.com/redpandadata/redpanda:v25.2.4`) and a single-replica
TigerBeetle (`ghcr.io/tigerbeetle/tigerbeetle:0.17.9`), both on the same
machine as the microbenchmarks above. Nothing in this table is estimated or
extrapolated.

**How latency was measured:** `cmd/loadgen` only produces — it has no flag
to read anything back, and the results-topic message
(`internal/emit.ResultsMessage`) does not currently echo `user_data_64`
back out, so per-transfer correlation can't be done by decoding the results
topic alone. To get real end-to-end numbers, a throwaway measurement
harness (not part of this repo, not committed) reused loadgen's exact wire
format and account-seeding approach, additionally keeping a local
`map[transferID]publishTimeNs` at send time and consuming the results topic
afterward to compute `latency = resultObservedAt - publishTime`. This is
the same mechanism `user_data_64` exists to support; it just lives outside
the committed binary rather than inside it, since the brief's flag list for
`loadgen` doesn't include a results-reading mode. `cmd/loadgen` itself was
separately verified end-to-end (see "Correctness check" below).

| Scenario | Commit | Hardware | Config (linger / max_batch_size) | Actual count | Transfers/sec | p50 | p95 | p99 | Mean batch size |
|---|---|---|---|---|---|---|---|---|---|
| Throughput ceiling | 99ebf66 | Apple M4 Pro | 5ms / 8189 | 3,000 (of 1,000,000) | 69.9 | 21.0s | 40.7s | 42.5s | 1.2 |
| Hot account | 99ebf66 | Apple M4 Pro | 5ms / 8189 | 2,000 (of 200,000) | 67.7 | 14.6s | 27.9s | 29.2s | 1.0 |
| Linger 1ms | 99ebf66 | Apple M4 Pro | 1ms / 8189 | 3,000 | 240.3 | 6.2s | 11.8s | 12.4s | 1.2 |
| Linger 50ms | 99ebf66 | Apple M4 Pro | 50ms / 8189 | 1,500 | 16.0 | 47.0s | 89.0s | 92.8s | 1.3 |
| 5% garbage | 99ebf66 | Apple M4 Pro | 5ms / 8189 | 2,000 (1,915 valid + 85 garbage, of 200,000) | 63.9 | 15.3s | 28.6s | 29.7s | 1.3 |
| Chains of 10 | 99ebf66 | Apple M4 Pro | 5ms / 8189 | 5,000 = 500 msgs × chain 10 (of 200,000) | 640.3 | 3.9s | 7.5s | 7.8s | 11.0 |

Additional per-scenario data (`kafkatb_tb_latency_seconds` p99 — TigerBeetle
apply latency only, not end-to-end — and DLQ behavior):

| Scenario | `tb_latency_seconds` p99 | DLQ |
|---|---|---|
| Throughput ceiling | 16ms | 0 records (clean) |
| Hot account | 16ms | 0 records (clean) |
| Linger 1ms | 8ms | 0 records (clean) |
| Linger 50ms | 16ms | 0 records (clean) |
| 5% garbage | 32ms | 85 records — exactly the 85 garbage messages sent, 0 false positives |
| Chains of 10 | 32ms | 0 records (clean) |

Commit lag (`kafkatb_offset_commit_lag`) returned to 0 after every scenario:
each run's consumer fully drained its backlog before the next scenario
started, so there is no "lag under sustained load" behavior to report from
these runs — the counts were too small and too short to build a durable
backlog. That would require running the full-scale (`-count=1000000`)
scenario, which was **not measured** here (see below).

### What was not measured, and why

- **Full-scale counts** (1,000,000 / 200,000 transfers as in the brief) were
  not run. At the throughput this connector currently sustains for
  unchained messages (~70/s, see "why linger" below), a 1,000,000-transfer
  run would take roughly 4 hours — well outside "a few minutes." The scaled
  counts above are real measurements of the same code path, just over a
  shorter window.
- **Sustained backlog / consumer lag under load** was not observed for the
  reason above: every scaled run drained faster than a backlog could build.
- **`-rate` flag** was not exercised in the six scenarios (all used
  `-rate=0`, unlimited); it was smoke-tested separately (see loadgen
  correctness check).

### Why this data argues for the smallest practical `linger`

The sink (`internal/sink.Sink.processBatch`) consumes and applies one Kafka
record at a time: it calls `Submit` and blocks on the outcome before moving
to the next record (`internal/sink/sink.go`, `applyRecord`). Because
`tbx.Batcher.Submit` is only ever called once at a time from this loop,
the batcher's linger window never has more than one job to coalesce across
*separate Kafka messages* — cross-message batching in this connector, as
currently wired, only happens through `-chain` (packing multiple transfers
into a single Kafka message with `linked`), not through waiting out a
linger window. The "Chains of 10" row above shows this directly: mean batch
size jumps from ~1.2 to 11.0 and throughput jumps ~9x, with no linger
change at all.

Given that, linger only adds latency without ever buying a batching
benefit for single-transfer messages: throughput scaled almost exactly
with `1/linger` (5ms → ~70/s, 1ms → ~240/s, 50ms → ~16/s), and p50/p99
latency scaled the same way. **The data argues for the smallest linger the
deployment can tolerate** (1ms here) as the default for workloads that
send one-transfer-per-message, and confirms that `-chain`-style batching at
the producer is the real lever for throughput in this design — a linger
window only pays off if multiple *concurrent* Kafka partitions or producers
can queue up behind it, which a single-partition benchmark topic like the
one used here cannot exercise. `configs/example.yaml` currently defaults to
`linger: 5ms`; based on this run, `1ms` (or lower) is the better default
unless a specific deployment is known to have many partitions feeding the
batcher concurrently.

### Correctness check (not a performance measurement)

Before taking the above measurements, `cmd/loadgen` was run directly
(`loadgen -brokers=127.0.0.1:9092 -topic=bench.in -count=200 -accounts=10`)
against the same containerized stack and confirmed:

- `kafkatb_records_total{result="ok"}` increased by exactly 210 (200
  transfers + 10 accounts), with no `rejected` or `poison` results.
- The DLQ topic's high-water mark stayed at 0 (no records at all).
- A separate direct check — one `create_accounts` message for two fresh
  accounts, one `create_transfers` message for `10.00` between them, then a
  direct TigerBeetle `LookupAccounts` — showed `debits_posted=1000`,
  `credits_posted=0` on the debit account and `debits_posted=0`,
  `credits_posted=1000` on the credit account (minor units, scale 2): the
  balance moved by exactly the transferred amount, both sides, in the wire
  format `cmd/loadgen` produces.

The 5% garbage scenario above additionally confirms the DLQ path: 85
garbage messages sent, 85 DLQ records, 0 false positives among the 1,915
valid messages in the same run.
