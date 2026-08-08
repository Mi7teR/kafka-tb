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

**Latest re-run:** [`523ffd3.txt`](./523ffd3.txt), same machine and same
`./scripts/bench.sh` invocation, at commit `523ffd3` (after per-partition
pipelining and async DLQ/results publication — see the four-way table
below). Every number here is within run-to-run noise of the `99ebf66`
figures above (e.g. `BenchmarkDecodeJSON/n=1` median 2,024ns vs 1,990ns):
none of the code these microbenchmarks cover (decode, amount/ID parsing,
result mapping, batch assembly) changed in either perf commit, so an
unchanged result here is the expected outcome, not a null result.

**Latest re-run after batcher sharding:** [`173d712.txt`](./173d712.txt),
same machine and invocation, at commit `173d712` (after batcher sharding —
see the four-way table below). Again within run-to-run noise of both prior
files (e.g. `BenchmarkDecodeJSON/n=1` ~2.02µs here too): none of the
microbenchmarked code path changed in the sharding commits either.

## Sink throughput: baseline → pipelining → async publish → batcher sharding

Four point-in-time measurements of the same code path (Kafka record in,
TigerBeetle transfer applied, result published), each reproducing the prior
run's method for comparability. Full detail and raw per-run numbers are in
`.superpowers/sdd/perf-baseline.md`, `.superpowers/sdd/perf-after-pipelining.md`,
`.superpowers/sdd/perf-final.md`, and `.superpowers/sdd/perf-sharded.md`.
**What's measured vs. estimated:** every number in this table comes from a
real run against the same containerized Redpanda/TigerBeetle stack as the
load scenarios below — none of it is extrapolated or scaled up from a
smaller run. All four are single-machine, few-run measurements (1 run for
the baseline, 2 for pipelining, 3-4 for async publish, 2 for the headline
sharding rows — the shards/max-in-flight/linger sweeps in `perf-sharded.md`
are single runs per value and flagged there as noisy) — order-of-magnitude,
not an SLA; see each linked doc for the per-run spread.

**Hardware:** Apple M4 Pro, macOS, Docker via OrbStack, go1.26.3 darwin/arm64
(same machine for all four). **Config held constant across all four runs:**
`batcher.linger=5ms`, `batcher.max_batch_size=8189`, `batcher.max_queue=1000`,
1 partition unless noted. `sink.max_in_flight_per_partition` did not exist
before pipelining; it defaulted to 1000 from the "after pipelining" column
onward. `batcher.shards` did not exist before sharding; the sharding column
uses the default (4) unless noted — see `perf-sharded.md` for the
`shards=1/4/8/16` sweep.

| Metric | Baseline (serial sink, pre-`ab2b47a`; no commit sha recorded in `perf-baseline.md`) | After pipelining (`ab2b47a`) | After async publish (`523ffd3`) | **After batcher sharding (`173d712`)** |
|---|---|---|---|---|
| Throughput, 1 partition (n=3,000) | 48.2 rec/sec | 4,494 rec/sec | 16,343 rec/sec (avg of 3 runs) | **16,459.5 rec/sec** (avg of 2 runs) |
| Per-record wall clock | 20.75 ms | 0.222 ms | 61.2 µs | **60.7 µs** |
| Mean batch size TigerBeetle sees | 1.0000 | 750 | 428.7 | **514.5** (avg of 2: 600.2 / 428.7 — see `perf-sharded.md` on why 428.7 recurs exactly) |
| p99 batch size | n/a (all batches = 1) | 1,000 (capped by `max_in_flight_per_partition`) | ~594 | ~663-1,000 (2 runs) |
| Throughput, 12 partitions (n=3,000 or 1,500) | ~1.06x vs 1p (no effect) | 2.20x vs 1p (12p faster) | ~1.03x vs 1p (parity) | **1.36x vs 1p** (12p faster again — avg 22,341.0 rec/sec) |
| Dominant cost | TigerBeetle round trip (51%) + linger wait (30%) | `emit.Results` synchronous `ProduceSync` (92%) | TigerBeetle round trip (~55%), serialized one batch at a time | **TigerBeetle round trip, now split across up to `shards` (4) concurrent batches — see below** |

Three results worth calling out because they reverse or qualify the previous
column's conclusion rather than just extending it:

- **Partition count matters again, partially.** Pipelining made partition
  count a real throughput lever (2.20x at 12p) because the bottleneck then
  (`emit.Results`) ran once per record on each partition's own goroutine.
  Async publication moved the bottleneck to `tbx.Batcher`'s single in-flight
  `CreateTransfers` call *shared across all partitions*, flattening the ratio
  to ~1.0x. Sharding gives partitions concurrent batcher workers to land on
  again (up to `shards`, 4 by default) — the ratio moved to 1.36x, real but
  well short of pipelining's 2.20x, because the default caps concurrency at 4
  regardless of partition count and per-shard batches are smaller than the
  single-worker case was (mean ~300-333 at 12p vs ~429-600 at 1p — see
  `perf-sharded.md` §2).
- **At 1 partition, sharding changes nothing — confirmed directly, not just
  inferred.** All of one partition's records share one ordering key, so
  `pickShard` always routes them to the same worker no matter how many
  shards exist: a direct `shards=1` vs `shards=4` check at 1 partition came
  back 12,982.3 vs 12,983.5 rec/sec, a 0.01% difference (`perf-sharded.md`
  §3). This is the expected result of the routing design, not a null
  finding.
- **The `shards` sweep itself (1/4/8/16 at 12 partitions) did not produce a
  clean ranking** in a single run per value — `shards=1` was actually
  fastest in that run, ahead of the default `shards=4` (`perf-sharded.md`
  §4). Batch size shrank monotonically with more shards, as expected, but
  whether that nets out to more throughput at typical partition counts needs
  a repeated, sustained-load sweep this report does not have. What can be
  said without more data: shards beyond the number of distinct ordering keys
  in flight (partitions, here 12) cannot help by construction — `hashShard`
  has nothing to route to them.
- **`sink.max_in_flight_per_partition` is still a real, measurable limiter.**
  Raising it from the default 1,000 to 4,000 gave 1.47x more throughput at 1
  partition (single run); 8,000 gave 1.38x, not further improvement over
  4,000 — plausibly a plateau, but only one run each, so not strong evidence
  either way (`perf-sharded.md` §7).

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

### `linger`: re-run against current (pipelined + async-publish + sharded) code

The original analysis in the collapsed box at the end of this subsection was
written against the serial sink, before pipelining, and is superseded by the
re-run below — it is kept only for the historical record, not as current
guidance.

`.superpowers/sdd/perf-sharded.md` §5 re-ran the 1ms/5ms/50ms sweep against
current code (1 partition, `shards=4`, `max_in_flight_per_partition=1000`,
n=3,000, single run per value):

| `linger` | Throughput | Mean batch |
|---|---|---|
| 1ms | **22,281.0 rec/sec** | 428.7 |
| 5ms (current default) | 16,701.4 rec/sec | 428.7 |
| 50ms | 10,102.0 rec/sec | 750.2 |

Clean and monotonic — 1.33x faster at 1ms than 5ms, 2.21x faster than 50ms —
and mechanistically explained rather than just a numeric win: mean batch
size is *identical* at 1ms and 5ms, so the extra 4ms of waiting bought zero
additional coalescing in this run; the sink's own pipelined submission
(records queuing up to `max_in_flight_per_partition` in quick succession)
already saturates the batch the linger timer would have waited for, so a
shorter timer just flushes the same-size batch sooner. Only at 50ms does the
batch grow (750.2, pinned near the `max_in_flight_per_partition` cap), and
even then the added coalescing doesn't pay for the wait: throughput is still
worse than the 1ms row.

**Recommendation: the data supports `linger: 1ms`** as a better default than
the current `5ms`, for this workload shape. Two caveats before treating this
as settled: each value was run once (not repeated), and the test produces
its whole volume in one burst (`ProduceSync` of the full batch at once) —
the best case for pipelined submission to saturate batches on its own. A
slower, steadier arrival rate closer to real production traffic could
plausibly still benefit from a longer linger, and this report has no data on
that shape. **`configs/example.yaml` still defaults to `linger: 5ms` — this
recommendation was not applied to the config as part of this measurement
task.**

The re-run above lands on the same 1ms-is-best conclusion as the original
serial-sink analysis kept below, but for a different, now-verified
mechanistic reason (pipelined submission saturating batches on its own,
rather than the serial sink never having more than one job to coalesce) —
see above, not the archival box below.

<details>
<summary>Original (now superseded) analysis, serial sink, pre-<code>ab2b47a</code></summary>

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

</details>

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
