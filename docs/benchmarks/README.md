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
pipelining and async DLQ/results publication — see the three-way table
below). Every number here is within run-to-run noise of the `99ebf66`
figures above (e.g. `BenchmarkDecodeJSON/n=1` median 2,024ns vs 1,990ns):
none of the code these microbenchmarks cover (decode, amount/ID parsing,
result mapping, batch assembly) changed in either perf commit, so an
unchanged result here is the expected outcome, not a null result.

## Sink throughput: baseline → pipelining → async publish

Three point-in-time measurements of the same code path (Kafka record in,
TigerBeetle transfer applied, result published), each reproducing the prior
run's method for comparability. Full detail and raw per-run numbers are in
`.superpowers/sdd/perf-baseline.md`, `.superpowers/sdd/perf-after-pipelining.md`,
and `.superpowers/sdd/perf-final.md`. **What's measured vs. estimated:** every
number in this table comes from a real run against the same containerized
Redpanda/TigerBeetle stack as the load scenarios below — none of it is
extrapolated or scaled up from a smaller run. All three are single-machine,
few-run measurements (1 run for the baseline, 2 for pipelining, 3-4 for the
final column) — order-of-magnitude, not an SLA; see each linked doc for the
per-run spread.

**Hardware:** Apple M4 Pro, macOS, Docker via OrbStack, go1.26.3 darwin/arm64
(same machine for all three). **Config held constant across all three runs:**
`batcher.linger=5ms`, `batcher.max_batch_size=8189`, `batcher.max_queue=1000`,
1 partition unless noted. `sink.max_in_flight_per_partition` did not exist
before pipelining; it defaulted to 1000 for the "after pipelining" and
"final" columns.

| Metric | Baseline (serial sink, pre-`ab2b47a`; no commit sha recorded in `perf-baseline.md`) | After pipelining (`ab2b47a`) | **Final (`523ffd3`, + async publish)** |
|---|---|---|---|
| Throughput, 1 partition (n=3,000) | 48.2 rec/sec | 4,494 rec/sec | **16,343 rec/sec** (avg of 3 runs) |
| Per-record wall clock | 20.75 ms | 0.222 ms | **61.2 µs** |
| Mean batch size TigerBeetle sees | 1.0000 | 750 | **428.7** |
| p99 batch size | n/a (all batches = 1) | 1,000 (capped by `max_in_flight_per_partition`) | ~594 |
| 12 vs 1 partition (n=1,500) | ~1.06x (no effect) | 2.20x (12p faster) | **~1.0x** (parity — no longer a lever, see below) |
| Dominant cost | TigerBeetle round trip (51%) + linger wait (30%) | `emit.Results` synchronous `ProduceSync` (92%) | **TigerBeetle round trip (~55%), still serialized one batch at a time** |

Two results worth calling out because they reverse the previous column's
conclusion rather than just extending it:

- **Partition count stopped mattering.** Pipelining made partition count a
  real throughput lever (2.20x at 12p) because the bottleneck at that point
  (`emit.Results`) ran once per record on each partition's own goroutine, so
  more partitions meant more concurrent produce calls. Async publication
  moved the bottleneck to `tbx.Batcher`'s `CreateTransfers`, which keeps
  exactly one in-flight batch *shared across all partitions* — so adding
  partitions no longer adds independent throughput, and four repeat runs at
  1 vs 12 partitions came back at 0.86x-1.11x, centered on parity.
- **`sink.max_in_flight_per_partition` is still a real, measurable limiter**,
  just a smaller one now: raising it from the default 1,000 to 8,000 gave
  1.29x-1.45x more throughput (two runs), down from the 14.8x swing
  pipelining found between 1,000 and a pathologically low value of 5. See
  `perf-final.md` §4 for why the mechanism changed (it now bounds how many
  records one `pass()` accepts before paying a per-pass publish-confirm
  tail, not batch size directly — this run's batches never reached the cap).

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

### `linger`: the smallest-value conclusion below is outdated — read this first

**Everything in this subsection was written against the original serial sink
and is invalidated by the pipelining work.** It is kept verbatim below for
the record, not as current guidance.

The serial sink (`internal/sink.Sink.processBatch` as it existed at the time)
consumed and applied one Kafka record at a time: it called `Submit` and
blocked on the outcome before moving to the next record. Because
`tbx.Batcher.Submit` was only ever called once at a time from that loop, the
batcher's linger window genuinely never had more than one job to coalesce
across *separate Kafka messages* — the "smallest linger wins" conclusion
followed correctly from that constraint. It does not follow anymore: the
sink now pipelines up to `sink.max_in_flight_per_partition` records per
partition (`internal/sink/sink.go`, `pass`), enqueuing them into the batcher
in quick succession rather than one at a time, so linger *does* now coalesce
across separate Kafka messages — `.superpowers/sdd/perf-final.md` measured a
mean TigerBeetle batch size of **428.7** at the unchanged `linger: 5ms`
default, which would be structurally impossible if linger still coalesced
nothing.

**What linger value the current data argues for:** directionally, some
non-minimal value — but this repo has not re-run the 1ms/5ms/50ms sweep
below against the pipelined-plus-async-publish code, so there is no
re-derived specific number to give here. `perf-final.md`'s closing section
says this explicitly rather than guessing: raise it if throughput needs to
improve further and the linger sweep below hasn't been redone, but treat
`linger: 5ms` as merely "not yet shown to be wrong" rather than "confirmed
optimal" until that sweep is repeated on current code.

<details>
<summary>Original (now outdated) analysis, serial sink, pre-<code>ab2b47a</code></summary>

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
