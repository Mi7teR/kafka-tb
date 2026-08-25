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

## Sink throughput: baseline → pipelining → async publish → batcher sharding (reverted) → batcher fixes → ParseAmount fast path

Six point-in-time measurements of the same code path (Kafka record in,
TigerBeetle transfer applied, result published), each reproducing the prior
run's method for comparability. Full detail and raw per-run numbers were
kept in each run's own working notes; what they concluded is summarised here.
**What's measured vs. estimated:** every number in this table comes from a
real run against the same containerized Redpanda/TigerBeetle stack as the
load scenarios below — none of it is extrapolated or scaled up from a
smaller run. All six are single-machine, few-run measurements (1 run for
the baseline, 2 for pipelining, 3-4 for async publish, 2 for the headline
sharding rows, 2 for the batcher-fixes rows, 2 for the ParseAmount rows —
the shards/max-in-flight/linger sweeps in the sharding run are single runs
per value and flagged there as noisy) — order-of-magnitude, not an SLA; see
each run's notes for the per-run spread.

**Hardware:** Apple M4 Pro, macOS, Docker via OrbStack, go1.26.3 darwin/arm64,
for every column except the fifth. **The fifth column (batcher fixes) was
measured on a different machine** — Docker Desktop on macOS, TigerBeetle
running inside Docker Desktop's Linux VM rather than OrbStack's — see
the batcher-fixes run for why that is called out rather than silently
mixed in: absolute numbers across the two Docker backends are not
guaranteed comparable, only the ratios within each run are. **The sixth
column (ParseAmount fast path) returns to OrbStack** — the same backend as
the first four columns and as the CDC table below — so it is directly
comparable to those, and only loosely so to the fifth.
**Config held constant across all six runs:** `batcher.linger=5ms`,
`batcher.max_batch_size=8189`, `batcher.max_queue=1000`, 1 partition unless
noted — the fifth and sixth columns' scratch harnesses both pin
`linger=5ms` explicitly even though the shared test harness's own default
moved to `1ms` after the sharding run's linger sweep, precisely so these
rows stay comparable to the other four. `sink.max_in_flight_per_partition`
did not exist before pipelining; it defaulted to 1000 from the "after
pipelining" column onward. `batcher.shards` did not exist before sharding
and does not exist after the revert. **The sixth column is the current
state of the code** (`1530614`): the only change on top of the fifth
column's `b21f2d4` that touches the sink's hot path at all is
`model.ParseAmount` going allocation-free (94.7 ns / 3 allocs → 11.3 ns / 0
allocs) — `internal/codec/jsonc`'s decode path calls `ParseAmount`, not
`FormatAmount`, so the CDC-side `FormatAmount` and encoder fixes landed in
the same window are not exercised by the sink at all.

| Metric | Baseline (serial sink, pre-`ab2b47a`; no commit sha recorded in the baseline run) | After pipelining (`ab2b47a`) | After async publish (`523ffd3`) | After batcher sharding (`173d712`, later reverted) | After batcher fixes, sharding reverted (`b21f2d4`) | **After `ParseAmount` fast path (`1530614`) — current** |
|---|---|---|---|---|---|---|
| Throughput, 1 partition (n=3,000) | 48.2 rec/sec | 4,494 rec/sec | 16,343 rec/sec (avg of 3 runs) | 16,459.5 rec/sec (avg of 2 runs) | 19,974.6 rec/sec (avg of 2: 18,329.8 / 21,619.3) | **20,317.7 rec/sec** (avg of 2: 19,856.6 / 20,778.8) |
| Per-record wall clock | 20.75 ms | 0.222 ms | 61.2 µs | 60.7 µs | 50.1 µs | **49.2 µs** |
| Mean batch size TigerBeetle sees | 1.0000 | 750 | 428.7 | 514.5 (avg of 2: 600.2 / 428.7 — see the sharding run on why 428.7 recurs exactly) | 750.2 (both runs identical, n=4 TB calls each) | **1,000.0** (both runs, n=3 TB calls each — see below on why this recurs at exactly `max_in_flight_per_partition`) |
| p99 batch size | n/a (all batches = 1) | 1,000 (capped by `max_in_flight_per_partition`) | ~594 | ~663-1,000 (2 runs) | 1,000 (both runs) | **1,000** (both runs) |
| Throughput, 12 partitions (n=3,000 or 1,500) | ~1.06x vs 1p (no effect) | 2.20x vs 1p (12p faster) | ~1.03x vs 1p (parity) | 1.36x vs 1p (12p faster again — avg 22,341.0 rec/sec) | 1.16x vs 1p (avg 23,258.7 rec/sec) | **1.25x vs 1p** (avg 25,341.2 rec/sec: 25,633.2 / 25,049.2) |
| Dominant cost | TigerBeetle round trip (51%) + linger wait (30%) | `emit.Results` synchronous `ProduceSync` (92%) | TigerBeetle round trip (~55%), serialized one batch at a time | TigerBeetle round trip, split across up to `shards` (4) concurrent batches | TigerBeetle round trip, one batch at a time (no sharding), ~36-44% of wall clock | **TigerBeetle round trip, unchanged in kind — still the dominant cost** |

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
  §2 of the sharding run).
- **At 1 partition, sharding changes nothing — confirmed directly, not just
  inferred.** All of one partition's records share one ordering key, so
  `pickShard` always routes them to the same worker no matter how many
  shards exist: a direct `shards=1` vs `shards=4` check at 1 partition came
  back 12,982.3 vs 12,983.5 rec/sec, a 0.01% difference (the sharding run
  §3). This is the expected result of the routing design, not a null
  finding.
- **The `shards` sweep itself (1/4/8/16 at 12 partitions) did not produce a
  clean ranking** in a single run per value — `shards=1` was actually
  fastest in that run, ahead of the default `shards=4` (the sharding run
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
  either way (§7 of the sharding run).

### Decision: batcher sharding was reverted

Sharding was implemented (`b9c4bfb`, `173d712`), measured (the fourth column
above and the sharding run), and then reverted. The measurements are left
above exactly as they were taken; this section records what was concluded
from them.

What the numbers said:

- **1 partition: 16,343 → 16,459 rec/sec** — inside the run-to-run noise, and
  expected: one partition is one ordering key, so it lands on one shard
  whatever `shards` is set to.
- **12 partitions: 1.36x** vs 1 partition, against ~1.0x parity before
  sharding. The only column where sharding did anything.
- **The `shards` sweep contradicted the premise.** 1 shard → 22,944 rec/sec,
  4 → 18,861, 8 → 20,968, 16 → 22,276. One shard was the fastest value
  measured. The sweep is noisy — a single run per value, only 4-18
  TigerBeetle calls per run — so it does not establish that sharding is
  *harmful*. It does mean nothing in the data supports sharding.

What it cost: real complexity in `internal/tbx/batcher.go`, the most
safety-critical file in the project, plus a concurrency defect that only code
review caught — commands sharing an ordering key but differing in operation
type were routed to separate worker pools and could fly concurrently, so a
`create_accounts` and a transfer debiting that account could reach TigerBeetle
out of order and come back `debit_account_not_found`: a business rejection,
dead-lettered and committed, never retried. `173d712` fixed that, but it is
the kind of defect the design invites. An unproven benefit does not buy it.

**The question is open, not settled.** The one measurement that would decide
it was never run: a repeated, properly-sampled sweep at higher partition
counts — enough TigerBeetle calls per run for the numbers to mean something —
with `linger: 1ms` in place, since the linger change alters exactly the
mechanism (how full a batch gets while the previous one is in flight) that
sharding was meant to exploit. Anyone revisiting this should run that first;
the revert is a response to absent evidence, not to evidence of harm.

### Batcher fixes, measured after the revert

After the sharding revert, two more changes landed in `internal/tbx/batcher.go`:
`SubmitAsync` no longer parks a goroutine on the process-wide `finished`
channel in the common case (mutex delay 2,754.65 ms → 0 over
the batcher-fixes commit's own profiling window), and the per-send
1 MiB slice became a reused worker-owned buffer (`sendTransfers` allocation
121.31 MB → 2.5-4 MB, same window). Neither change touches the TigerBeetle
round trip itself, which the async-publish run had already identified as the
dominant cost (55% of wall clock) and *waiting*, not CPU or allocation — so
a small-to-nil throughput change was the expected outcome going in, and
that commit's own quick before/after readout found exactly that
(47,923 → 49,409 → 49,748/50,126 rec/s, a ~1.03-1.05x wobble its own text
flags as too noisy — a different, faster measurement window than the one
below — to read as a result).

Measured properly (the batcher-fixes run, same warm-up/no-settle-tail
method as the async-publish and sharding runs), the
gain is real and larger than that quick check suggested: **~1.22x** at 1
partition (16,343 → 19,974.6 rec/sec avg), and the 12-partition lever —
flattened to parity by async publication — is **partially back** (1.16x,
short of pipelining's 2.20x or sharding's 1.36x, but repeatable across 2
runs and achieved with *no* sharding in the build). The mechanism: cheaper
dispatch (`SubmitAsync`'s removed contention) lets the batcher's own loop
iterate faster and compose larger batches, which is visible directly in
this column's own mean batch size (750.2 vs the async-publish run's 428.7) and in
TigerBeetle's share of wall clock dropping from ~55% to ~36-44% — not
because the round trip got cheaper, but because everything around it did.

The `sink.max_in_flight_per_partition` sweep (1,000 vs 8,000, the async-publish run
§4's exact scenario) was re-run and **still shows a real gain — 1.23x
(avg of 2 runs each), against the async-publish run's 1.29-1.45x** — smaller than
before, consistent with the ack-tail cost this knob amortizes now being a
slightly smaller share of a smaller total. The finding still holds; the
report does not recommend a specific new default (see
§4 of the batcher-fixes run for why — changing this knob trades
throughput against replay-tail cost on an ungraceful stop, a separate
decision this task did not make).

### `ParseAmount` fast path, sink re-measured (`1530614`) — current

The only change on top of the batcher-fixes column (`b21f2d4`) that touches
anything the sink's hot path runs is `c2a0141`: `model.ParseAmount` gained a
`uint64` fast path (94.7 ns / 3 allocs → 11.3 ns / 0 allocs, per the
microbenchmark table above). The sink's decode path
(`internal/codec/jsonc`) calls `ParseAmount` once per transfer amount, so
this is on the hot path in a way `FormatAmount`'s CDC-side fix (below) is
not.

Measured with the same instrumented-decorator method and warm-up/no-settle-
tail discipline as the batcher-fixes run (two runs each, this table's own
Docker-via-OrbStack environment): **1 partition moved from 19,974.6 to
20,317.7 rec/sec, a ~1.7% change** — inside the run-to-run spread both
columns already show on their own (18,329.8-21,619.3 then, 19,856.6-20,778.8
now). **12 partitions moved from 23,258.7 to 25,341.2 rec/sec, ~9%,
partially outside that noise band but still a small effect next to
pipelining's 2.20x or the batcher fixes' own 1.22x.** This is the expected
result, not a surprising one: `perf-sink-after-batcher.md` had already found
the sink bounded by the TigerBeetle round trip and ack wait (13-19 ms each),
not CPU or allocation, and 11.3 ns saved once per transfer cannot move a
millisecond-scale bottleneck. **The honest reading is that this fix does
essentially nothing for sink throughput** — it was worth taking for the
allocation profile (0 allocs on the parse side, see the microbenchmark
table), not for records/sec.

One number moved for a reason unrelated to the code change: mean TigerBeetle
batch size in this run's own two 1-partition runs landed on **1,000.0**
(both runs, `n=3` `CreateTransfers` calls each — the batcher-fixes run
landed on 750.2, `n=4` calls each). This is the same phenomenon the
batcher-fixes run's own note and the sharding run's before it both flagged:
at this burst-produced, 1-partition, 5ms-linger shape, mean batch size is a
deterministic function of exactly how the test producer's single
`ProduceSync` call interleaves with the sink's pipelined passes, and it can
differ between two runs of a scratch harness that are otherwise identical in
every configured knob. It is not evidence of a behavioral change, and the
amortized-cost calculation below accounts for it directly rather than
assuming it away.

Amortized cost against the 6.13 µs/event floor (mean TB RTT / mean batch
size): 1 partition, 17.32 µs/event (run 1) and 16.49 µs/event (run 2), avg
**16.9 µs/event (2.76x the floor)** — down from the batcher-fixes run's 3.2x,
almost entirely because this run's larger mean batch (1,000 vs 750.2)
amortizes the same TigerBeetle round trip over more events, not because the
round trip itself changed. 12 partitions: 13.76 / 15.36 µs/event, avg
**14.6 µs/event (2.37x the floor)**, same mechanism.

**Do not compare the CDC numbers below to the sink table above.** They
measure the other direction of the bridge: one *change event* read out of
TigerBeetle, encoded and published to Kafka, versus one *Kafka record*
decoded and applied to TigerBeetle. Different unit of work, different
database operation, different message size (867 bytes on the wire against
257 — see below). They are kept in a separate table for that reason.

Full method and every scenario are in the CDC run's notes; the profiling
that explains these numbers is in the profiling run's.

**Hardware and stack:** Apple M4 Pro, 12 cores, macOS 26.5.2, go1.26.3
darwin/arm64, Docker via OrbStack — the same machine as everything above. Same
`redpanda:v25.2.4` and `tigerbeetle:0.17.9` containers as the integration
suite. **TigerBeetle needs io_uring, which macOS does not have, so it runs
inside the OrbStack Linux VM behind a forwarded port; every absolute latency
here includes that boundary.** Ratios within this environment are more
trustworthy than the absolutes.

**What is measured vs. estimated:** everything below is a real run. Each
scenario was run **twice**, in two independent container lifetimes, and both
runs are shown. Change events were generated by writing transfers straight to
TigerBeetle (bypassing Kafka and the sink), and throughput is measured from the
job's own query and publish calls — the first three windows are discarded as
warm-up and idle `poll_interval` waits are excluded by construction, so no
group-join or settle-window tax is inside the clock. Nothing is extrapolated.

### `cdc.batch_size` sweep — 200,000 events, 1 partition

| `cdc.batch_size` | Run 1 | Run 2 | µs/event | Windows | vs `100` |
|---|---|---|---|---|---|
| 100 | 19,624 events/sec | 19,438 events/sec | 51.0 / 51.4 | 1,997 | 1.00x |
| **1000** (current default) | 42,048 | 44,191 | 23.8 / 22.6 | 197 | **2.16x** |
| **2730** (TigerBeetle's real ceiling) | **105,364** | **102,157** | 9.5 / 9.8 | 71 | **5.32x** |
| 8189 (`config.MaxBatchSize`) | — | — | — | — | **job never advances** |

Per-stage split at the current default (`batch_size: 1000`, 1 partition, run 1 /
run 2, mean per window):

| Stage | p50 (r1) | p99 (r1) | mean (r1 / r2) | % of window |
|---|---|---|---|---|
| `GetChangeEvents` | 15.31 ms | 19.68 ms | 14.58 / 13.73 ms | **61%** |
| encode (window minus closing record) | 4.84 ms | 6.58 ms | 4.73 / 4.65 ms | 20% |
| `ProduceSync #1` (all but the closing record) | 3.68 ms | 5.46 ms | 3.67 / 3.46 ms | 15% |
| encode (closing record) | 25.9 µs | 68.9 µs | 26.7 / 26.0 µs | 0.1% |
| `ProduceSync #2` (closing record alone) | 757 µs | 1.50 ms | 778 / 769 µs | 3.3% |

### Three results worth calling out

- **`cdc.batch_size: 8189` loads cleanly and then wedges the job forever.**
  `config.validate` bounds it by `config.MaxBatchSize` (8189), which is
  TigerBeetle's limit on a batch of *transfers* — 128 bytes each. A
  `types.ChangeEvent` is **384 bytes**, so the real ceiling is
  `floor((1 MiB − 128 B) / 384) = 2730`, confirmed by binary search against the
  running cluster: 2730 is accepted, 2731 is rejected with *"too much data was
  sent or requested in this batch"*. `Job.Run` retries a failed query forever
  and never escalates past WARN (its `stuck` counter only counts *publication*
  failures), so a misconfigured job is indistinguishable from an idle one. The
  first sweep attempt spent its whole 15-minute budget in that loop and
  published zero events.
- **The default of `1000` is measurably too low.** Almost all of a window's cost
  is per-*window*, not per-event — a 2,730-event query costs *less* wall clock
  than a 1,000-event one (12.1 ms vs 14.6 ms, both runs) — so raising
  `cdc.batch_size` to 2730 is worth **2.4x**, with no code change and no new
  failure mode. This is the largest single win found in either direction.
- **The two-step publish's extra barrier amortises exactly as designed.** The
  closing record's own round trip is a flat ~0.3–0.8 ms per window: 5.2–5.9% of
  a 100-event window, 3.3% at 1,000, **1.7% at 2,730**. It is not worth
  attacking at any sensible window size. On a *trickling* stream it is a
  different story — see below.

### Re-measured at `2730` after the `FormatAmount`/encoder fixes (`1530614`) — current

`2730` is no longer a swept alternative: `config.DefaultCDCBatchSize` now
*is* `config.MaxCDCBatchSize` (2730), so this is the shipped default, and
two code changes landed on top of the sweep above that are squarely on the
CDC job's hot path: `7a6eaed` gave `model.FormatAmount` a fast path (12.3x
per the microbenchmark table, and per the profiling run it had been **69.5%
of every allocation the CDC job made** — nine formatted amounts per event,
the transfer plus four balance fields on each side), and `9355f53` cut the
encoder's JSON serialisation from 8 allocations to 1. Both are inside the
"encode" stage this report's own method isolates by subtraction, not inside
`GetChangeEvents` — so the prediction going in was that the wait-dominated
61% `GetChangeEvents` share would be untouched and the ~20% encode share
would shrink.

Measured with the same instrumented-decorator method, same warm-up
(discard the first three windows), same no-settle-tail discipline, same
200,000-event seed, two independent container lifetimes:

| | Run 1 | Run 2 |
|---|---|---|
| **Throughput** | **144,226.2 events/sec** | **141,044.5 events/sec** |
| Per-event wall clock | 6.93 µs | 7.09 µs |
| Windows (measured) | 71 | 71 |
| Mean / p99 / max window size | 2,701.5 / 2,730 / 2,730 | 2,701.5 / 2,730 / 2,730 |

**vs the original `2730` baseline (105,364 / 102,157 events/sec): ~1.37x
(avg 142,635 vs avg 103,761).** This is a real, repeatable move, not noise —
both new runs land 34-37% above both old runs, no overlap. Per-stage split
confirms the predicted mechanism exactly:

| Stage | mean, old baseline (r1 only) | mean, this run (r1 / r2) | change |
|---|---|---|---|
| `GetChangeEvents` | 12.08 ms | 11.65 / 12.37 ms | flat, as predicted — this stage does no encoding |
| encode (window minus closing) | 8.92 ms | 3.70 / 3.42 ms | **~2.5x cheaper** |
| `ProduceSync #1` | 4.16 ms | 3.07 / 3.05 ms | smaller, consistent with less to serialize per call |
| encode (closing record) | not published | 7.3 / 6.5 µs | no old figure to compare against |
| `ProduceSync #2` | 0.43 ms | 306 / 312 µs | smaller |

(`perf-cdc.md` §2b's mean-per-window table only reports Run 1's per-stage
breakdown at `batch_size: 2730`, and it does not include the closing-record
encode row at all — only the headline `batch_size: 1000` table does. Run
2's per-stage numbers, and the closing-record encode figure at 2730, were
never published for the old baseline; only its throughput and µs/event
figures exist for both runs.)

**Reading it: the fix landed exactly where the profiling said it would.**
Encoding — working, not waiting — shrank by roughly the ratio
`FormatAmount`'s own microbenchmark predicted, and `GetChangeEvents` — the
TigerBeetle round trip, unrelated to either fix — did not move outside its
own run-to-run noise (12.08 ms old vs 11.65/12.37 ms new). Because encode
shrank sharply and the wait stage did not move at all, `GetChangeEvents`'s
*share* of the window rose substantially: summing the old baseline's own
published stages (12.08 + 8.92 + 4.16 + 0.43 = 25.59 ms) puts it at **~47%**
of the old `batch_size: 2730` window, against **~62-65%** of this run's
smaller window (11.65-12.37 ms of an 18.7-19.2 ms total) — the arithmetic of
a fixed-in-kind cost becoming relatively more dominant once the cost next to
it shrinks, not a regression in either stage. **This is the larger of the
two optimisations measured in this round**, unlike the sink-side
`ParseAmount` fix above, because it landed on a cost this job's own
profiling had already identified as its single
biggest *working* expense — where the sink's bottleneck was never CPU or
allocation to begin with.

Not re-run here: the `batch_size: 100`/`1000`/`8189` sweep points, the
partition-count sweep, the trickle scenarios, and cursor recovery. Nothing
in this round's data suggests any of those would behave differently — the
fixes measured here are on the encode stage only, which those scenarios did
not identify as their bottleneck either — but that is an inference, not a
re-measurement, and is stated as such.

### Other CDC scenarios

| Scenario | Result |
|---|---|
| Output-topic partitions (1 / 12 / 32, `batch_size: 1000`) | 41,335 / 45,625 / 53,646 events/sec (mean of 2). **Weak and unreliable lever** — the two runs disagree at 32 partitions (62,511 vs 44,781). Choose partitions for consumer parallelism, not producer throughput. |
| `cdc.poll_interval` on a trickle (20 events/sec) | A **latency** knob, not a throughput knob. `100ms`: 1.82 events/window, publication lag p50 38 ms / p99 67 ms. `1s`: 9.4–10.9 events/window, p50 47–54 ms / p99 125–187 ms. The shipped `1s` default is defensible. |
| Publish-barrier cost per event, dense vs trickle | **1.7 µs/event** at `batch_size: 2730` against **1.93 ms/event** on a 20/sec trickle — ~1,100x. A consequence of window size, not of waiting; no `poll_interval` setting fixes it. |
| Cursor recovery (`cdc.Resume`) at startup | **5–20 ms**, and **flat** in both history depth and partition count (32 partitions with 200,000 records behind them is <10 ms slower than an empty topic). The scan is O(partitions) — one record read per partition — so it cannot degrade as the topic ages. A third to a half of it is the two offset-listing round trips. Not a problem; the 30 s `scanTimeout` has three orders of magnitude of headroom. |
| Message size | CDC record **867 bytes** on the wire (701 body + 36 key + 130 headers) vs **257 bytes** for a one-transfer sink input message — **3.37x**. Structural: a CDC event carries the transfer plus a full post-event balance snapshot of both accounts. |

### Where the CDC job's time and memory actually go

From the profiling run, over a 178,000-event profile window:

- **The job goroutine is blocked 73.5% of the time**, 52.3% of it inside
  `GetChangeEvents` — a single synchronous TigerBeetle round trip per window.
  That is *waiting*, and raising `cdc.batch_size` is how you buy it back.
- **`model.FormatAmount` is 69.5% of every object the job allocates** and 48% of
  the encoder's CPU. A CDC message carries nine formatted amounts (the transfer
  plus four balance fields on each account), each routed through `math/big` and
  `fmt.Sprintf`. That is *working*, and it is the one clearly avoidable CPU cost
  in either direction.
- **The whole process runs at about half of one core out of twelve** in both
  directions. A CPU profile alone would have ranked nothing useful.

To collect the same four profiles from a running connector, set `pprof: true`
in the config (default off — the endpoints expose the process's command line,
goroutine stacks and heap to anything that can reach `metrics_addr`) and pull
them from `metrics_addr`:

```sh
go tool pprof "http://$ADDR/debug/pprof/profile?seconds=30"   # CPU
go tool pprof "http://$ADDR/debug/pprof/block"                # where goroutines wait
go tool pprof "http://$ADDR/debug/pprof/mutex"                # contention
go tool pprof "http://$ADDR/debug/pprof/allocs"               # allocation
```

The flag also turns on Go's block and mutex profilers, which are off by default
and would otherwise report nothing.

## End-to-end load scenarios (sink direction)

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

§5 of the sharding run re-ran the 1ms/5ms/50ms sweep against
current code (1 partition, `shards=4`, `max_in_flight_per_partition=1000`,
n=3,000, single run per value):

| `linger` | Throughput | Mean batch |
|---|---|---|
| 1ms | **22,281.0 rec/sec** | 428.7 |
| 5ms (default at the time) | 16,701.4 rec/sec | 428.7 |
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
the then-current `5ms`, for this workload shape. Two caveats before treating this
as settled: each value was run once (not repeated), and the test produces
its whole volume in one burst (`ProduceSync` of the full batch at once) —
the best case for pipelined submission to saturate batches on its own. A
slower, steadier arrival rate closer to real production traffic could
plausibly still benefit from a longer linger, and this report has no data on
that shape. **This recommendation has since been applied:
`configs/example.yaml` now defaults to `linger: 1ms`**, with the caveats above
standing — a steadier arrival rate than this burst-shaped test could still
favour a longer window.

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
  transfers + 10 accounts), with no `rejected` or `poison` results. Every
  message in this run carried exactly one event, so the per-record and
  per-event counts coincided; `kafkatb_events_total` is the one that stays 210
  when a message carries several transfers.
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
