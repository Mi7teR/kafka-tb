# kafka-tb

[![CI](https://github.com/Mi7teR/kafka-tb/actions/workflows/ci.yml/badge.svg)](https://github.com/Mi7teR/kafka-tb/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/Mi7teR/kafka-tb/branch/main/graph/badge.svg)](https://codecov.io/gh/Mi7teR/kafka-tb)

**Kafka → [TigerBeetle](https://tigerbeetle.com): a sink for applying financial commands from a
stream — plus CDC back out.**

Streaming change events *out* of TigerBeetle is a solved problem: the project ships
[`tigerbeetle amqp`](https://docs.tigerbeetle.com/operating/cdc/) for RabbitMQ, and Redpanda
Connect has a [`tigerbeetle_cdc` input](https://docs.redpanda.com/redpanda-connect/components/inputs/tigerbeetle_cdc/).
Applying a stream of commands *into* it is not, and that is the harder half: reading a change
stream is mostly a matter of not losing your place, while writing means deciding what to do when a
message is malformed, when TigerBeetle refuses the operation, and when TigerBeetle cannot be
reached — three cases that must behave differently, because dead-lettering an outage loses money
and retrying a malformed message forever stops the world.

This service does that, and streams changes back out in the same vocabulary so one contract covers
both directions.

```
Kafka topic ──►  kafkatb sink  ──►  TigerBeetle     apply transfers
TigerBeetle ──►  kafkatb cdc   ──►  Kafka topic     stream changes out
                 kafkatb run                        both in one process
```

## What it gives you

**A human contract instead of a binary one.** Producers write UUID strings, `"USD"` instead of
`1`, `"payment"` instead of `718`, `"12.34"` instead of `1234`, and flags as names. The ledger and
code registries live in config; amounts are scaled per ledger. The same vocabulary is used on the
way in and on the way out.

**Idempotency without exactly-once.** Transfer ids come from the producer, so a replay lands as
`exists` and counts as success. Restart the service, replay a topic, re-drive a dead-lettered
record — balances do not move twice.

**Bad data cannot stop the stream.** Malformed input is dead-lettered and the consumer moves on.
A panic while handling one message becomes a poison record, not a dead process.

**Losing a record is the one thing that cannot happen.** An offset is committed only after
TigerBeetle confirmed the operation *and* the broker acknowledged the dead-letter/results write,
and only as a contiguous prefix — one unfinished record holds the line.

## How this compares

Three things stream TigerBeetle data today. Two of them are more mature than this one, and for a
lot of use cases they are the right answer.

| | [`tigerbeetle amqp`](https://docs.tigerbeetle.com/operating/cdc/) | [`tigerbeetle_cdc`](https://docs.redpanda.com/redpanda-connect/components/inputs/tigerbeetle_cdc/) + Redpanda Connect | kafka-tb |
|---|---|---|---|
| Maintained by | TigerBeetle, first-party | community tier, not Redpanda-supported | this repo |
| TigerBeetle → broker | RabbitMQ | anywhere Redpanda Connect can write | Kafka |
| Broker → TigerBeetle | — | — | **yes** |
| Downstream reach | one exchange | 300+ connectors, plus filtering, transforms, routing | one Kafka topic |
| Delivery | at-least-once | at-least-once | at-least-once |
| CDC progress | stateless, resumes from what the broker acked | external `progress_cache` resource (required) | stateless, reads the output topic's own tail |
| Values on the wire | TigerBeetle's fields, long integers as JSON strings | JSON mirroring the change event | named ledgers and codes, decimal amount strings, UUID ids, named flags |
| Write-side failure handling | n/a | n/a | poison / reject / infrastructure split, byte-identical DLQ payload with the reason in headers |
| Write-side ordering | n/a | n/a | preserved within a Kafka partition, end to end |
| Offset safety | n/a | n/a | committed only after TigerBeetle confirmed **and** the broker acked, contiguous prefix only |

Their columns were read from the linked upstream docs in August 2026; if something there has moved
since, the docs are right and this table is stale.

### When to pick one of the others

**Pick `tigerbeetle amqp`** if you are on RabbitMQ, or if you want the thing the TigerBeetle team
itself ships and supports. It is first-party, it is the reference implementation of this pattern,
and nothing here is a reason to run a third-party job instead.

**Pick `tigerbeetle_cdc` with Redpanda Connect** if what you need is TigerBeetle *out* — especially
out to somewhere that is not Kafka. You get a mature stream-processing runtime, filtering and
transformation before the data lands, and a few hundred destinations for free. If your pipeline is
already Redpanda Connect, adding one input beats adding one service. Note it needs an external
cache resource for progress, where this project reads its own output topic.

**Pick neither** if you only read from TigerBeetle and never write into it from a stream. Half of
this project would be dead weight.

### Where this is weaker, plainly

It is one repository maintained by one person, its CDC job must run as a single instance with no
leader election, it speaks JSON only so far, and its published numbers come from a laptop.

## The value contract

TigerBeetle stores a ledger as a `uint32`, a code as a `uint16`, an amount as an integer number of
minor units, and flags as a bitmask. Without a translation layer every producer has to know that
USD is `1`, that `payment` is `718`, and that `12.34` must be written as `1234` — knowledge that
then spreads across every service writing to the bus.

Two config tables replace it:

```yaml
ledgers:
  USD: {id: 1, scale: 2}
  # JPY: {id: 3, scale: 0}   # no minor unit at all
  # BTC: {id: 4, scale: 8}   # satoshi
codes:
  payment: 1
```

`scale` is the ledger's decimal places, and it is what turns the string into minor units. At scale
2, `"12.34"` is `1234`; on a scale-8 ledger the same string is `1234000000`; on a scale-0 ledger it
is rejected, because that ledger has no fractional part.

**Amounts travel as strings**, never as JSON numbers — a JSON number is a `float64`, which cannot
represent decimal fractions exactly. Internally they are `big.Int` and integers; `float64` is
banned anywhere near money.

**More decimals than the ledger's scale is a rejection, not rounding.** `"12.345"` on a scale-2
ledger is a poison message. Silently rounding somebody else's money is worse than dead-lettering
the record that asked for it.

**Flags travel as names**, not as a mask: `linked`, `pending`, `post_pending_transfer`,
`void_pending_transfer`, `balancing_debit`, `balancing_credit`, `closing_debit`, `closing_credit`
for transfers; `linked`, `debits_must_not_exceed_credits`, `credits_must_not_exceed_debits`,
`history`, `closed` for accounts. An unrecognised name is poison rather than a silently ignored
flag — `"balancing_debt"` is a typo that would otherwise change what a transfer means.

`imported` is the one asymmetric flag: it is reported on read but refused on write, because
importing requires caller-supplied event timestamps that this connector does not support — and
hiding it on an already-imported record would misreport stored state.

### The same vocabulary in both directions

The CDC job emits exactly this contract, so an event coming out reads with the same code that
builds a message going in.

### An unknown name behaves differently by direction, deliberately

| | unknown ledger or code |
|---|---|
| sink, writing | **poison** — dead-lettered |
| cdc, reading | published as the **raw number**, amounts **unscaled**, warned once per id |

Writing with a guessed value would put wrong data into the ledger, so refusing is the safe move.
But on the way out the event has already happened and the money has already moved; dropping a real
financial event because the config lagged behind reality would lose the record of it. So it goes
out honestly, as a number rather than a name.

The warning is logged once per unknown id, not once per event: a gap in the registry is a static
condition affecting every event on that ledger, and a line per occurrence would flood the log at
full stream rate while telling the operator nothing the first line did not.

`create_accounts` and `create_transfers` are supported, including two-phase transfers
(`pending` / `post_pending_transfer` / `void_pending_transfer`) and atomic `linked` chains. A chain
is atomic within one message and is never split across TigerBeetle batches.

One message carries operations of exactly one kind. Decoding is strict: an unknown field is a
poison record, because `"amont"` silently becoming a zero amount on a money path is worse than a
rejected message.

## Quick start

Requires Go 1.25+ and `CGO_ENABLED=1` (the TigerBeetle client is cgo, so cross-compilation is not
available).

```bash
make build
./bin/kafkatb sink --config configs/example.yaml
```

Subcommands: `sink` (consumer only), `cdc` (change stream only), `run` (both). `--config` is
shared. Precedence is flags → `KAFKATB_*` environment → file → defaults.

> **macOS:** build and test through `make` only. TigerBeetle ships a prebuilt static library whose
> members are not 8-byte aligned, which Apple's current linker refuses; the Makefile passes
> `-ld_classic` on Darwin. A bare `go build ./...` fails at link time. Linux is unaffected.

## Message contract

### Into the sink

```json
{
  "operation": "create_transfers",
  "transfers": [
    {
      "id": "0193f8a1-7c2e-7000-8000-000000000001",
      "debit_account_id":  "0193f8a1-0000-7000-8000-000000000010",
      "credit_account_id": "0193f8a1-0000-7000-8000-000000000020",
      "amount": "12.34",
      "ledger": "USD",
      "code": "payment",
      "flags": ["linked"]
    }
  ]
}
```

### Out of CDC

The skeleton of TigerBeetle's official AMQP CDC message, with this project's value conventions —
`type`, `timestamp`, `ledger`, `transfer`, and snapshots of **both accounts as of the event**. See
[docs/cdc.md](docs/cdc.md) for the full shape, headers and consumer contract.

## Outcomes

Every record ends in exactly one of three classes:

| Class | Example | What happens |
|---|---|---|
| **poison** | malformed JSON, misspelled field, unknown ledger | dead-letter queue, offset advances |
| **reject** | TigerBeetle refused — `exceeds_credits` | results topic + DLQ with the reason, offset advances |
| **infrastructure** | TigerBeetle unreachable | **no commit**, the same record is retried |

Dead-lettered payloads are republished byte-identical, so replaying one requires no reassembly;
everything the connector knows goes into headers.

## Ordering

Order of application is guaranteed **within a Kafka partition** and is preserved end to end — have
producers partition by account. Across partitions there is no ordering guarantee and never was.

## Delivery guarantees

At-least-once in both directions.

The sink deduplicates through TigerBeetle itself: a replayed transfer id returns `exists`.

For CDC, deduplicate on `timestamp`, which TigerBeetle guarantees unique — but **apply it
idempotently**, as a seen-set or an upsert. Discarding anything at or below a running maximum is
wrong and loses events; [docs/cdc.md](docs/cdc.md) explains why.

## Operating

**Exactly one CDC instance may run against a given output topic.** The sink is fenced by its
consumer group; the CDC job has no group, no lock and no leader election. Two instances — an
overlapping rolling deploy, a stale pod — each publish the whole stream forever.

CDC keeps no state outside its output topic: on startup it reads the tail of every partition and
resumes from there. `--timestamp-last` overrides it, and `--timestamp-last=0` replays everything.

`tigerbeetle.addresses` takes an IP literal or a bare port. Hostnames are rejected at config load,
because the TigerBeetle client rejects them at connect time with a far less obvious error.

Metrics and health live on `metrics_addr` (`:9464` by default): `/metrics`, `/healthz`, `/readyz`,
and `kafkatb_records_total`, `kafkatb_events_total`, `kafkatb_dlq_total`, `kafkatb_tb_batch_size`,
`kafkatb_tb_latency_seconds`, `kafkatb_offset_commit_lag`. `pprof: true` adds `/debug/pprof/`; it
is off by default because those endpoints are an exposure surface.

An unknown key in the config file fails startup and names the key.

## Performance

All figures below were measured on a **MacBook Pro with an Apple M4 Pro (12 cores) and 24 GB of
RAM**, on Docker for macOS (OrbStack for this table's numbers; some earlier rows in the linked
history used Docker Desktop instead), with TigerBeetle running inside a Linux VM either way —
macOS has no io_uring, which TigerBeetle needs. That VM boundary is inside every absolute number
here, so **treat the ratios as portable and the absolutes as indicative**; a Linux host would very
likely produce different figures, and possibly a different bottleneck. Full method and history in
[docs/benchmarks/README.md](docs/benchmarks/README.md).

| | throughput | mean TigerBeetle batch |
|---|---|---|
| sink, 1 partition | ~20,300 records/sec | 1,000 events |
| sink, 12 partitions | ~25,300 records/sec | |
| cdc | ~142,600 events/sec | |

The sink is bounded by the TigerBeetle round trip at roughly 17 µs/event amortised, against a
measured 6.13 µs/event floor on a full batch — so headroom remains. The cheapest untaken wins are
raising `sink.max_in_flight_per_partition` (measured 1.23x) and having producers put several
transfers in one message.

CDC gained about 1.37x from two allocation fixes; the sink barely moved from the same class of
change, because its cost is the TigerBeetle round trip rather than encoding. Both runs, the
per-stage breakdown and the reasoning are in
[docs/benchmarks/README.md](docs/benchmarks/README.md).

### Hot-path microbenchmarks

MacBook Pro, Apple M4 Pro (12 cores), 24 GB RAM, macOS 26.5.2, Go 1.26.3; median of 6 runs.
These run in-process and do not touch the VM. Raw output in
[docs/benchmarks/6c461be.txt](docs/benchmarks/6c461be.txt).

| | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `ParseAmount` — decimal string → u128 minor units | 11.4 | 0 | **0** |
| `FormatAmount` — u128 → decimal string | 16.3 | 8 | 1 |
| `ParseID` — UUID → u128 | 13.5 | 0 | **0** |
| decode a 1-transfer message | 699 | 608 | 11 |
| decode a 10-transfer message | 6,162 | 9,104 | 69 |
| decode a 100-transfer message | 57,546 | 80,432 | 612 |
| encode one CDC event (whole record) | 1,224 | 2,224 | 35 |
| ↳ its JSON serialisation alone | 555 | 1,152 | 1 |
| ↳ the same via plain `easyjson.Marshal` | 783 | 2,467 | 8 |
| map a full 8,189-event TigerBeetle reply | 106,939 | 526,256 | 83 |

Money and identifier conversion is allocation-free on the parse side and one allocation — the
returned string — on the format side. Both are on the sink's per-transfer path, so the allocation
that is not there is the point rather than the nanoseconds.

The CDC encoder keeps one allocation per event for the JSON body, down from eight. That last one
cannot go while franz-go retains `Record.Value` until the broker acknowledges it, so a pooled body
would have to be released on the produce callback — reasoned about, deliberately not built.

The full-reply mapping works out to about 13 ns per event, so it does not register against the
TigerBeetle round trip it follows.

## Configuration

[configs/example.yaml](configs/example.yaml) documents every field, including the reasoning behind
values that were chosen from measurement rather than taste.

## Development

```bash
make test          # unit tests, -race
make integration   # end-to-end against real Redpanda and TigerBeetle containers (needs Docker)
make coverage      # merged unit + integration coverage, prints the total (needs Docker)
make bench         # benchmarks
make lint
make generate      # regenerate easyjson marshalers
```

The integration suite boots both containers itself. It covers idempotent replay, garbage
interleaved with valid records, crash-and-restart, linked-chain atomicity, and — for CDC — a
mid-window crash driven through the production publication seam.

### What the coverage number means

The badge is the unit and integration suites merged into one profile, which is the only way it
says anything true: `cmd/kafkatb` is exercised solely by the integration suite, which runs the
real binary as a subprocess and stops it with a real `SIGTERM`, while most of `internal/` is
exercised solely by the unit suite. Measuring either alone reports large parts of the project as
untested when they are not. `scripts/coverage.sh` produces the profile and names its two
exclusions — the generated `*_easyjson.go` marshalers, which the hand-written codec, CDC and emit
tests drive through the real API, and `cmd/loadgen`, a benchmarking tool rather than part of the
connector.

The number is reported, not enforced. A threshold that gates merges rewards tests written to move
the number, and a test that raises coverage without being able to fail is worse than no test: it
manufactures confidence and hides the gap it appears to close. New tests here are checked by
mutation instead — break the behaviour, watch the test fail, put it back.

## Not in scope

No API: reading balances over HTTP is not this service's job. No exactly-once. No business logic —
who owes whom and why is the producer's decision. No import of historical transfers: the
`imported` flag is surfaced on read and rejected on write.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
