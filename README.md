# kafka-tb

A two-way bridge between Kafka and [TigerBeetle](https://tigerbeetle.com).

TigerBeetle is a fast double-entry accounting database, and that is all it does. It speaks a
binary protocol over cgo — not HTTP, not SQL — and its data model is 128-bit integers, bit-mask
flags and numeric codes. There is no way to hand it an event from Kafka, and no way for the rest
of your system to learn what it recorded. This service is the bridge across that gap.

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

`create_accounts` and `create_transfers` are supported, including two-phase transfers
(`pending` / `post_pending_transfer` / `void_pending_transfer`) and atomic `linked` chains. A chain
is atomic within one message and is never split across TigerBeetle batches.

One message carries operations of exactly one kind. Decoding is strict: an unknown field is a
poison record, because `"amont"` silently becoming a zero amount on a money path is worse than a
rejected message.

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

Measured on Docker for macOS (OrbStack for this table's numbers; some earlier rows in the linked
history used Docker Desktop instead) with TigerBeetle inside a Linux VM either way, so treat the
ratios as more portable than the absolutes. Full method and history in
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

Re-measured against `1530614` (the commit under test) after two allocation fixes:
`model.ParseAmount` going allocation-free (sink decode path) and the CDC job's `FormatAmount` fast
path plus a JSON-encoder allocation cut (8 allocations → 1). **Read the two directions separately: the sink
barely moved (~1.7% at 1 partition, ~9% at 12 — inside or just outside run-to-run noise), because
`ParseAmount` was already nanoseconds against a TigerBeetle round trip measured in milliseconds.
CDC moved substantially, ~1.37x (105,364/102,157 → 144,226/141,045 events/sec at `batch_size:
2730`), because encoding — not waiting — was the cost those two fixes actually cut, and CDC's
encode stage was a much larger share of its window than the sink's decode ever was of its own.**
Full method, both runs, and the per-stage breakdown are in
[docs/benchmarks/README.md](docs/benchmarks/README.md).

### Hot-path microbenchmarks

Apple M4 Pro, Go 1.26.3, median of 6 runs. Raw output in
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
make bench         # benchmarks
make lint
make generate      # regenerate easyjson marshalers
```

The integration suite boots both containers itself. It covers idempotent replay, garbage
interleaved with valid records, crash-and-restart, linked-chain atomicity, and — for CDC — a
mid-window crash driven through the production publication seam.

## Not in scope

No API: reading balances over HTTP is not this service's job. No exactly-once. No business logic —
who owes whom and why is the producer's decision. No import of historical transfers: the
`imported` flag is surfaced on read and rejected on write.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
