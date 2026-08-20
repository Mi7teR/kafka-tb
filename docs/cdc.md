# CDC: TigerBeetle → Kafka

`kafkatb cdc` streams TigerBeetle's change events to a Kafka topic. It is this
project's counterpart to TigerBeetle's official `tigerbeetle amqp` job: the
same idea, the same guarantee, this connector's message vocabulary. It runs on
its own (`kafkatb cdc`) or beside the sink in one process (`kafkatb run`),
sharing a single TigerBeetle client either way.

The source is the Go client's `GetChangeEvents`, which TigerBeetle marks
**experimental and undocumented**. It is reached through one narrow interface
(`cdc.Source`), so a change to its signature lands in one declaration and one
adapter rather than across the package.

The job requires that answer to be a **strictly ascending, timestamp-ordered
prefix** of what follows the cursor. Everything below rests on it, so it is
checked on every window rather than trusted: a window that is not ascending
stops the job with an `ERROR` instead of being published.

## ⚠️ Exactly one CDC instance per output topic

**Run exactly one CDC instance against a given output topic. Nothing enforces
this.**

The sink is fenced by its Kafka consumer group: start a second one and the
group hands each partition to exactly one of them. The CDC job has no such
fence — no consumer group, no lock, no leader election — and none is planned.

Two instances (an overlapping rolling deploy, a stale pod nobody noticed) each
read the whole change stream and each publish all of it, forever. That is:

- **permanent duplication**, not the bounded, crash-scoped kind the
  at-least-once contract allows — a consumer sees every event twice for as
  long as both run; and
- **broken per-key ordering**: two writers interleaving on the same key make
  the per-account (or per-ledger, or per-transfer) order that
  `cdc.partition_key` exists to provide meaningless.

So: deploy the CDC job as a **single replica**, and cut the old instance over
— stopped, not merely draining — before starting the new one. `kafkatb run`
starts the CDC job too whenever `cdc.topic` is set, so the same rule covers
every `kafkatb run` replica.

## Configuration

```yaml
cdc:
  topic: ledger.events          # empty or absent: the job is disabled
  batch_size: 2730              # events per window; also the hard ceiling
  poll_interval: 1s             # wait after an empty answer
  partition_key: debit_account_id
```

`batch_size` defaults to 2730, which is also the largest value accepted: a
`ChangeEvent` is 384 bytes, so that is all TigerBeetle will return in one
request. (`batcher.max_batch_size`'s 8189 counts 128-byte *transfers*; the two
ceilings are not interchangeable, and a `cdc.batch_size` above 2730 is rejected
at startup rather than left to wedge the stream.) It defaults to the ceiling
because the cost of a window is almost entirely per-window: over 200,000
events on one partition, 100 gave ~19.5k events/sec, 1000 ~43k and 2730 ~104k.

`partition_key` is one of `debit_account_id` (default), `credit_account_id`,
`ledger`, `transfer_id`. It decides which ordering a consumer of the topic
gets — per debit account, per credit account, per ledger, per transfer — so it
is the consumer's decision, not the job's.

With `cdc.topic` empty, `kafkatb run` starts the sink alone and `kafkatb cdc`
refuses to start.

## Message format

The skeleton is the official job's; the values follow this connector's
contract, the same one the sink accepts on the way in — ledger and code as
names from the registries, amounts as decimal strings at the ledger's scale,
ids as UUID strings, flags as name lists. It is deliberately **not**
wire-compatible with the official AMQP consumer.

```json
{
  "type": "single_phase",
  "timestamp": "1745328372192037030",
  "checkpoint": "1745328372192037030",
  "ledger": "USD",
  "transfer": {
    "id": "00000000-0000-0000-0000-000000000065",
    "amount": "12.34",
    "pending_id": "...",
    "user_data_128": "...", "user_data_64": 64, "user_data_32": 32,
    "timeout": "1m30s",
    "code": "payment",
    "flags": ["pending"],
    "timestamp": "1745328372192037030"
  },
  "debit_account": {
    "id": "...",
    "debits_pending": "1.00", "debits_posted": "1250.00",
    "credits_pending": "2.00", "credits_posted": "3.00",
    "user_data_128": "...", "user_data_64": 641, "user_data_32": 321,
    "code": "customer",
    "flags": ["history"],
    "timestamp": "1745328372192037000"
  },
  "credit_account": { "...": "..." }
}
```

`type` is one of `single_phase`, `two_phase_pending`, `two_phase_posted`,
`two_phase_voided`, `two_phase_expired`. Both account snapshots are the state
TigerBeetle held as of the event, including each account's own timestamp.
Optional fields (`pending_id`, `user_data_*`, `timeout`) are omitted when
zero. `flags` is **always an array** — a transfer or account with no flags
carries `"flags": []`, never `null`, which is the common case and the one a
consumer's schema is most likely to see.

**A registry gap never costs an event.** An unknown ledger or code is
published with its numeric value in place of the name and a `WARN` naming the
event; amounts of an unknown ledger stay in minor units, since inventing a
scale would misstate the amount. Dropping a financial event because a config
entry is missing would be far worse than an ugly message.

The warning is emitted **once per distinct unknown value** — once per ledger
id, once per code, once per event type — not once per event. A gap is a static
condition affecting every event on that ledger, and warning per occurrence
would flood the log at the full event rate while telling the operator nothing
the first line did not.

### Headers

`event_type`, `ledger`, `transfer_code`, `debit_account_code`,
`credit_account_code`, `timestamp` — named after the official job's, so a
consumer can route or filter without parsing the body.

## Delivery: at-least-once

A window of events is published and **acknowledged in full before the cursor
moves past it**. Losing an event is unacceptable; duplicating one after a
crash is acceptable, and is the contract.

**Consumers must deduplicate on `timestamp`.** TigerBeetle's event timestamp
is unique, so it is a complete deduplication key on its own — no compound key
is needed.

But it has to be applied **idempotently, per event**: either

- keep a set of the timestamps already applied and skip an event whose
  timestamp is in it, or
- apply the event as an **upsert keyed on `timestamp`**, so re-applying it is
  a no-op.

**Do not keep a running maximum.** "Skip anything at or below the highest
timestamp I have handled" looks like it follows from uniqueness, and it loses
events. Two independent reasons, either one on its own is enough:

- **The topic is keyed and multi-partition, so there is no global timestamp
  order across it.** Records are routed by `partition_key`, and a consumer
  interleaves partitions in whatever order they arrive. Read `105` from
  partition 0, set max to 105, then read `103` from partition 1 — and `103`
  is discarded as already handled, though it never was.
- **Replay delivers events behind ones already handled.** After a crash
  mid-window the job republishes that whole window (see below). If `104`
  landed before the crash and `103` did not, the replay delivers `103` after
  `104` has been handled, and the maximum swallows it.

A **per-partition** high-water mark fixes only the first reason. The second
one still loses `103`, on its own partition, in timestamp order. Only
idempotent application keyed on `timestamp` is correct.

## Progress: no external state

Like the official job, this one stores its cursor nowhere. On startup it reads
the **last record of every partition** of the output topic and resumes from the
highest `checkpoint` it finds, plus one. An empty or missing topic starts from
zero. `--timestamp-last N` overrides that (including `--timestamp-last 0` to
replay everything). `kafkatb run` rejects `--timestamp-last` when `cdc.topic`
is empty rather than accepting a cursor for a job it is not going to start.

`checkpoint` is the field that makes this safe, and it reads: *every event with
a timestamp up to and including this one is present in this topic.* It is not
the same as the record's own `timestamp`, and the difference is the whole
point.

A window's records land on whatever partitions their keys select. A crash in
the middle of one leaves every partition holding some prefix of what was
destined for it, and those prefixes end at different timestamps — the topic's
partitions have **unequal tails**. The highest *event* timestamp then present
is therefore not a point the stream is complete up to: an earlier event routed
to a partition that fell behind may be missing. Resuming from it would leave a
permanent gap.

So the job publishes each window in two steps: everything except the record
carrying the window's highest timestamp first, all of them claiming the
checkpoint the window started from; then, only once those are acknowledged,
that one closing record alone, claiming its own timestamp. A record can only
claim a checkpoint that was already acknowledged in full, so the highest claim
anywhere in the topic is always true, whatever the tails look like around it.

The cost is one extra acknowledgement round-trip per window, and — when a
crash does interrupt a window — the republication of that whole window on
restart. That is duplicates, which the contract allows. A clean restart
resumes exactly where it left off: the last completed window's closing record
claims its own timestamp, so nothing is replayed.

A tail record that cannot be read (foreign, corrupt, or written by a future
version) is skipped with a `WARN` rather than failing the start: skipping one
lowers the cursor and costs duplicates, while trusting it could cost an event.

## Failures: retried forever, escalated when stuck

A failed query or a failed publication is retried from the same cursor with a
backoff, forever. Giving up would stop the stream until somebody noticed,
which is the worse failure.

But some failures never clear: a record larger than the topic's
`max.message.bytes`, an output topic that cannot be auto-created, a broker
ACL. The window is then retried forever and the stream is stuck — and a stuck
stream looks exactly like an idle one from the outside.

So after **5 consecutive failures of the same window** the retry line is
logged at `ERROR` instead of `WARN`:

```
cdc: the stream is stuck: the same window has failed to publish repeatedly
    checkpoint=1745328372192037030 window_min=1745328372192037031
    window_max=1745328372192038112 events=1000 consecutive_failures=5
    error="..." in=30s
```

`window_min`/`window_max` bracket the events that cannot get out, so the
offending record can be found; `consecutive_failures` keeps counting, so the
line distinguishes "stuck since a moment ago" from "stuck all night".

Two things are *not* retried, because no retry could fix them: a window that
is not in ascending timestamp order (see above), and a config the job cannot
start with. Both stop the job with an `ERROR`.
