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

## Configuration

```yaml
cdc:
  topic: ledger.events          # empty or absent: the job is disabled
  batch_size: 1000              # events per window
  poll_interval: 1s             # wait after an empty answer
  partition_key: debit_account_id
```

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
zero.

**A registry gap never costs an event.** An unknown ledger or code is
published with its numeric value in place of the name and a `WARN` naming the
event; amounts of an unknown ledger stay in minor units, since inventing a
scale would misstate the amount. Dropping a financial event because a config
entry is missing would be far worse than an ugly message.

### Headers

`event_type`, `ledger`, `transfer_code`, `debit_account_code`,
`credit_account_code`, `timestamp` — named after the official job's, so a
consumer can route or filter without parsing the body.

## Delivery: at-least-once

A window of events is published and **acknowledged in full before the cursor
moves past it**. Losing an event is unacceptable; duplicating one after a
crash is acceptable, and is the contract.

**Consumers must deduplicate on `timestamp`.** TigerBeetle's event timestamp
is unique and monotonic, so it is a complete deduplication key on its own —
no compound key, no state beyond the highest timestamp already handled.

## Progress: no external state

Like the official job, this one stores its cursor nowhere. On startup it reads
the **last record of every partition** of the output topic and resumes from the
highest `checkpoint` it finds, plus one. An empty or missing topic starts from
zero. `--timestamp-last N` overrides that (including `--timestamp-last 0` to
replay everything).

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
