//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	types "github.com/tigerbeetle/tigerbeetle-go"

	"github.com/Mi7teR/kafka-tb/internal/emit"
	"github.com/Mi7teR/kafka-tb/internal/tbx"
)

// applyTimeout is how long a scenario waits for the sink to drain its topic.
// Generous on purpose: containers and the first client handshake are slow, and
// a tight bound would turn a slow machine into a false failure.
const applyTimeout = 90 * time.Second

func TestHarnessBoots(t *testing.T) {
	brokers := startRedpanda(t)
	addr := startTigerBeetle(t)
	require.NotEmpty(t, brokers[0])
	require.NotEmpty(t, addr)

	cfg := testConfig(t, brokers, addr)
	createTopics(t, cfg)
	tb := newTBClient(t, cfg)
	require.NoError(t, tb.Nop())
}

// TestTigerBeetleResultShape answers the question tbx/outcome.go is built on:
// when every event in a batch succeeds, does TigerBeetle return a result per
// event, or an empty array? MapTransferResults requires len(results) ==
// batchSize; if the array were empty on total success, every happy path would
// fail with ErrResultCountMismatch.
func TestTigerBeetleResultShape(t *testing.T) {
	brokers := startRedpanda(t)
	cfg := testConfig(t, brokers, startTigerBeetle(t))
	tb := newTBClient(t, cfg)

	debit, credit := seedAccounts(t, tb)

	transfers := []types.Transfer{
		newTransfer(t, uuid.NewString(), debit, credit, "1.00", 0),
		newTransfer(t, uuid.NewString(), debit, credit, "2.00", 0),
		newTransfer(t, uuid.NewString(), debit, credit, "3.00", 0),
	}
	res, err := tb.CreateTransfers(transfers)
	require.NoError(t, err)
	t.Logf("create_transfers: %d events sent, %d results returned: %+v", len(transfers), len(res), res)
	require.Len(t, res, len(transfers),
		"create_transfers returned a non-dense result array when every event succeeded")
	for i, r := range res {
		require.Equal(t, types.TransferCreated, r.Status, "result %d", i)
		require.NotZero(t, r.Timestamp, "result %d carries no timestamp", i)
	}

	// The same batch again: every event is a duplicate, so every result must
	// be TransferExists — the property idempotent replay rests on.
	again, err := tb.CreateTransfers(transfers)
	require.NoError(t, err)
	t.Logf("create_transfers replay: %d results returned: %+v", len(again), again)
	require.Len(t, again, len(transfers))
	for i, r := range again {
		require.Equal(t, types.TransferExists, r.Status, "replay result %d", i)
	}
	require.Equal(t, "6.00", balanceOf(t, tb, credit), "replay must not move money")
}

// 1. Valid messages are applied and the balances match.
func TestSinkAppliesTransfers(t *testing.T) {
	brokers := startRedpanda(t)
	cfg := testConfig(t, brokers, startTigerBeetle(t))
	createTopics(t, cfg)
	tb := newTBClient(t, cfg)

	debit, credit := seedAccounts(t, tb)
	topic := cfg.Kafka.Topics[0].Name
	id1, id2 := uuid.NewString(), uuid.NewString()
	produce(t, brokers, topic, []string{
		transferJSON(id1, debit, credit, "10.00"),
		transferJSON(id2, debit, credit, "5.50"),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	runSink(t, ctx, cfg, tb)

	requireBalance(t, tb, credit, "15.50", applyTimeout)
	require.Equal(t, "-15.50", balanceOf(t, tb, debit))

	// The two input records land on offsets 0 and 1 of a single-partition
	// topic, so results are produced and read back in that same order.
	results := readTopic(t, brokers, cfg.Kafka.ResultsTopic, 2, applyTimeout)
	for i, want := range []string{id1, id2} {
		var msg emit.ResultsMessage
		require.NoError(t, json.Unmarshal(results[i].Value, &msg), "results record %d", i)
		require.Equal(t, topic, msg.Source.Topic, "results record %d", i)
		require.Equal(t, int32(0), msg.Source.Partition, "results record %d", i)
		require.Equal(t, int64(i), msg.Source.Offset, "results record %d", i)
		require.Len(t, msg.Results, 1, "results record %d", i)
		require.Equal(t, 0, msg.Results[0].Index, "results record %d", i)
		require.Equal(t, want, msg.Results[0].ID, "results record %d", i)
		require.Equal(t, string(tbx.StatusOK), msg.Results[0].Status, "results record %d", i)
	}
	dlqRecords(t, brokers, cfg.Kafka.DLQTopic, 0, applyTimeout)
}

// 2. Idempotent replay: the same topic consumed twice from the beginning
// leaves balances unchanged, and the second pass lands entirely as "exists".
func TestSinkIsIdempotentOnReplay(t *testing.T) {
	brokers := startRedpanda(t)
	cfg := testConfig(t, brokers, startTigerBeetle(t))
	createTopics(t, cfg)
	tb := newTBClient(t, cfg)

	debit, credit := seedAccounts(t, tb)
	ids := []string{uuid.NewString(), uuid.NewString()}
	topic := cfg.Kafka.Topics[0].Name
	produce(t, brokers, topic, []string{
		transferJSON(ids[0], debit, credit, "10.00"),
		transferJSON(ids[1], debit, credit, "5.50"),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	stopFirst := runSink(t, ctx, cfg, tb)
	requireBalance(t, tb, credit, "15.50", applyTimeout)
	readTopic(t, brokers, cfg.Kafka.ResultsTopic, 2, applyTimeout)
	stopFirst()

	applied := lookupTransfers(t, tb, ids)
	require.Len(t, applied, 2)
	stamps := []uint64{applied[0].Timestamp, applied[1].Timestamp}

	// A fresh group reads the very same records from offset 0 again.
	runSink(t, ctx, withGroup(cfg, "replay"), tb)

	// Four results messages prove the second pass really submitted both
	// records rather than skipping them on a committed offset.
	readTopic(t, brokers, cfg.Kafka.ResultsTopic, 4, applyTimeout)
	requireBalance(t, tb, credit, "15.50", applyTimeout)
	require.Equal(t, "-15.50", balanceOf(t, tb, debit))
	dlqRecords(t, brokers, cfg.Kafka.DLQTopic, 0, applyTimeout)

	// Unchanged timestamps mean no transfer was created a second time: the
	// replay landed as "exists", which is also what a direct duplicate
	// submission reports.
	after := lookupTransfers(t, tb, ids)
	require.Len(t, after, 2)
	require.Equal(t, stamps, []uint64{after[0].Timestamp, after[1].Timestamp})

	dup, err := tb.CreateTransfers([]types.Transfer{
		newTransfer(t, ids[0], debit, credit, "10.00", 0),
	})
	require.NoError(t, err)
	require.Len(t, dup, 1)
	require.Equal(t, types.TransferExists, dup[0].Status)
}

// 3. Garbage interleaved with valid messages: the bad ones land in the DLQ,
// the good ones are applied, and the consumer does not stall behind them.
func TestSinkQuarantinesGarbageAndKeepsGoing(t *testing.T) {
	brokers := startRedpanda(t)
	cfg := testConfig(t, brokers, startTigerBeetle(t))
	createTopics(t, cfg)
	tb := newTBClient(t, cfg)

	debit, credit := seedAccounts(t, tb)
	topic := cfg.Kafka.Topics[0].Name
	produce(t, brokers, topic, []string{
		transferJSON(uuid.NewString(), debit, credit, "10.00"),
		"this is not json at all",
		transferJSON(uuid.NewString(), debit, credit, "5.50"),
		`{"operation":"transmogrify","transfers":[]}`,
		`{"operation":"create_transfers","transfers":[{"id":"not-a-uuid",` +
			`"debit_account_id":"` + debit + `","credit_account_id":"` + credit + `",` +
			`"amount":"1.00","ledger":"USD","code":"payment"}]}`,
		transferJSON(uuid.NewString(), debit, credit, "1.00"),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	runSink(t, ctx, cfg, tb)

	// 16.50 can only be reached by applying the transfer that sits *after*
	// the garbage, so this is the "does not stall" assertion.
	requireBalance(t, tb, credit, "16.50", applyTimeout)

	dlq := dlqRecords(t, brokers, cfg.Kafka.DLQTopic, 3, applyTimeout)
	for _, rec := range dlq {
		require.Equal(t, string(emit.ReasonPoison), header(t, rec, emit.HeaderReason))
		require.Equal(t, topic, header(t, rec, emit.HeaderSrcTopic))
	}
	require.Equal(t, "this is not json at all", string(dlq[0].Value),
		"the DLQ must carry the original bytes so the record can be replayed")
}

// 4. Business rejection: a debit beyond the balance produces a DLQ entry
// carrying exceeds_credits, and the stream continues.
func TestSinkSendsRejectToDLQ(t *testing.T) {
	brokers := startRedpanda(t)
	cfg := testConfig(t, brokers, startTigerBeetle(t))
	createTopics(t, cfg)
	tb := newTBClient(t, cfg)

	ids := createAccounts(t, tb,
		types.AccountFlags{}, // funder
		types.AccountFlags{DebitsMustNotExceedCredits: true}, // debit, capped
		types.AccountFlags{}, // credit
	)
	funder, debit, credit := ids[0], ids[1], ids[2]
	fund(t, tb, funder, debit, "100.00")

	topic := cfg.Kafka.Topics[0].Name
	rejectPayload := transferJSON(uuid.NewString(), debit, credit, "1000.00")
	produce(t, brokers, topic, []string{
		rejectPayload,
		transferJSON(uuid.NewString(), debit, credit, "25.00"),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	runSink(t, ctx, cfg, tb)

	requireBalance(t, tb, credit, "25.00", applyTimeout)
	require.Equal(t, "75.00", balanceOf(t, tb, debit))

	dlq := dlqRecords(t, brokers, cfg.Kafka.DLQTopic, 1, applyTimeout)
	require.Equal(t, string(emit.ReasonReject), header(t, dlq[0], emit.HeaderReason))
	require.Equal(t, "exceeds_credits", header(t, dlq[0], emit.HeaderError))
	require.Equal(t, rejectPayload, string(dlq[0].Value),
		"the DLQ must carry the original bytes so the record can be replayed")
	require.Equal(t, "0", header(t, dlq[0], emit.HeaderSrcPartition))
	require.Equal(t, "0", header(t, dlq[0], emit.HeaderSrcOffset))
}

// 5. DLQ replay is safe: republishing a dead-lettered record leaves balances
// unchanged, whether it was dead-lettered for a rejection or already applied.
func TestDLQReplayIsSafe(t *testing.T) {
	brokers := startRedpanda(t)
	cfg := testConfig(t, brokers, startTigerBeetle(t))
	createTopics(t, cfg)
	tb := newTBClient(t, cfg)

	ids := createAccounts(t, tb,
		types.AccountFlags{},
		types.AccountFlags{DebitsMustNotExceedCredits: true},
		types.AccountFlags{},
	)
	funder, debit, credit := ids[0], ids[1], ids[2]
	fund(t, tb, funder, debit, "100.00")

	topic := cfg.Kafka.Topics[0].Name
	good := transferJSON(uuid.NewString(), debit, credit, "10.00")
	bad := transferJSON(uuid.NewString(), debit, credit, "1000.00")
	produce(t, brokers, topic, []string{good, bad})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	runSink(t, ctx, cfg, tb)

	requireBalance(t, tb, credit, "10.00", applyTimeout)
	dlq := dlqRecords(t, brokers, cfg.Kafka.DLQTopic, 1, applyTimeout)
	require.Equal(t, bad, string(dlq[0].Value))

	// Replay both: the dead-lettered record verbatim, and one that already
	// succeeded. Neither may move money.
	produce(t, brokers, topic, []string{good, string(dlq[0].Value)})

	replayed := dlqRecords(t, brokers, cfg.Kafka.DLQTopic, 2, applyTimeout)
	t.Logf("replayed reject dead-lettered as %q", header(t, replayed[1], emit.HeaderError))
	require.Equal(t, string(emit.ReasonReject), header(t, replayed[1], emit.HeaderReason))

	requireBalance(t, tb, credit, "10.00", applyTimeout)
	require.Equal(t, "90.00", balanceOf(t, tb, debit))
}

// 6. Restart mid-stream: kill and restart the sink, then assert no loss and no
// double-spend.
func TestSinkSurvivesRestart(t *testing.T) {
	const count = 300

	brokers := startRedpanda(t)
	cfg := testConfig(t, brokers, startTigerBeetle(t))
	// A small TigerBeetle batch size makes the sink advance in visible steps,
	// so the restart really lands in the middle of the stream instead of
	// after it.
	cfg.Batcher.MaxBatchSize = 8
	createTopics(t, cfg)
	tb := newTBClient(t, cfg)

	debit, credit := seedAccounts(t, tb)
	ids := make([]string, count)
	payloads := make([]string, count)
	for i := range ids {
		ids[i] = uuid.NewString()
		payloads[i] = transferJSON(ids[i], debit, credit, "1.00")
	}
	produce(t, brokers, cfg.Kafka.Topics[0].Name, payloads)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	stopFirst := runSink(t, ctx, cfg, tb)
	waitBalanceAtLeast(t, tb, credit, 300, applyTimeout) // 3.00, i.e. 3 records
	stopFirst()

	atRestart := balanceMinor(t, tb, credit)
	t.Logf("sink stopped with %s of %d.00 applied", formatSigned(atRestart), count)
	require.Less(t, atRestart.Int64(), int64(count*100),
		"the sink drained the topic before the restart; this run proves nothing about mid-stream restarts")

	runSink(t, ctx, cfg, tb)
	requireBalance(t, tb, credit, "300.00", 3*time.Minute)

	// No loss: every id is present. No double-spend: the balance above is
	// exactly count * 1.00, and a re-applied transfer would have overshot it.
	require.Len(t, lookupTransfers(t, tb, ids), count)
	dlqRecords(t, brokers, cfg.Kafka.DLQTopic, 0, applyTimeout)
}

// 6b. Restart after an abrupt stop: same no-loss, no-double-spend property as
// scenario 6, but the first sink is not given a chance to run its graceful
// drain (no ctx-cancellation final commit, no revoke commit — see
// runSinkAbrupt for exactly how far that goes with this harness). A
// regression that only commits offsets ahead of what TigerBeetle actually
// durably applied would show up here as a gap the graceful case cannot
// exercise, because the graceful case flushes and commits everything applied
// right up to the stop.
func TestSinkSurvivesAbruptRestart(t *testing.T) {
	const count = 300

	brokers := startRedpanda(t)
	cfg := testConfig(t, brokers, startTigerBeetle(t))
	// A small TigerBeetle batch size makes the sink advance in visible steps,
	// so the kill really lands in the middle of the stream instead of after
	// it.
	cfg.Batcher.MaxBatchSize = 8
	createTopics(t, cfg)
	tb := newTBClient(t, cfg)

	debit, credit := seedAccounts(t, tb)
	ids := make([]string, count)
	payloads := make([]string, count)
	for i := range ids {
		ids[i] = uuid.NewString()
		payloads[i] = transferJSON(ids[i], debit, credit, "1.00")
	}
	produce(t, brokers, cfg.Kafka.Topics[0].Name, payloads)

	kill := runSinkAbrupt(t, cfg, tb)
	waitBalanceAtLeast(t, tb, credit, 300, applyTimeout) // 3.00, i.e. 3 records

	atRestart := balanceMinor(t, tb, credit)
	t.Logf("sink killed abruptly with %s of %d.00 applied", formatSigned(atRestart), count)
	require.Less(t, atRestart.Int64(), int64(count*100),
		"the sink drained the topic before the kill; this run proves nothing about mid-stream restarts")
	kill()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	runSink(t, ctx, cfg, tb)
	requireBalance(t, tb, credit, "300.00", 3*time.Minute)

	// No loss: every id is present. No double-spend: the balance above is
	// exactly count * 1.00, and a re-applied transfer would have overshot it.
	require.Len(t, lookupTransfers(t, tb, ids), count)
	dlqRecords(t, brokers, cfg.Kafka.DLQTopic, 0, applyTimeout)
}

// 7. Linked chain atomicity: a two-transfer chain whose second leg has
// insufficient funds applies neither leg.
func TestLinkedChainIsAtomic(t *testing.T) {
	brokers := startRedpanda(t)
	cfg := testConfig(t, brokers, startTigerBeetle(t))
	createTopics(t, cfg)
	tb := newTBClient(t, cfg)

	ids := createAccounts(t, tb,
		types.AccountFlags{},
		types.AccountFlags{DebitsMustNotExceedCredits: true},
		types.AccountFlags{},
		types.AccountFlags{},
	)
	funder, debit, first, second := ids[0], ids[1], ids[2], ids[3]
	fund(t, tb, funder, debit, "50.00")

	legs := []string{uuid.NewString(), uuid.NewString()}
	topic := cfg.Kafka.Topics[0].Name
	chainPayload := transfersJSON(
		transferSpec{ID: legs[0], Debit: debit, Credit: first, Amount: "10.00", Flags: []string{"linked"}},
		transferSpec{ID: legs[1], Debit: debit, Credit: second, Amount: "1000.00"},
	)
	produce(t, brokers, topic, []string{
		chainPayload,
		// A follow-up that must still be applied: a failed chain does not
		// poison the stream.
		transferJSON(uuid.NewString(), debit, first, "1.00"),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	runSink(t, ctx, cfg, tb)

	// Only the follow-up transfer ever lands.
	requireBalance(t, tb, first, "1.00", applyTimeout)
	require.Equal(t, "0.00", balanceOf(t, tb, second))
	require.Equal(t, "49.00", balanceOf(t, tb, debit))
	require.Empty(t, lookupTransfers(t, tb, legs), "no leg of a failed chain may be applied")

	// One DLQ record per rejected event, both carrying the same original
	// message: the failing leg says why, the linked leg says it was rolled
	// back with the chain.
	dlq := dlqRecords(t, brokers, cfg.Kafka.DLQTopic, 2, applyTimeout)
	errs := []string{header(t, dlq[0], emit.HeaderError), header(t, dlq[1], emit.HeaderError)}
	require.ElementsMatch(t, []string{"linked_event_failed", "exceeds_credits"}, errs)
	for _, rec := range dlq {
		require.Equal(t, string(emit.ReasonReject), header(t, rec, emit.HeaderReason))
		require.Equal(t, chainPayload, string(rec.Value),
			"the DLQ must carry the original bytes so the record can be replayed")
		require.Equal(t, "0", header(t, rec, emit.HeaderSrcPartition))
		require.Equal(t, "0", header(t, rec, emit.HeaderSrcOffset))
	}
}
