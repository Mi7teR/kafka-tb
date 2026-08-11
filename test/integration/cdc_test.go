//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	types "github.com/tigerbeetle/tigerbeetle-go"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Mi7teR/kafka-tb/internal/cdc"
	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/Mi7teR/kafka-tb/internal/tbx"
)

// cdcTimeout is how long a scenario waits for the CDC job to publish what it
// is supposed to publish. Generous on purpose, for the same reason
// applyTimeout is: a slow container must not read as a lost event.
const cdcTimeout = 90 * time.Second

// cdcPartitions is how many partitions the CDC topic gets in the tests that
// care about recovery. More than one is the point: the job's records are
// keyed, so a window is spread over several partitions and a crash leaves
// their tails at different timestamps. That is the condition the checkpoint
// field exists for, and a single-partition topic never reaches it.
const cdcPartitions = 3

func cdcLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// startCDC wires the production CDC job exactly the way cmd/kafkatb's startCDC
// does — same Source, same Publisher, same cursor recovery — and runs it until
// the returned stop is called.
//
// from nil means "recover the cursor from the output topic", which is the
// production default and what a restart must go through. A non-nil from is the
// operator's --timestamp-last, and every scenario uses it for its *first*
// start: TigerBeetle's change stream is cluster-wide and these tests share one
// replica, so a job starting from zero would republish every other test's
// transfers into this test's topic.
//
// wrap, when set, sits between the job and the real Kafka producer. It is
// handed the cancel of the job's own context, which is what lets a test end
// the job at a chosen point inside a window (see crashPublisher).
func startCDC(
	t *testing.T, ctx context.Context, cfg *config.Config, tb tbx.Client,
	from *uint64, wrap func(cdc.Publisher, context.CancelFunc) cdc.Publisher,
) (stop func()) {
	t.Helper()
	log := cdcLogger()

	// The same assertion cmd/kafkatb makes: GetChangeEvents is experimental
	// and deliberately absent from tbx.Client.
	src, ok := tb.(cdc.Source)
	require.True(t, ok, "this TigerBeetle client does not expose change events")

	var checkpoint uint64
	if from != nil {
		checkpoint = *from
	} else {
		var err error
		checkpoint, err = cdc.Resume(ctx, cfg.Kafka.Brokers, cfg.CDC.Topic, log)
		require.NoError(t, err)
	}
	t.Logf("cdc: starting from checkpoint %d", checkpoint)

	producer, err := kgo.NewClient(kgo.SeedBrokers(cfg.Kafka.Brokers...))
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(ctx)
	var pub cdc.Publisher = producer
	if wrap != nil {
		pub = wrap(pub, cancel)
	}
	job := cdc.New(cfg.CDC, cfg.Retry, src, pub, model.NewRegistry(cfg), log)

	done := make(chan error, 1)
	go func() { done <- job.Run(runCtx, checkpoint) }()

	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				require.NoError(t, err, "the cdc job stopped with an error")
			case <-time.After(60 * time.Second):
				t.Error("the cdc job did not stop within 60s")
			}
			producer.Close()
		})
	}
	t.Cleanup(stop)
	return stop
}

// resumeCDC recovers a cursor from the output topic the way a restart does,
// without starting a job.
func resumeCDC(t *testing.T, ctx context.Context, cfg *config.Config) uint64 {
	t.Helper()
	ts, err := cdc.Resume(ctx, cfg.Kafka.Brokers, cfg.CDC.Topic, cdcLogger())
	require.NoError(t, err)
	return ts
}

// errCrashed stands in for "this producer's process no longer exists".
var errCrashed = errors.New("cdc test: the job is gone")

// crashPublisher lets a window through as far as the gap the two-step publish
// opens, and no further.
//
// Job.publish writes a window in two ProduceSync calls: every record but the
// one carrying the window's highest timestamp, and then — only once those are
// acknowledged — that closing record alone. crashPublisher forwards the first
// call to the real broker, so those records genuinely land on their real
// partitions, and then behaves like a process that died in the gap: it cancels
// the job's context and answers every later call with an error without ever
// touching Kafka, so the closing record cannot reach the topic by any path.
//
// The topic is then in exactly the state a SIGKILL between the two calls would
// leave it in — a window's records present with unequal tails across
// partitions, and no record claiming a checkpoint that covers them. What this
// cannot reproduce is a kill *inside* the first ProduceSync, which would leave
// only part of that batch published; that would make the tails more ragged
// still, but no weaker a starting point for recovery, since every one of those
// records claims the same pre-window checkpoint either way.
type crashPublisher struct {
	pub  cdc.Publisher
	kill context.CancelFunc
	// crashed closes once the first window's non-closing records are
	// acknowledged and the job has been ended.
	crashed chan struct{}

	mu   sync.Mutex
	dead bool
	once sync.Once
}

func newCrashPublisher(pub cdc.Publisher, kill context.CancelFunc) *crashPublisher {
	return &crashPublisher{pub: pub, kill: kill, crashed: make(chan struct{})}
}

func (c *crashPublisher) ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	c.mu.Lock()
	dead := c.dead
	c.dead = true
	c.mu.Unlock()

	if dead {
		results := make(kgo.ProduceResults, 0, len(rs))
		for _, r := range rs {
			results = append(results, kgo.ProduceResult{Record: r, Err: errCrashed})
		}
		return results
	}

	results := c.pub.ProduceSync(ctx, rs...)
	c.kill()
	c.once.Do(func() { close(c.crashed) })
	return results
}

// --- reading the CDC topic ----------------------------------------------

// cdcEvent is one record off the CDC topic with its body already decoded.
type cdcEvent struct {
	msg        cdc.Message
	rec        *kgo.Record
	timestamp  uint64
	checkpoint uint64
}

// readCDC reads exactly want records off the CDC topic, decodes them, and
// returns them in event-timestamp order. Sorting is not cosmetic: fetch order
// interleaves partitions arbitrarily, so positional assertions on a
// multi-partition topic mean nothing without it.
//
// Decoding goes through the job's own Message type, whose generated
// unmarshaler rejects unknown fields — so a field the format grew without this
// suite noticing fails here rather than passing silently.
func readCDC(t *testing.T, brokers []string, topic string, want int, timeout time.Duration) []cdcEvent {
	t.Helper()
	recs := readTopic(t, brokers, topic, want, timeout)
	out := make([]cdcEvent, len(recs))
	for i, rec := range recs {
		var msg cdc.Message
		require.NoError(t, json.Unmarshal(rec.Value, &msg), "cdc record %d: %s", i, rec.Value)
		ts, err := strconv.ParseUint(msg.Timestamp, 10, 64)
		require.NoError(t, err, "cdc record %d has an unparseable timestamp %q", i, msg.Timestamp)
		cp, err := strconv.ParseUint(msg.Checkpoint, 10, 64)
		require.NoError(t, err, "cdc record %d has an unparseable checkpoint %q", i, msg.Checkpoint)
		out[i] = cdcEvent{msg: msg, rec: rec, timestamp: ts, checkpoint: cp}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].timestamp < out[j].timestamp })
	return out
}

// timestampSet is the deduplication the message format prescribes: a set keyed
// on the event timestamp, applied per event.
//
// It is deliberately not a running maximum. A running maximum looks like it
// follows from the timestamps being unique and monotonic in TigerBeetle, and
// it loses events on this topic for two independent reasons — the topic is
// keyed and multi-partition, so there is no global timestamp order across it;
// and a replayed window delivers events behind ones already handled. A test
// that deduplicated that way would report a gap-free stream as gap-free
// *and* a lossy one as gap-free, so it would be worth nothing.
func timestampSet(events []cdcEvent) map[uint64]cdcEvent {
	seen := make(map[uint64]cdcEvent, len(events))
	for _, ev := range events {
		if _, ok := seen[ev.timestamp]; !ok {
			seen[ev.timestamp] = ev
		}
	}
	return seen
}

// requireNoGaps is the whole delivery contract in one assertion: every event
// TigerBeetle applied is in the topic, deduplicated per timestamp, and nothing
// that was not applied is.
func requireNoGaps(t *testing.T, want []uint64, events []cdcEvent) {
	t.Helper()
	seen := timestampSet(events)
	got := make([]uint64, 0, len(seen))
	for ts := range seen {
		got = append(got, ts)
	}
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	sorted := append([]uint64(nil), want...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	require.Equal(t, sorted, got, "the deduplicated stream is not exactly what TigerBeetle applied")
}

// requirePartitionsUsed guards the premise of the recovery scenarios: a window
// that happened to land on one partition would exercise none of what the
// checkpoint scheme is for. Keys are hashed, so this is a property of the
// account ids a test generated; if it ever fails, the run proved nothing and
// the test says so instead of passing.
func requirePartitionsUsed(t *testing.T, events []cdcEvent, least int) {
	t.Helper()
	parts := map[int32]int{}
	for _, ev := range events {
		parts[ev.rec.Partition]++
	}
	require.GreaterOrEqual(t, len(parts), least,
		"the window landed on %d partition(s) (%v); this run does not exercise ragged tails", len(parts), parts)
}

// --- TigerBeetle helpers --------------------------------------------------

// applyDirect submits transfers straight to TigerBeetle and returns their
// timestamps in order. It bypasses Kafka on purpose where a scenario needs to
// know exactly which events exist and in what order before the CDC job is
// started; where the point is the sink's own output, the sink is used instead.
func applyDirect(t *testing.T, c tbx.Client, transfers ...types.Transfer) []uint64 {
	t.Helper()
	res, err := c.CreateTransfers(transfers)
	require.NoError(t, err)
	require.Len(t, res, len(transfers))
	out := make([]uint64, len(res))
	for i, r := range res {
		require.Equal(t, types.TransferCreated, r.Status, "transfer %d", i)
		require.NotZero(t, r.Timestamp, "transfer %d carries no timestamp", i)
		out[i] = r.Timestamp
	}
	return out
}

// cdcBaseline applies one throwaway transfer and returns its timestamp, to be
// used as the CDC job's starting cursor. Everything this test goes on to do
// sits strictly above it, and everything every other test did sits strictly
// below: TigerBeetle's timestamps are cluster-wide and monotonic, and the
// change stream is cluster-wide too.
func cdcBaseline(t *testing.T, c tbx.Client, debit, credit string) uint64 {
	t.Helper()
	ts := applyDirect(t, c, newTransfer(t, uuid.NewString(), debit, credit, "0.01", 0))
	return ts[0]
}

// transferTimestamps looks up what TigerBeetle actually recorded for ids, in
// the order given. Asserting the CDC topic against these rather than against
// what a test intended to apply is the difference between checking the job and
// checking the test.
func transferTimestamps(t *testing.T, c tbx.Client, ids []string) []uint64 {
	t.Helper()
	byID := map[string]uint64{}
	for _, tr := range lookupTransfers(t, c, ids) {
		byID[model.FormatID(tr.ID)] = tr.Timestamp
	}
	out := make([]uint64, len(ids))
	for i, id := range ids {
		ts, ok := byID[id]
		require.True(t, ok, "transfer %s was never applied", id)
		out[i] = ts
	}
	return out
}

// accountTimestamp is an account's own timestamp, which every event carrying
// that account must repeat.
func accountTimestamp(t *testing.T, c tbx.Client, id string) uint64 {
	t.Helper()
	u, err := model.ParseID(id)
	require.NoError(t, err)
	accounts, err := c.LookupAccounts([]types.Uint128{u})
	require.NoError(t, err)
	require.Len(t, accounts, 1, "account %s not found", id)
	return accounts[0].Timestamp
}

// --- 1. the events match what was applied ---------------------------------

// The bodies on the topic are checked field by field against what TigerBeetle
// actually holds: ids, amounts, the ledger and code names from the registries,
// the event type, and both account snapshots as of the event. A test that only
// counted records would pass with every amount wrong.
func TestCDCPublishesWhatTheSinkApplied(t *testing.T) {
	brokers := startRedpanda(t)
	cfg := testConfig(t, brokers, startTigerBeetle(t))
	createTopics(t, cfg)
	tb := newTBClient(t, cfg)

	ids := createAccounts(t, tb, types.AccountFlags{}, types.AccountFlags{}, types.AccountFlags{})
	funder, debit, credit := ids[0], ids[1], ids[2]
	// Funded so the debit account's own snapshot carries a credits_posted
	// worth asserting on, rather than three zeroes on one side.
	applyDirect(t, tb, newTransfer(t, uuid.NewString(), funder, debit, "100.00", 0))
	baseline := cdcBaseline(t, tb, funder, debit)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// The job is started before the transfers exist, so this is the live
	// path — poll, find new events, publish — and not a replay of history.
	startCDC(t, ctx, cfg, tb, &baseline, nil)

	amounts := []string{"10.00", "5.50", "1.25"}
	tids := make([]string, len(amounts))
	payloads := make([]string, len(amounts))
	for i, amt := range amounts {
		tids[i] = uuid.NewString()
		payloads[i] = transferJSON(tids[i], debit, credit, amt)
	}
	produce(t, brokers, cfg.Kafka.Topics[0].Name, payloads)
	runSink(t, ctx, cfg, tb)
	requireBalance(t, tb, credit, "16.75", applyTimeout)

	events := readCDC(t, brokers, cfg.CDC.Topic, len(amounts), cdcTimeout)
	requireNoGaps(t, transferTimestamps(t, tb, tids), events)

	debitTS := strconv.FormatUint(accountTimestamp(t, tb, debit), 10)
	creditTS := strconv.FormatUint(accountTimestamp(t, tb, credit), 10)

	// Cumulative, because each snapshot is the state as of its own event.
	wantDebits := []string{"10.00", "15.50", "16.75"}
	for i, ev := range events {
		msg := ev.msg
		where := "event " + strconv.Itoa(i)

		require.Equal(t, "single_phase", msg.Type, where)
		require.Equal(t, ledgerName, msg.Ledger, where)
		require.LessOrEqual(t, ev.checkpoint, ev.timestamp,
			"%s claims a checkpoint above its own timestamp", where)

		require.Equal(t, tids[i], msg.Transfer.ID, where)
		require.Equal(t, amounts[i], msg.Transfer.Amount, where)
		require.Equal(t, codeName, msg.Transfer.Code, where)
		require.Equal(t, []string{}, msg.Transfer.Flags, where)
		require.Equal(t, msg.Timestamp, msg.Transfer.Timestamp,
			"%s: a single-phase event and its transfer share one timestamp", where)
		require.Empty(t, msg.Transfer.PendingID, where)
		require.Empty(t, msg.Transfer.Timeout, where)

		require.Equal(t, debit, msg.DebitAccount.ID, where)
		require.Equal(t, wantDebits[i], msg.DebitAccount.DebitsPosted, where)
		require.Equal(t, "0.00", msg.DebitAccount.DebitsPending, where)
		require.Equal(t, "100.01", msg.DebitAccount.CreditsPosted,
			"%s: the funding and the baseline transfer, and nothing else", where)
		require.Equal(t, "0.00", msg.DebitAccount.CreditsPending, where)
		require.Equal(t, codeName, msg.DebitAccount.Code, where)
		require.Equal(t, []string{"history"}, msg.DebitAccount.Flags, where)
		require.Equal(t, debitTS, msg.DebitAccount.Timestamp, where)

		require.Equal(t, credit, msg.CreditAccount.ID, where)
		require.Equal(t, wantDebits[i], msg.CreditAccount.CreditsPosted, where)
		require.Equal(t, "0.00", msg.CreditAccount.CreditsPending, where)
		require.Equal(t, "0.00", msg.CreditAccount.DebitsPosted, where)
		require.Equal(t, "0.00", msg.CreditAccount.DebitsPending, where)
		require.Equal(t, creditTS, msg.CreditAccount.Timestamp, where)

		// The headers exist so a consumer can route without parsing the body,
		// which is only true if they agree with the body.
		require.Equal(t, msg.Type, header(t, ev.rec, cdc.HeaderEventType), where)
		require.Equal(t, msg.Ledger, header(t, ev.rec, cdc.HeaderLedger), where)
		require.Equal(t, msg.Transfer.Code, header(t, ev.rec, cdc.HeaderTransferCode), where)
		require.Equal(t, msg.Timestamp, header(t, ev.rec, cdc.HeaderTimestamp), where)
		// partition_key is debit_account_id here, and the key is what gives a
		// consumer per-account ordering.
		require.Equal(t, debit, string(ev.rec.Key), where)
	}

	// The last record of a completed window is its closing record, and a
	// closing record claims its own timestamp: that is the claim a restart
	// resumes from.
	last := events[len(events)-1]
	require.Equal(t, last.timestamp, last.checkpoint,
		"the final record must claim its own timestamp as the checkpoint")
	require.Equal(t, last.timestamp, resumeCDC(t, ctx, cfg),
		"a restart would resume from somewhere other than the last completed window")
}

// --- 2. a clean restart loses nothing and replays nothing ------------------

// The job keeps its cursor nowhere but the output topic, so a restart has to
// find it by reading the tail of every partition. Events applied while the job
// was down must appear, and events published before it went down must not
// appear twice.
func TestCDCSurvivesACleanRestart(t *testing.T) {
	const (
		before = 12
		after  = 9
	)

	brokers := startRedpanda(t)
	cfg := testConfig(t, brokers, startTigerBeetle(t))
	createTopicsWithCDCPartitions(t, cfg, cdcPartitions)
	tb := newTBClient(t, cfg)

	// Several debit accounts so the records spread over the partitions:
	// cdc.partition_key is debit_account_id.
	accounts := createAccounts(t, tb, make([]types.AccountFlags, 7)...)
	debits, credit := accounts[:6], accounts[6]
	baseline := cdcBaseline(t, tb, debits[0], credit)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	ids := make([]string, 0, before+after)
	apply := func(n int) {
		batch := make([]types.Transfer, 0, n)
		for i := 0; i < n; i++ {
			id := uuid.NewString()
			batch = append(batch, newTransfer(t, id, debits[len(ids)%len(debits)], credit, "1.00", 0))
			ids = append(ids, id)
		}
		applyDirect(t, tb, batch...)
	}

	stopFirst := startCDC(t, ctx, cfg, tb, &baseline, nil)
	apply(before)
	first := readCDC(t, brokers, cfg.CDC.Topic, before, cdcTimeout)
	requireNoGaps(t, transferTimestamps(t, tb, ids), first)
	requirePartitionsUsed(t, first, 2)
	stopFirst()

	// The cursor the restart will pick up, recovered from the topic itself:
	// the last completed window's closing record, and nothing lower.
	resumed := resumeCDC(t, ctx, cfg)
	require.Equal(t, first[len(first)-1].timestamp, resumed,
		"recovery did not land on the highest checkpoint the topic holds")

	apply(after)

	// from nil: the restart goes through cdc.Resume, exactly like `kafkatb cdc`.
	startCDC(t, ctx, cfg, tb, nil, nil)

	all := readCDC(t, brokers, cfg.CDC.Topic, before+after, cdcTimeout)
	requireNoGaps(t, transferTimestamps(t, tb, ids), all)
	// readCDC already required exactly before+after records, so this says the
	// same thing from the other side: a clean restart republished nothing.
	require.Len(t, timestampSet(all), before+after,
		"a clean restart must not replay records the previous run already published")
	requirePartitionsUsed(t, all, 2)
}

// --- 3. a crash mid-window replays instead of skipping ---------------------

// This is the scenario the whole checkpoint design exists for. The job is
// killed in the gap between a window's non-closing records and its closing
// one, which is precisely when the topic's partition tails end at different
// timestamps and the highest event timestamp present is *not* a point the
// stream is complete up to.
//
// A cursor recovered from event timestamps would resume above the missing
// event and leave a permanent gap. The checkpoint field is what prevents that,
// and what this asserts: duplicates after the restart are fine, a gap is not.
func TestCDCReplaysAWindowInterruptedMidPublish(t *testing.T) {
	const count = 8

	brokers := startRedpanda(t)
	cfg := testConfig(t, brokers, startTigerBeetle(t))
	createTopicsWithCDCPartitions(t, cfg, cdcPartitions)
	tb := newTBClient(t, cfg)

	accounts := createAccounts(t, tb, make([]types.AccountFlags, count+1)...)
	debits, credit := accounts[:count], accounts[count]
	baseline := cdcBaseline(t, tb, debits[0], credit)

	// Every event exists before the job starts, and cdc.batch_size is well
	// above count, so the job's first window is exactly these count events.
	// That is what makes "killed mid-window" a statement about a known window.
	ids := make([]string, count)
	batch := make([]types.Transfer, count)
	for i := range batch {
		ids[i] = uuid.NewString()
		batch[i] = newTransfer(t, ids[i], debits[i], credit, "1.00", 0)
	}
	applied := applyDirect(t, tb, batch...)
	require.Greater(t, cfg.CDC.BatchSize, count, "the window must fit in one batch")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	var crash *crashPublisher
	stopCrashed := startCDC(t, ctx, cfg, tb, &baseline, func(
		pub cdc.Publisher, kill context.CancelFunc,
	) cdc.Publisher {
		crash = newCrashPublisher(pub, kill)
		return crash
	})
	select {
	case <-crash.crashed:
	case <-time.After(cdcTimeout):
		t.Fatal("the job never published a window's non-closing records")
	}
	stopCrashed()

	// What the crash left behind: every event of the window but the one
	// carrying its highest timestamp.
	partial := readCDC(t, brokers, cfg.CDC.Topic, count-1, cdcTimeout)
	require.Equal(t, applied[:count-1], timestamps(partial),
		"the interrupted window left something other than its first count-1 events")
	for i, ev := range partial {
		require.Equal(t, baseline, ev.checkpoint,
			"record %d claims completeness up to %d, but the window it belongs to never finished",
			i, ev.checkpoint)
	}
	requirePartitionsUsed(t, partial, 2)

	// The highest event timestamp in the topic is now above events that are
	// missing from it. A cursor taken from event timestamps would resume from
	// there and lose them; the recovered cursor is the checkpoint instead.
	highestEvent := partial[len(partial)-1].timestamp
	resumed := resumeCDC(t, ctx, cfg)
	require.Equal(t, baseline, resumed,
		"recovery resumed from %d, above events the topic is missing", resumed)
	require.Less(t, resumed, highestEvent,
		"this run did not reproduce ragged tails: the recovered cursor is not below "+
			"the highest event timestamp present")

	startCDC(t, ctx, cfg, tb, nil, nil)

	// The whole window is republished: count-1 duplicates plus the count
	// events themselves.
	all := readCDC(t, brokers, cfg.CDC.Topic, 2*count-1, cdcTimeout)
	requireNoGaps(t, applied, all)
	require.Len(t, all, len(timestampSet(all))+count-1,
		"the replay must be duplicates of the interrupted window, nothing else")

	// And the window now closes: some record claims the highest timestamp.
	last := all[len(all)-1]
	require.Equal(t, applied[count-1], last.timestamp)
	require.Equal(t, last.timestamp, last.checkpoint,
		"the replayed window's closing record must claim its own timestamp")
	require.Equal(t, last.timestamp, resumeCDC(t, ctx, cfg))
}

func timestamps(events []cdcEvent) []uint64 {
	out := make([]uint64, len(events))
	for i, ev := range events {
		out[i] = ev.timestamp
	}
	return out
}

// --- 4. two-phase flows ----------------------------------------------------

// A pending transfer and the post that resolves it are two separate events
// with two separate types, and the balances they carry move from pending to
// posted. Voiding is the other resolution; both are driven through the sink,
// which is how a two-phase flow actually reaches TigerBeetle here.
func TestCDCEmitsTwoPhaseEvents(t *testing.T) {
	brokers := startRedpanda(t)
	cfg := testConfig(t, brokers, startTigerBeetle(t))
	createTopics(t, cfg)
	tb := newTBClient(t, cfg)

	debit, credit := seedAccounts(t, tb)
	baseline := cdcBaseline(t, tb, debit, credit)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	startCDC(t, ctx, cfg, tb, &baseline, nil)

	pendingPost, post := uuid.NewString(), uuid.NewString()
	pendingVoid, void := uuid.NewString(), uuid.NewString()
	produce(t, brokers, cfg.Kafka.Topics[0].Name, []string{
		transfersJSON(transferSpec{
			ID: pendingPost, Debit: debit, Credit: credit, Amount: "7.25",
			Flags: []string{"pending"}, Timeout: "1m",
		}),
		transfersJSON(transferSpec{
			ID: post, Debit: debit, Credit: credit, Amount: "7.25",
			Flags: []string{"post_pending_transfer"}, PendingID: pendingPost,
		}),
		transfersJSON(transferSpec{
			ID: pendingVoid, Debit: debit, Credit: credit, Amount: "3.00",
			Flags: []string{"pending"}, Timeout: "1m",
		}),
		transfersJSON(transferSpec{
			ID: void, Debit: debit, Credit: credit, Amount: "3.00",
			Flags: []string{"void_pending_transfer"}, PendingID: pendingVoid,
		}),
	})
	runSink(t, ctx, cfg, tb)
	requireBalance(t, tb, credit, "7.26", applyTimeout) // the baseline transfer plus the posted one
	dlqRecords(t, brokers, cfg.Kafka.DLQTopic, 0, applyTimeout)

	ids := []string{pendingPost, post, pendingVoid, void}
	events := readCDC(t, brokers, cfg.CDC.Topic, len(ids), cdcTimeout)
	requireNoGaps(t, transferTimestamps(t, tb, ids), events)

	require.Equal(t,
		[]string{"two_phase_pending", "two_phase_posted", "two_phase_pending", "two_phase_voided"},
		eventTypes(events))

	// The pending leg holds the amount in debits_pending and moves nothing.
	p := events[0].msg
	require.Equal(t, pendingPost, p.Transfer.ID)
	require.Equal(t, "7.25", p.Transfer.Amount)
	require.Equal(t, []string{"pending"}, p.Transfer.Flags)
	require.Equal(t, "1m0s", p.Transfer.Timeout)
	require.Empty(t, p.Transfer.PendingID)
	require.Equal(t, "7.25", p.DebitAccount.DebitsPending)
	require.Equal(t, "0.01", p.DebitAccount.DebitsPosted, "only the baseline transfer has posted")
	require.Equal(t, "7.25", p.CreditAccount.CreditsPending)
	require.Equal(t, "0.01", p.CreditAccount.CreditsPosted)

	// The post names the pending leg and moves the amount from pending to
	// posted on both sides.
	q := events[1].msg
	require.Equal(t, post, q.Transfer.ID)
	require.Equal(t, pendingPost, q.Transfer.PendingID)
	require.Equal(t, "7.25", q.Transfer.Amount)
	require.Equal(t, []string{"post_pending_transfer"}, q.Transfer.Flags)
	require.Equal(t, "0.00", q.DebitAccount.DebitsPending)
	require.Equal(t, "7.26", q.DebitAccount.DebitsPosted)
	require.Equal(t, "0.00", q.CreditAccount.CreditsPending)
	require.Equal(t, "7.26", q.CreditAccount.CreditsPosted)

	// The void releases the pending amount and posts nothing.
	v := events[3].msg
	require.Equal(t, void, v.Transfer.ID)
	require.Equal(t, pendingVoid, v.Transfer.PendingID)
	require.Equal(t, "3.00", v.Transfer.Amount)
	require.Equal(t, []string{"void_pending_transfer"}, v.Transfer.Flags)
	require.Equal(t, "0.00", v.DebitAccount.DebitsPending)
	require.Equal(t, "7.26", v.DebitAccount.DebitsPosted)
	require.Equal(t, "0.00", v.CreditAccount.CreditsPending)
	require.Equal(t, "7.26", v.CreditAccount.CreditsPosted)
}

// The third resolution of a pending transfer is the one nobody submits: it
// expires on its own, and TigerBeetle emits the event with no message behind
// it. The job has to publish it like any other.
func TestCDCEmitsAnExpiredTwoPhaseEvent(t *testing.T) {
	brokers := startRedpanda(t)
	cfg := testConfig(t, brokers, startTigerBeetle(t))
	createTopics(t, cfg)
	tb := newTBClient(t, cfg)

	debit, credit := seedAccounts(t, tb)
	baseline := cdcBaseline(t, tb, debit, credit)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	startCDC(t, ctx, cfg, tb, &baseline, nil)

	pending := uuid.NewString()
	produce(t, brokers, cfg.Kafka.Topics[0].Name, []string{
		transfersJSON(transferSpec{
			ID: pending, Debit: debit, Credit: credit, Amount: "2.00",
			// One second is the shortest timeout TigerBeetle's second-resolution
			// timeout field can express.
			Flags: []string{"pending"}, Timeout: "1s",
		}),
	})
	runSink(t, ctx, cfg, tb)

	// The pending event, then the expiry TigerBeetle raises by itself.
	events := readCDC(t, brokers, cfg.CDC.Topic, 2, cdcTimeout)
	require.Equal(t, []string{"two_phase_pending", "two_phase_expired"}, eventTypes(events))

	expired := events[1].msg
	require.Equal(t, pending, expired.Transfer.ID, "the expiry names the pending transfer")
	require.Equal(t, "2.00", expired.Transfer.Amount)
	require.Equal(t, "0.00", expired.DebitAccount.DebitsPending, "expiry releases the pending amount")
	require.Equal(t, "0.01", expired.DebitAccount.DebitsPosted, "expiry posts nothing")
	require.Equal(t, "0.00", expired.CreditAccount.CreditsPending)
	require.Equal(t, "0.01", expired.CreditAccount.CreditsPosted)
	require.Greater(t, events[1].timestamp, events[0].timestamp,
		"the expiry is its own event, above the pending transfer it resolves")
	require.Equal(t, expired.Type, header(t, events[1].rec, cdc.HeaderEventType))
}

func eventTypes(events []cdcEvent) []string {
	out := make([]string, len(events))
	for i, ev := range events {
		out[i] = ev.msg.Type
	}
	return out
}
