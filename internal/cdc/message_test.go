package cdc

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"math/big"
	"strings"
	"testing"

	"github.com/mailru/easyjson"
	"github.com/stretchr/testify/require"
	types "github.com/tigerbeetle/tigerbeetle-go"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/model"
)

const (
	testLedgerID   = uint32(1)
	testLedgerName = "USD"
	testScale      = int32(2)
)

func testRegistry() *model.Registry {
	return model.NewRegistry(&config.Config{
		Ledgers: map[string]config.Ledger{testLedgerName: {ID: testLedgerID, Scale: testScale}},
		Codes:   map[string]uint16{"payment": 7, "customer": 3, "merchant": 4},
	})
}

func u128(n int64) types.Uint128 {
	bi := big.NewInt(n)
	return types.BigIntToUint128(bi)
}

// idOf makes a distinct, valid Uint128 id whose UUID rendering is stable.
func idOf(n int64) types.Uint128 { return u128(n) }

// sampleEvent is a fully-populated change event: every field the message
// format carries is non-zero, so a field dropped by the encoder shows up as a
// zero value in the assertions rather than passing unnoticed.
func sampleEvent(t types.ChangeEventType) types.ChangeEvent {
	return types.ChangeEvent{
		Type:                        t,
		Timestamp:                   1745328372192037030,
		Ledger:                      testLedgerID,
		TransferID:                  idOf(101),
		TransferAmount:              u128(1234),
		TransferPendingID:           idOf(102),
		TransferUserData128:         idOf(103),
		TransferUserData64:          64,
		TransferUserData32:          32,
		TransferTimeout:             90,
		TransferCode:                7,
		TransferFlags:               types.TransferFlags{Pending: true}.ToUint16(),
		TransferTimestamp:           1745328372192037030,
		DebitAccountID:              idOf(201),
		DebitAccountDebitsPending:   u128(100),
		DebitAccountDebitsPosted:    u128(125000),
		DebitAccountCreditsPending:  u128(200),
		DebitAccountCreditsPosted:   u128(300),
		DebitAccountUserData128:     idOf(202),
		DebitAccountUserData64:      641,
		DebitAccountUserData32:      321,
		DebitAccountCode:            3,
		DebitAccountFlags:           types.AccountFlags{History: true}.ToUint16(),
		DebitAccountTimestamp:       1745328372192037000,
		CreditAccountID:             idOf(301),
		CreditAccountDebitsPending:  u128(400),
		CreditAccountDebitsPosted:   u128(500),
		CreditAccountCreditsPending: u128(600),
		CreditAccountCreditsPosted:  u128(700),
		CreditAccountUserData128:    idOf(302),
		CreditAccountUserData64:     642,
		CreditAccountUserData32:     322,
		CreditAccountCode:           4,
		CreditAccountFlags:          types.AccountFlags{Closed: true}.ToUint16(),
		CreditAccountTimestamp:      1745328372192037001,
	}
}

func testEncoder(t *testing.T, key string) (*Encoder, *bytes.Buffer) {
	t.Helper()
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	if key == "" {
		key = config.PartitionKeyDebitAccountID
	}
	return NewEncoder(config.CDC{Topic: "events", PartitionKey: key}, testRegistry(), log), &logs
}

func decode(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(body, &m))
	return m
}

func TestRecordCarriesEveryEventType(t *testing.T) {
	want := map[types.ChangeEventType]string{
		types.ChangeEventSinglePhase:     "single_phase",
		types.ChangeEventTwoPhasePending: "two_phase_pending",
		types.ChangeEventTwoPhasePosted:  "two_phase_posted",
		types.ChangeEventTwoPhaseVoided:  "two_phase_voided",
		types.ChangeEventTwoPhaseExpired: "two_phase_expired",
	}
	require.Len(t, want, 5)
	enc, logs := testEncoder(t, "")
	for evType, name := range want {
		rec, err := enc.Record(sampleEvent(evType), 42)
		require.NoError(t, err)
		body := decode(t, rec.Value)
		require.Equal(t, name, body["type"])
		require.Equal(t, name, headerMap(rec)[HeaderEventType])
	}
	require.NotContains(t, logs.String(), "level=WARN")
}

// Both account snapshots travel with the event: balances, user data, code,
// flags and each account's own timestamp.
func TestRecordCarriesBothAccountSnapshots(t *testing.T) {
	enc, _ := testEncoder(t, "")
	rec, err := enc.Record(sampleEvent(types.ChangeEventTwoPhasePending), 42)
	require.NoError(t, err)
	body := decode(t, rec.Value)

	require.Equal(t, "1745328372192037030", body["timestamp"])
	require.Equal(t, "42", body["checkpoint"])
	require.Equal(t, testLedgerName, body["ledger"])

	transfer := body["transfer"].(map[string]any)
	require.Equal(t, model.FormatID(idOf(101)), transfer["id"])
	require.Equal(t, "12.34", transfer["amount"], "amounts are decimal strings at the ledger scale")
	require.Equal(t, model.FormatID(idOf(102)), transfer["pending_id"])
	require.Equal(t, model.FormatID(idOf(103)), transfer["user_data_128"])
	require.EqualValues(t, 64, transfer["user_data_64"])
	require.EqualValues(t, 32, transfer["user_data_32"])
	require.Equal(t, "1m30s", transfer["timeout"])
	require.Equal(t, "payment", transfer["code"])
	require.Equal(t, []any{"pending"}, transfer["flags"])
	require.Equal(t, "1745328372192037030", transfer["timestamp"])

	debit := body["debit_account"].(map[string]any)
	require.Equal(t, model.FormatID(idOf(201)), debit["id"])
	require.Equal(t, "1.00", debit["debits_pending"])
	require.Equal(t, "1250.00", debit["debits_posted"])
	require.Equal(t, "2.00", debit["credits_pending"])
	require.Equal(t, "3.00", debit["credits_posted"])
	require.Equal(t, model.FormatID(idOf(202)), debit["user_data_128"])
	require.EqualValues(t, 641, debit["user_data_64"])
	require.EqualValues(t, 321, debit["user_data_32"])
	require.Equal(t, "customer", debit["code"])
	require.Equal(t, []any{"history"}, debit["flags"])
	require.Equal(t, "1745328372192037000", debit["timestamp"])

	credit := body["credit_account"].(map[string]any)
	require.Equal(t, model.FormatID(idOf(301)), credit["id"])
	require.Equal(t, "4.00", credit["debits_pending"])
	require.Equal(t, "5.00", credit["debits_posted"])
	require.Equal(t, "6.00", credit["credits_pending"])
	require.Equal(t, "7.00", credit["credits_posted"])
	require.Equal(t, model.FormatID(idOf(302)), credit["user_data_128"])
	require.EqualValues(t, 642, credit["user_data_64"])
	require.EqualValues(t, 322, credit["user_data_32"])
	require.Equal(t, "merchant", credit["code"])
	require.Equal(t, []any{"closed"}, credit["flags"])
	require.Equal(t, "1745328372192037001", credit["timestamp"])
}

func TestRecordHeaders(t *testing.T) {
	enc, _ := testEncoder(t, "")
	rec, err := enc.Record(sampleEvent(types.ChangeEventSinglePhase), 0)
	require.NoError(t, err)
	h := headerMap(rec)
	require.Equal(t, "single_phase", h[HeaderEventType])
	require.Equal(t, testLedgerName, h[HeaderLedger])
	require.Equal(t, "payment", h[HeaderTransferCode])
	require.Equal(t, "customer", h[HeaderDebitAccountCode])
	require.Equal(t, "merchant", h[HeaderCreditAccountCode])
	require.Equal(t, "1745328372192037030", h[HeaderTimestamp])
	require.Equal(t, "events", rec.Topic)
}

// A registry gap must never cost an event. The message goes out with the
// numeric value in place of the name, and the operator is told.
func TestUnknownLedgerAndCodePublishNumericValuesAndWarn(t *testing.T) {
	enc, logs := testEncoder(t, "")
	ev := sampleEvent(types.ChangeEventSinglePhase)
	ev.Ledger = 99
	ev.TransferCode = 98
	ev.DebitAccountCode = 97
	ev.CreditAccountCode = 96

	rec, err := enc.Record(ev, 0)
	require.NoError(t, err)
	body := decode(t, rec.Value)
	require.Equal(t, "99", body["ledger"])
	require.Equal(t, "98", body["transfer"].(map[string]any)["code"])
	require.Equal(t, "97", body["debit_account"].(map[string]any)["code"])
	require.Equal(t, "96", body["credit_account"].(map[string]any)["code"])

	// Without a scale, minor units are all we can honestly claim: the amount
	// stays an integer string rather than being scaled by a guess.
	require.Equal(t, "1234", body["transfer"].(map[string]any)["amount"])

	out := logs.String()
	require.Contains(t, out, "level=WARN")
	require.Contains(t, out, "unknown ledger")
	require.Contains(t, out, "unknown code")
	require.Contains(t, out, "1745328372192037030", "the warning must name the event it is about")
}

// A transfer or account with no flags is the common case, and it must still
// serialise as [] — a consumer validating the field as an array would break
// on the majority of messages if it were null.
func TestZeroFlagsSerialiseAsAnEmptyArray(t *testing.T) {
	enc, _ := testEncoder(t, "")
	ev := sampleEvent(types.ChangeEventSinglePhase)
	ev.TransferFlags = 0
	ev.DebitAccountFlags = 0
	ev.CreditAccountFlags = 0

	rec, err := enc.Record(ev, 0)
	require.NoError(t, err)
	require.NotContains(t, string(rec.Value), `"flags":null`)

	body := decode(t, rec.Value)
	for _, field := range []string{"transfer", "debit_account", "credit_account"} {
		require.Equal(t, []any{}, body[field].(map[string]any)["flags"], field)
	}
}

// A registry gap is a static condition: every event on that ledger hits it.
// Warning per occurrence would flood the log at the full event rate, so each
// distinct unknown value is reported once.
func TestUnknownValuesAreWarnedAboutOnlyOnce(t *testing.T) {
	enc, logs := testEncoder(t, "")
	ev := sampleEvent(types.ChangeEventSinglePhase)
	ev.Ledger = 99
	ev.TransferCode = 98
	ev.DebitAccountCode = 98
	ev.CreditAccountCode = 98
	ev.Type = types.ChangeEventType(200)

	for i := 0; i < 50; i++ {
		ev.Timestamp = uint64(1745328372192037030 + i)
		_, err := enc.Record(ev, 0)
		require.NoError(t, err)
	}

	out := logs.String()
	require.Equal(t, 1, strings.Count(out, "unknown ledger"))
	require.Equal(t, 1, strings.Count(out, "unknown code"), "one warning per code value, not per field or event")
	require.Equal(t, 1, strings.Count(out, "unknown change event type"))

	// A second, different unknown code is still reported: the rate limit is
	// per value, not a global one-shot.
	ev.TransferCode = 97
	_, err := enc.Record(ev, 0)
	require.NoError(t, err)
	require.Equal(t, 2, strings.Count(logs.String(), "unknown code"))
}

func TestPartitionKeyMatchesConfig(t *testing.T) {
	ev := sampleEvent(types.ChangeEventSinglePhase)
	for _, tc := range []struct {
		key  string
		want string
	}{
		{config.PartitionKeyDebitAccountID, model.FormatID(idOf(201))},
		{config.PartitionKeyCreditAccountID, model.FormatID(idOf(301))},
		{config.PartitionKeyTransferID, model.FormatID(idOf(101))},
		{config.PartitionKeyLedger, testLedgerName},
	} {
		t.Run(tc.key, func(t *testing.T) {
			enc, _ := testEncoder(t, tc.key)
			rec, err := enc.Record(ev, 0)
			require.NoError(t, err)
			require.Equal(t, tc.want, string(rec.Key))
		})
	}
}

// Optional fields are omitted rather than sent as an empty string: a
// zero pending id is not an id, and "" is not a UUID the sink would accept.
func TestZeroOptionalFieldsAreOmitted(t *testing.T) {
	enc, _ := testEncoder(t, "")
	ev := sampleEvent(types.ChangeEventSinglePhase)
	ev.TransferPendingID = types.Uint128{}
	ev.TransferUserData128 = types.Uint128{}
	ev.TransferTimeout = 0
	ev.DebitAccountUserData128 = types.Uint128{}

	rec, err := enc.Record(ev, 0)
	require.NoError(t, err)
	body := decode(t, rec.Value)
	transfer := body["transfer"].(map[string]any)
	require.NotContains(t, transfer, "pending_id")
	require.NotContains(t, transfer, "user_data_128")
	require.NotContains(t, transfer, "timeout")
	require.NotContains(t, body["debit_account"].(map[string]any), "user_data_128")
}

// The reused writer must be invisible in the output: the same bytes
// easyjson.Marshal produced before it existed, for every event type and
// whichever chunk path the message takes.
func TestRecordBodyMatchesEasyjsonMarshal(t *testing.T) {
	enc, _ := testEncoder(t, "")
	for _, typ := range []types.ChangeEventType{
		types.ChangeEventSinglePhase,
		types.ChangeEventTwoPhasePending,
		types.ChangeEventTwoPhasePosted,
		types.ChangeEventTwoPhaseVoided,
		types.ChangeEventTwoPhaseExpired,
	} {
		ev := sampleEvent(typ)
		rec, err := enc.Record(ev, ev.Timestamp)
		require.NoError(t, err)

		var msg Message
		require.NoError(t, easyjson.Unmarshal(rec.Value, &msg))
		want, err := easyjson.Marshal(msg)
		require.NoError(t, err)
		require.Equal(t, string(want), string(rec.Value))
	}
}

// The hazard the scratch buffer exists to avoid. franz-go retains
// kgo.Record.Value until the broker acknowledges the record, so a body handed
// to one record must not be touched by the next event. This encodes a batch
// the way the job does — holding every record — and then checks that nothing
// was rewritten under it.
func TestRecordBodiesDoNotAliasAcrossEvents(t *testing.T) {
	enc, _ := testEncoder(t, "")
	const n = 64
	recs := make([]*kgo.Record, 0, n)
	want := make([]string, 0, n)
	for i := 0; i < n; i++ {
		ev := sampleEvent(types.ChangeEventSinglePhase)
		// Vary the length so a later, longer message would overwrite an
		// earlier, shorter one if they shared a buffer.
		ev.Timestamp = uint64(1745328372192037030 + i)
		ev.TransferAmount = u128(int64(1 + i*7919))
		ev.TransferID = idOf(int64(1000 + i))
		rec, err := enc.Record(ev, ev.Timestamp)
		require.NoError(t, err)
		recs = append(recs, rec)
		want = append(want, string(append([]byte(nil), rec.Value...)))
	}
	for i, rec := range recs {
		require.Equal(t, want[i], string(rec.Value), "record %d was rewritten by a later event", i)
	}
	// Distinct backing arrays, not merely equal contents at the end.
	seen := map[*byte]int{}
	for i, rec := range recs {
		p := &rec.Value[0]
		if prev, ok := seen[p]; ok {
			t.Fatalf("records %d and %d share a backing array", prev, i)
		}
		seen[p] = i
	}
}

// A message larger than the scratch chunk has to come out identical too:
// that path leaves easyjson to chain and concatenate its own chunks, and it
// must not keep the scratch, which easyjson returns to its pool.
func TestRecordBodyLargerThanScratch(t *testing.T) {
	long := strings.Repeat("l", scratchSize)
	reg := model.NewRegistry(&config.Config{
		Ledgers: map[string]config.Ledger{long: {ID: testLedgerID, Scale: testScale}},
		Codes:   map[string]uint16{"payment": 7, "customer": 3, "merchant": 4},
	})
	log := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	enc := NewEncoder(config.CDC{Topic: "events"}, reg, log)

	ev := sampleEvent(types.ChangeEventSinglePhase)
	rec, err := enc.Record(ev, ev.Timestamp)
	require.NoError(t, err)
	require.Greater(t, len(rec.Value), scratchSize)

	var msg Message
	require.NoError(t, easyjson.Unmarshal(rec.Value, &msg))
	want, err := easyjson.Marshal(msg)
	require.NoError(t, err)
	require.Equal(t, string(want), string(rec.Value))
	require.Equal(t, long, decode(t, rec.Value)["ledger"])

	// The encoder must still be usable, and still not alias, afterwards.
	next, err := enc.Record(ev, ev.Timestamp)
	require.NoError(t, err)
	require.Equal(t, string(rec.Value), string(next.Value))
	require.NotSame(t, &rec.Value[0], &next.Value[0])
}

// What the scratch replacement in marshal's spill branch guards against:
// BuildBytes hands every chunk it walks, the scratch included, back to
// easyjson's global sync.Pool via putBuf. That pool is shared with every
// other easyjson user in the process — internal/emit, running on the sink
// goroutine, is the other one — so if the encoder kept writing into the
// scratch after a spill instead of replacing it, a later encode here and a
// later encode in internal/emit could both draw that same pooled chunk and
// silently overwrite each other's JSON. Nothing would crash; a downstream
// consumer would just receive corrupted bytes. This pins that the scratch
// actually becomes a fresh array once a spill happens, not merely a
// same-array slice with a reset length.
func TestSpillReplacesScratchArray(t *testing.T) {
	long := strings.Repeat("l", scratchSize)
	reg := model.NewRegistry(&config.Config{
		Ledgers: map[string]config.Ledger{long: {ID: testLedgerID, Scale: testScale}},
		Codes:   map[string]uint16{"payment": 7, "customer": 3, "merchant": 4},
	})
	log := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	enc := NewEncoder(config.CDC{Topic: "events"}, reg, log)

	// The array backing the pre-spill scratch: what BuildBytes will walk and
	// hand back to easyjson's pool once the encode below spills.
	before := enc.scratch
	beforeArray := &before[:cap(before)][0]

	ev := sampleEvent(types.ChangeEventSinglePhase)
	rec, err := enc.Record(ev, ev.Timestamp)
	require.NoError(t, err)
	require.Greater(t, len(rec.Value), scratchSize, "the message must actually spill for this test to mean anything")

	afterArray := &enc.scratch[:cap(enc.scratch)][0]
	require.NotSame(t, beforeArray, afterArray,
		"scratch after a spill must be a fresh array, not the one BuildBytes walked and returned to easyjson's pool")
}
