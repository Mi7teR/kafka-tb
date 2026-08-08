package tbx

import (
	"context"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/stretchr/testify/require"
	types "github.com/tigerbeetle/tigerbeetle-go"
)

// nopReads закрывает читающую половину Client для стабов, которым интересна
// только запись.
type nopReads struct{}

func (nopReads) CreateAccounts([]types.Account) ([]types.CreateAccountResult, error) {
	return nil, nil
}
func (nopReads) LookupAccounts([]types.Uint128) ([]types.Account, error)   { return nil, nil }
func (nopReads) LookupTransfers([]types.Uint128) ([]types.Transfer, error) { return nil, nil }
func (nopReads) GetAccountTransfers(types.AccountFilter) ([]types.Transfer, error) {
	return nil, nil
}
func (nopReads) GetAccountBalances(types.AccountFilter) ([]types.AccountBalance, error) {
	return nil, nil
}
func (nopReads) QueryAccounts(types.QueryFilter) ([]types.Account, error)   { return nil, nil }
func (nopReads) QueryTransfers(types.QueryFilter) ([]types.Transfer, error) { return nil, nil }
func (nopReads) Nop() error                                                 { return nil }
func (nopReads) Close()                                                     {}

// orderedCmd — команда из одного события, помеченного порядковым номером
// постановки: по нему стаб восстанавливает порядок применения.
func orderedCmd(seq uint64, key string) *model.Command {
	return &model.Command{
		Op:        model.OpCreateTransfers,
		Key:       key,
		Transfers: []types.Transfer{{ID: types.ToUint128(seq), UserData64: seq}},
		IDs:       []string{"c" + strconv.FormatUint(seq, 10)},
	}
}

// orderedAccountCmd — команда создания одного счёта, помеченная тем же порядковым
// номером, что и orderedCmd: обе операции складываются в один порядок применения.
func orderedAccountCmd(seq uint64, key string) *model.Command {
	return &model.Command{
		Op:       model.OpCreateAccounts,
		Key:      key,
		Accounts: []types.Account{{ID: types.ToUint128(seq), UserData64: seq}},
		IDs:      []string{"c" + strconv.FormatUint(seq, 10)},
	}
}

// orderClient записывает порядок, в котором события доходят до TigerBeetle, и
// растягивает каждый вызов на случайное время: батчи, которые действительно
// летят одновременно, возвращаются вперемешку. Поэтому совпадение записанного
// порядка с порядком постановки — свидетельство сериализации, а не везения.
type orderClient struct {
	nopReads
	stagger time.Duration

	mu      sync.Mutex
	applied []uint64
}

func (c *orderClient) CreateTransfers(ts []types.Transfer) ([]types.CreateTransferResult, error) {
	if c.stagger > 0 {
		time.Sleep(time.Duration(rand.Int63n(int64(c.stagger)) + 1))
	}
	c.mu.Lock()
	for _, t := range ts {
		c.applied = append(c.applied, t.UserData64)
	}
	c.mu.Unlock()
	return defaultTransferResults(len(ts)), nil
}

func (c *orderClient) order() []uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]uint64(nil), c.applied...)
}

// barrierClient отвечает только после того, как в него зашли n вызовов
// одновременно, — то есть после того, как n батчей реально оказались в полёте
// разом. Если этого не случилось за timeout, вызов всё равно отпускается (иначе
// батчер повис бы и тест не сказал бы ничего внятного), но факт фиксируется в
// timedOut: именно он отличает «параллельно» от «по очереди».
//
// reverse задаёт порядок записи: при true собранные барьером батчи пишутся в
// порядке, обратном порядку постановки (маркеры по убыванию). Так инверсия
// между двумя одновременно летящими батчами получается детерминированно, а не
// по воле планировщика.
type barrierClient struct {
	nopReads
	n       int
	reverse bool
	timeout time.Duration
	gate    chan struct{}
	release []chan struct{}

	mu       sync.Mutex
	arrived  int
	marks    []uint64 // первый маркер батча каждого пришедшего, в порядке прихода
	next     []int    // next[i] — кто пишет после i; -1 — последний
	applied  []uint64
	timedout bool
}

func newBarrierClient(n int, reverse bool, timeout time.Duration) *barrierClient {
	c := &barrierClient{
		n: n, reverse: reverse, timeout: timeout,
		gate: make(chan struct{}), release: make([]chan struct{}, n),
	}
	for i := range c.release {
		c.release[i] = make(chan struct{})
	}
	return c
}

func (c *barrierClient) CreateTransfers(ts []types.Transfer) ([]types.CreateTransferResult, error) {
	c.mu.Lock()
	idx := c.arrived
	c.arrived++
	c.marks = append(c.marks, ts[0].UserData64)
	last := c.arrived == c.n
	first := -1
	if last {
		first = c.planReverseOrder()
	}
	c.mu.Unlock()

	full := true
	if last {
		close(c.gate)
		if first >= 0 {
			close(c.release[first])
		}
	} else {
		select {
		case <-c.gate:
		case <-time.After(c.timeout):
			full = false
			c.mu.Lock()
			c.timedout = true
			c.mu.Unlock()
		}
	}

	ordered := full && c.reverse && idx < c.n
	if ordered {
		<-c.release[idx]
	}
	c.mu.Lock()
	for _, t := range ts {
		c.applied = append(c.applied, t.UserData64)
	}
	after := -1
	if ordered {
		after = c.next[idx]
	}
	c.mu.Unlock()
	if after >= 0 {
		close(c.release[after])
	}
	return defaultTransferResults(len(ts)), nil
}

// planReverseOrder выстраивает пришедших в порядке убывания маркера и
// возвращает того, кто пишет первым. Вызывается под c.mu ровно один раз —
// последним пришедшим, до закрытия gate, поэтому next виден всем остальным.
func (c *barrierClient) planReverseOrder() int {
	if !c.reverse {
		return -1
	}
	order := make([]int, c.n)
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool { return c.marks[order[a]] > c.marks[order[b]] })
	c.next = make([]int, c.n)
	for i, arrival := range order {
		if i+1 < len(order) {
			c.next[arrival] = order[i+1]
			continue
		}
		c.next[arrival] = -1
	}
	return order[0]
}

func (c *barrierClient) order() []uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]uint64(nil), c.applied...)
}

func (c *barrierClient) overlapped() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.timedout
}

func startSharded(t *testing.T, c Client, cfg config.Batcher) *Batcher {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	b := NewBatcher(c, cfg, config.Retry{Initial: time.Millisecond, Max: 10 * time.Millisecond},
		testLogger(), nil)
	b.Start(ctx)
	t.Cleanup(func() { cancel(); b.Close() })
	return b
}

// submitAll ставит команды 1..n с ключом key и дожидается исхода каждой.
func submitAll(t *testing.T, b *Batcher, n int, key string) {
	t.Helper()
	chans := make([]<-chan SubmitResult, n)
	for i := range chans {
		ch, err := b.SubmitAsync(context.Background(), orderedCmd(uint64(i+1), key))
		require.NoError(t, err, "submit %d", i+1)
		chans[i] = ch
	}
	for i, ch := range chans {
		select {
		case res := <-ch:
			require.NoError(t, res.Err, "command %d", i+1)
		case <-time.After(10 * time.Second):
			t.Fatalf("no outcome for command %d", i+1)
		}
	}
}

func seq(n int) []uint64 {
	out := make([]uint64, n)
	for i := range out {
		out[i] = uint64(i + 1)
	}
	return out
}

// Инвариант, ради которого всё шардирование и делается: команды с одним ключом
// применяются строго в порядке постановки, сколько бы воркеров ни было.
func TestBatcherSameKeyAppliedInSubmitOrder(t *testing.T) {
	const n = 500
	fc := &orderClient{stagger: 300 * time.Microsecond}
	b := startSharded(t, fc, config.Batcher{
		MaxBatchSize: 8, Linger: time.Millisecond, MaxQueue: 512, Shards: 4,
	})
	submitAll(t, b, n, "ledger.transfers/7")
	require.Equal(t, seq(n), fc.order(),
		"команды одного ключа применились не в порядке постановки")
}

// Разные ключи обязаны уходить в разные запросы одновременно — иначе
// шардирование не даёт ничего. Стаб держит вызов, пока в него не зайдут двое:
// с одним воркером на тип операции этого не случится никогда.
func TestBatcherDifferentKeysRunConcurrently(t *testing.T) {
	fc := newBarrierClient(2, false, 2*time.Second)
	b := startSharded(t, fc, config.Batcher{
		MaxBatchSize: 1, Linger: time.Hour, MaxQueue: 8, Shards: 4,
	})
	k1, k2 := keysOnDistinctShards(t, b)

	chans := make([]<-chan SubmitResult, 0, 2)
	for i, k := range []string{k1, k2} {
		ch, err := b.SubmitAsync(context.Background(), orderedCmd(uint64(i+1), k))
		require.NoError(t, err)
		chans = append(chans, ch)
	}
	for i, ch := range chans {
		select {
		case res := <-ch:
			require.NoError(t, res.Err, "command %d", i+1)
		case <-time.After(10 * time.Second):
			t.Fatalf("no outcome for command %d", i+1)
		}
	}
	require.True(t, fc.overlapped(),
		"два ключа не оказались в полёте одновременно: батчер сериализует их")
}

// Обратная сторона того же барьера: два ключа он сводит вместе, а один ключ —
// не должен свести никогда. Здесь max_batch_size=1, поэтому первая команда
// улетает одна и вторая может догнать её только через второй воркер.
func TestBatcherSameKeyIsNeverInFlightTwice(t *testing.T) {
	fc := newBarrierClient(2, false, 300*time.Millisecond)
	b := startSharded(t, fc, config.Batcher{
		MaxBatchSize: 1, Linger: time.Hour, MaxQueue: 8, Shards: 4,
	})
	submitAll(t, b, 2, "ledger.transfers/3")
	require.False(t, fc.overlapped(),
		"две команды одного ключа оказались в полёте одновременно")
	require.Equal(t, seq(2), fc.order())
}

// Контроль: проверка порядка чего-то стоит только если батчер, разложивший
// один ключ по двум воркерам, её проваливает. Привязка здесь выключена
// принудительно, барьер сводит оба батча вместе и отдаёт их в обратном
// порядке — инверсия обязана быть замечена.
func TestBatcherWithoutKeyAffinityOrderIsViolated(t *testing.T) {
	fc := newBarrierClient(2, true, 2*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	b := NewBatcher(fc, config.Batcher{MaxBatchSize: 1, Linger: time.Hour, MaxQueue: 8, Shards: 2},
		config.Retry{Initial: time.Millisecond, Max: 10 * time.Millisecond}, testLogger(), nil)
	var next int
	var mu sync.Mutex
	b.pickShard = func(string) int {
		mu.Lock()
		defer mu.Unlock()
		next++
		return (next - 1) % 2
	}
	b.Start(ctx)
	t.Cleanup(func() { cancel(); b.Close() })

	submitAll(t, b, 2, "ledger.transfers/3")
	require.True(t, fc.overlapped(),
		"батчи не пересеклись — контроль ничего не доказывает")
	require.Equal(t, []uint64{2, 1}, fc.order(),
		"без привязки к ключу порядок применения обязан был нарушиться")
}

// mixedBarrierClient — тот же барьер, но общий для обеих операций: он сводит
// вместе вызовы CreateAccounts и CreateTransfers. Один ключ обязан не свести их
// никогда, сколько бы типов операций он ни нёс. Записывает порядок применения
// с пометкой операции ("a1", "t2"), чтобы было видно не только пересечение, но
// и инверсию.
type mixedBarrierClient struct {
	n       int
	timeout time.Duration
	gate    chan struct{}

	mu       sync.Mutex
	arrived  int
	applied  []string
	timedout bool
}

func newMixedBarrierClient(n int, timeout time.Duration) *mixedBarrierClient {
	return &mixedBarrierClient{n: n, timeout: timeout, gate: make(chan struct{})}
}

// enter отпускает вызывающего, только когда в клиенте одновременно оказались n
// вызовов. Если этого не случилось за timeout, вызов всё равно отпускается —
// иначе батчер повис бы, — но факт «одновременности не было» остаётся в
// timedout.
func (c *mixedBarrierClient) enter() {
	c.mu.Lock()
	c.arrived++
	last := c.arrived == c.n
	c.mu.Unlock()
	if last {
		close(c.gate)
		return
	}
	select {
	case <-c.gate:
	case <-time.After(c.timeout):
		c.mu.Lock()
		c.timedout = true
		c.mu.Unlock()
	}
}

func (c *mixedBarrierClient) record(prefix string, marks []uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range marks {
		c.applied = append(c.applied, prefix+strconv.FormatUint(m, 10))
	}
}

func (c *mixedBarrierClient) CreateTransfers(ts []types.Transfer) ([]types.CreateTransferResult, error) {
	c.enter()
	marks := make([]uint64, len(ts))
	for i, t := range ts {
		marks[i] = t.UserData64
	}
	c.record("t", marks)
	return defaultTransferResults(len(ts)), nil
}

func (c *mixedBarrierClient) CreateAccounts(as []types.Account) ([]types.CreateAccountResult, error) {
	c.enter()
	marks := make([]uint64, len(as))
	for i, a := range as {
		marks[i] = a.UserData64
	}
	c.record("a", marks)
	return defaultAccountResults(len(as)), nil
}

func (c *mixedBarrierClient) order() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.applied...)
}

func (c *mixedBarrierClient) overlapped() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.timedout
}

func (c *mixedBarrierClient) LookupAccounts([]types.Uint128) ([]types.Account, error) {
	return nil, nil
}

func (c *mixedBarrierClient) LookupTransfers([]types.Uint128) ([]types.Transfer, error) {
	return nil, nil
}

func (c *mixedBarrierClient) GetAccountTransfers(types.AccountFilter) ([]types.Transfer, error) {
	return nil, nil
}

func (c *mixedBarrierClient) GetAccountBalances(types.AccountFilter) ([]types.AccountBalance, error) {
	return nil, nil
}

func (c *mixedBarrierClient) QueryAccounts(types.QueryFilter) ([]types.Account, error) {
	return nil, nil
}

func (c *mixedBarrierClient) QueryTransfers(types.QueryFilter) ([]types.Transfer, error) {
	return nil, nil
}
func (c *mixedBarrierClient) Nop() error { return nil }
func (c *mixedBarrierClient) Close()     {}

// Ключ владеет собой целиком, а не по типу операции: create_accounts и
// create_transfers одной партиции обязаны примениться по очереди и в порядке
// постановки. Это ровно та последовательность, которую пишет обычный
// продюсер, — завести счёт и тут же с него списать; разъехавшись, она даёт
// debit_account_not_found, то есть бизнес-отказ и DLQ для законного трансфера.
func TestBatcherSameKeyMixedOpsNeverInFlightTogether(t *testing.T) {
	fc := newMixedBarrierClient(2, 300*time.Millisecond)
	b := startSharded(t, fc, config.Batcher{
		MaxBatchSize: 1, Linger: time.Hour, MaxQueue: 8, Shards: 4,
	})
	const key = "ledger.transfers/0"

	chans := make([]<-chan SubmitResult, 0, 2)
	for _, cmd := range []*model.Command{orderedAccountCmd(1, key), orderedCmd(2, key)} {
		ch, err := b.SubmitAsync(context.Background(), cmd)
		require.NoError(t, err)
		chans = append(chans, ch)
	}
	for i, ch := range chans {
		select {
		case res := <-ch:
			require.NoError(t, res.Err, "command %d", i+1)
		case <-time.After(10 * time.Second):
			t.Fatalf("no outcome for command %d", i+1)
		}
	}

	require.False(t, fc.overlapped(),
		"команды одного ключа разных операций оказались в полёте одновременно")
	require.Equal(t, []string{"a1", "t2"}, fc.order(),
		"порядок применения разошёлся с порядком постановки")
}

// keysOnDistinctShards подбирает два ключа, которые батчер разводит по разным
// воркерам. Хеш засеян на процесс, поэтому конкретные ключи заранее неизвестны.
func keysOnDistinctShards(t *testing.T, b *Batcher) (string, string) {
	t.Helper()
	first := "ledger.transfers/0"
	for i := 1; i < 1000; i++ {
		k := "ledger.transfers/" + strconv.Itoa(i)
		if b.pickShard(k) != b.pickShard(first) {
			return first, k
		}
	}
	t.Fatal("не нашлось двух ключей на разных шардах")
	return "", ""
}
