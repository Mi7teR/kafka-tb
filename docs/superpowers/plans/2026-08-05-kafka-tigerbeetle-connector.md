# Kafka → TigerBeetle коннектор: план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Сервис, который применяет финансовые операции из Kafka в TigerBeetle с DLQ и топиком результатов, и отдаёт балансы/выписки через gRPC + REST.

**Architecture:** Один Go-бинарь с режимами `sink|api|all`. Все обращения к TigerBeetle идут через единственный `tbx.Batcher`, который упаковывает Job'ы в батчи ≤8189 событий, отправляет их последовательно и разводит sparse-результаты обратно по Job'ам. Sink коммитит офсеты вручную непрерывным префиксом только после подтверждения от TigerBeetle и ack от DLQ/results-продюсера.

**Tech Stack:** Go 1.23+, `github.com/twmb/franz-go`, `github.com/tigerbeetle/tigerbeetle-go` v0.17.9 (cgo), `google.golang.org/grpc` + `grpc-gateway/v2`, `buf`, `prometheus/client_golang`, `testcontainers-go`.

**Спека:** [2026-08-05-kafka-tigerbeetle-connector-design.md](../specs/2026-08-05-kafka-tigerbeetle-connector-design.md)

## Global Constraints

- Модуль: `github.com/Mi7teR/kafka-tb`. Регистр владельца обязателен.
- Go 1.23 или новее. `CGO_ENABLED=1` — клиент TigerBeetle использует cgo, чистая кросс-компиляция невозможна.
- TigerBeetle client v0.17.9. Максимум событий в одном `create_accounts`/`create_transfers` — **8189**.
- **macOS:** предсобранная статическая библиотека TigerBeetle не проходит линковку новым `ld` (`64-bit mach-o member 'libtb_client.a.o' not 8-byte aligned`). Сборка и тесты идут только через `make` — Makefile подставляет `-ldflags=-extldflags=-Wl,-ld_classic` на Darwin. Голый `go test ./...` на macOS падает на линковке; это не дефект кода.
- **Проверено на установленном v0.17.9:** все типы лежат в корневом пакете `github.com/tigerbeetle/tigerbeetle-go` (импортировать как `types "github.com/tigerbeetle/tigerbeetle-go"`), подпакета `pkg/types` не существует.
- `CreateTransfers` возвращает `[]CreateTransferResult`, где `CreateTransferResult{Timestamp uint64; Status CreateTransferStatus; Reserved uint32}`. Поля `Index` нет: массив **плотный и позиционный** — `results[i]` относится к `transfers[i]`. Аналогично `CreateAccounts` → `[]CreateAccountResult{Timestamp, Status, Reserved}`.
- Успех — `types.TransferCreated` (значение `0xFFFFFFFF`, не 0) или `types.TransferExists`. Для счетов — `types.AccountCreated`, `types.AccountExists`. Всё остальное — reject.
- Клиент уже содержит `Nop() error` — отдельно добавлять в интерфейс не нужно.
- Флаг `linked` не может стоять на последнем элементе батча — снимаем его на последнем элементе каждого сообщения.
- Суммы — только строки и `big.Int`. Использование `float64` для денег запрещено в любом месте кода.
- Ошибка данных никогда не завершает процесс. Паника внутри обработки сообщения перехватывается и превращается в poison.
- Офсет коммитится только после подтверждения TigerBeetle **и** ack продюсера DLQ/results.
- Каждая задача заканчивается зелёными тестами и коммитом. Формат сообщений — Conventional Commits.

---

### Task 1: Bootstrap модуля и конфиг

**Files:**
- Create: `go.mod`, `Makefile`, `.gitignore`, `.golangci.yml`
- Create: `internal/config/config.go`
- Test: `internal/config/config_test.go`
- Create: `configs/example.yaml`

**Interfaces:**
- Consumes: ничего.
- Produces: `config.Config` со всеми секциями; `config.Load(path string) (*Config, error)`; `cfg.LedgerByName(name string) (Ledger, bool)`; `cfg.CodeByName(name string) (uint16, bool)`; типы `config.Ledger{ID uint32; Scale int32}`, `config.Mode` (`ModeSink|ModeAPI|ModeAll`).

- [ ] **Step 1: Инициализировать модуль и зависимости**

```bash
go mod init github.com/Mi7teR/kafka-tb
go get gopkg.in/yaml.v3@latest
go get github.com/stretchr/testify@latest
```

- [ ] **Step 2: Написать падающий тест конфига**

`internal/config/config_test.go`:

```go
package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeCfg(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "cfg.yaml")
	require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
	return p
}

const validCfg = `
mode: all
tigerbeetle:
  cluster_id: 0
  addresses: ["3000"]
batcher:
  max_batch_size: 8189
  linger: 5ms
  max_queue: 1000
kafka:
  brokers: ["localhost:9092"]
  group: kafkatb
  topics:
    - {name: ledger.transfers, codec: json}
  dlq_topic: ledger.transfers.dlq
  results_topic: ledger.results
limits:
  max_message_bytes: 1048576
  max_events_per_message: 8189
  max_json_depth: 32
api:
  grpc_addr: ":9090"
  http_addr: ":8080"
  max_page_size: 1000
ledgers:
  USD: {id: 1, scale: 2}
codes:
  payment: 1
retry: {initial: 100ms, max: 30s, jitter: true}
shutdown_timeout: 30s
`

func TestLoadValid(t *testing.T) {
	cfg, err := Load(writeCfg(t, validCfg))
	require.NoError(t, err)
	require.Equal(t, ModeAll, cfg.Mode)
	l, ok := cfg.LedgerByName("USD")
	require.True(t, ok)
	require.Equal(t, uint32(1), l.ID)
	require.Equal(t, int32(2), l.Scale)
	c, ok := cfg.CodeByName("payment")
	require.True(t, ok)
	require.Equal(t, uint16(1), c)
}

func TestLoadRejectsOversizedBatch(t *testing.T) {
	body := validCfg[:len(validCfg)] // копия
	body = replace(body, "max_batch_size: 8189", "max_batch_size: 9000")
	_, err := Load(writeCfg(t, body))
	require.ErrorContains(t, err, "max_batch_size")
}

func TestLoadRejectsDuplicateLedgerID(t *testing.T) {
	body := replace(validCfg, "ledgers:\n  USD: {id: 1, scale: 2}",
		"ledgers:\n  USD: {id: 1, scale: 2}\n  EUR: {id: 1, scale: 2}")
	_, err := Load(writeCfg(t, body))
	require.ErrorContains(t, err, "duplicate ledger id")
}

func TestLoadRejectsEmptyDLQ(t *testing.T) {
	body := replace(validCfg, "dlq_topic: ledger.transfers.dlq", "dlq_topic: \"\"")
	_, err := Load(writeCfg(t, body))
	require.ErrorContains(t, err, "dlq_topic")
}

func TestEnvOverride(t *testing.T) {
	t.Setenv("KAFKATB_MODE", "sink")
	cfg, err := Load(writeCfg(t, validCfg))
	require.NoError(t, err)
	require.Equal(t, ModeSink, cfg.Mode)
}

func replace(s, old, new string) string {
	return stringsReplace(s, old, new)
}
```

Добавь в начало файла `import "strings"` и хелпер `func stringsReplace(s, old, new string) string { return strings.Replace(s, old, new, 1) }`.

- [ ] **Step 3: Убедиться, что тесты падают**

Run: `go test ./internal/config/ -v`
Expected: FAIL — `undefined: Load`.

- [ ] **Step 4: Реализовать конфиг**

`internal/config/config.go`:

```go
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const MaxBatchSize = 8189

type Mode string

const (
	ModeSink Mode = "sink"
	ModeAPI  Mode = "api"
	ModeAll  Mode = "all"
)

type Ledger struct {
	ID    uint32 `yaml:"id"`
	Scale int32  `yaml:"scale"`
}

type TigerBeetle struct {
	ClusterID uint64   `yaml:"cluster_id"`
	Addresses []string `yaml:"addresses"`
}

type Batcher struct {
	MaxBatchSize int           `yaml:"max_batch_size"`
	Linger       time.Duration `yaml:"linger"`
	MaxQueue     int           `yaml:"max_queue"`
}

type Topic struct {
	Name  string `yaml:"name"`
	Codec string `yaml:"codec"`
}

type Kafka struct {
	Brokers      []string `yaml:"brokers"`
	Group        string   `yaml:"group"`
	Topics       []Topic  `yaml:"topics"`
	DLQTopic     string   `yaml:"dlq_topic"`
	ResultsTopic string   `yaml:"results_topic"`
}

type Limits struct {
	MaxMessageBytes     int `yaml:"max_message_bytes"`
	MaxEventsPerMessage int `yaml:"max_events_per_message"`
	MaxJSONDepth        int `yaml:"max_json_depth"`
}

type API struct {
	GRPCAddr    string `yaml:"grpc_addr"`
	HTTPAddr    string `yaml:"http_addr"`
	MaxPageSize uint32 `yaml:"max_page_size"`
}

type Retry struct {
	Initial time.Duration `yaml:"initial"`
	Max     time.Duration `yaml:"max"`
	Jitter  bool          `yaml:"jitter"`
}

type Config struct {
	Mode            Mode              `yaml:"mode"`
	TigerBeetle     TigerBeetle       `yaml:"tigerbeetle"`
	Batcher         Batcher           `yaml:"batcher"`
	Kafka           Kafka             `yaml:"kafka"`
	Limits          Limits            `yaml:"limits"`
	API             API               `yaml:"api"`
	Ledgers         map[string]Ledger `yaml:"ledgers"`
	Codes           map[string]uint16 `yaml:"codes"`
	Retry           Retry             `yaml:"retry"`
	ShutdownTimeout time.Duration     `yaml:"shutdown_timeout"`
}

func (c *Config) LedgerByName(name string) (Ledger, bool) {
	l, ok := c.Ledgers[name]
	return l, ok
}

func (c *Config) CodeByName(name string) (uint16, bool) {
	v, ok := c.Codes[name]
	return v, ok
}

func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	applyEnv(&cfg)
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func applyEnv(cfg *Config) {
	if v := os.Getenv("KAFKATB_MODE"); v != "" {
		cfg.Mode = Mode(v)
	}
	if v := os.Getenv("KAFKATB_KAFKA_BROKERS"); v != "" {
		cfg.Kafka.Brokers = strings.Split(v, ",")
	}
	if v := os.Getenv("KAFKATB_KAFKA_GROUP"); v != "" {
		cfg.Kafka.Group = v
	}
	if v := os.Getenv("KAFKATB_TB_ADDRESSES"); v != "" {
		cfg.TigerBeetle.Addresses = strings.Split(v, ",")
	}
	if v := os.Getenv("KAFKATB_TB_CLUSTER_ID"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			cfg.TigerBeetle.ClusterID = n
		}
	}
}

func (c *Config) validate() error {
	switch c.Mode {
	case ModeSink, ModeAPI, ModeAll:
	default:
		return fmt.Errorf("mode: want sink|api|all, got %q", c.Mode)
	}
	if len(c.TigerBeetle.Addresses) == 0 {
		return fmt.Errorf("tigerbeetle.addresses: must not be empty")
	}
	if c.Batcher.MaxBatchSize <= 0 || c.Batcher.MaxBatchSize > MaxBatchSize {
		return fmt.Errorf("batcher.max_batch_size: want 1..%d, got %d", MaxBatchSize, c.Batcher.MaxBatchSize)
	}
	if c.Batcher.MaxQueue <= 0 {
		return fmt.Errorf("batcher.max_queue: must be > 0")
	}
	if c.Limits.MaxEventsPerMessage <= 0 || c.Limits.MaxEventsPerMessage > MaxBatchSize {
		return fmt.Errorf("limits.max_events_per_message: want 1..%d", MaxBatchSize)
	}
	if c.Limits.MaxMessageBytes <= 0 {
		return fmt.Errorf("limits.max_message_bytes: must be > 0")
	}
	if c.Limits.MaxJSONDepth <= 0 {
		return fmt.Errorf("limits.max_json_depth: must be > 0")
	}
	if c.Mode != ModeAPI {
		if len(c.Kafka.Brokers) == 0 {
			return fmt.Errorf("kafka.brokers: must not be empty")
		}
		if c.Kafka.Group == "" {
			return fmt.Errorf("kafka.group: must not be empty")
		}
		if len(c.Kafka.Topics) == 0 {
			return fmt.Errorf("kafka.topics: must not be empty")
		}
		if c.Kafka.DLQTopic == "" {
			return fmt.Errorf("kafka.dlq_topic: must not be empty")
		}
		for _, t := range c.Kafka.Topics {
			if t.Name == "" {
				return fmt.Errorf("kafka.topics: empty topic name")
			}
			if t.Codec != "json" {
				return fmt.Errorf("kafka.topics[%s].codec: only \"json\" supported, got %q", t.Name, t.Codec)
			}
		}
	}
	if len(c.Ledgers) == 0 {
		return fmt.Errorf("ledgers: must not be empty")
	}
	seenLedger := map[uint32]string{}
	for name, l := range c.Ledgers {
		if l.ID == 0 {
			return fmt.Errorf("ledgers[%s].id: must not be 0", name)
		}
		if l.Scale < 0 || l.Scale > 18 {
			return fmt.Errorf("ledgers[%s].scale: want 0..18, got %d", name, l.Scale)
		}
		if prev, ok := seenLedger[l.ID]; ok {
			return fmt.Errorf("duplicate ledger id %d: %s and %s", l.ID, prev, name)
		}
		seenLedger[l.ID] = name
	}
	if len(c.Codes) == 0 {
		return fmt.Errorf("codes: must not be empty")
	}
	seenCode := map[uint16]string{}
	for name, v := range c.Codes {
		if v == 0 {
			return fmt.Errorf("codes[%s]: must not be 0", name)
		}
		if prev, ok := seenCode[v]; ok {
			return fmt.Errorf("duplicate code %d: %s and %s", v, prev, name)
		}
		seenCode[v] = name
	}
	if c.Retry.Initial <= 0 || c.Retry.Max < c.Retry.Initial {
		return fmt.Errorf("retry: want 0 < initial <= max")
	}
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("shutdown_timeout: must be > 0")
	}
	if c.Mode != ModeSink {
		if c.API.GRPCAddr == "" || c.API.HTTPAddr == "" {
			return fmt.Errorf("api: grpc_addr and http_addr must be set")
		}
		if c.API.MaxPageSize == 0 {
			return fmt.Errorf("api.max_page_size: must be > 0")
		}
	}
	return nil
}
```

- [ ] **Step 5: Прогнать тесты**

Run: `go test ./internal/config/ -v`
Expected: PASS, все пять тестов.

- [ ] **Step 6: Makefile, .gitignore, конфиг-пример**

`Makefile`:

```makefile
export CGO_ENABLED=1

.PHONY: build test lint bench integration proto

build:
	go build -o bin/kafkatb ./cmd/kafkatb

test:
	go test ./... -race -count=1

integration:
	go test ./test/integration/... -tags=integration -count=1 -timeout=15m

bench:
	go test ./... -run=^$$ -bench=. -benchmem

lint:
	golangci-lint run

proto:
	buf generate
```

`.gitignore`:

```
bin/
*.tigerbeetle
docs/benchmarks/*.raw
```

`configs/example.yaml` — содержимое `validCfg` из теста, с комментариями к каждой секции.

- [ ] **Step 7: Коммит**

```bash
git add go.mod go.sum Makefile .gitignore configs internal/config
git commit -m "feat(config): add yaml config with strict validation"
```

---

### Task 2: Модель — Uint128, суммы, справочники

**Files:**
- Create: `internal/model/id.go`, `internal/model/amount.go`, `internal/model/registry.go`, `internal/model/command.go`
- Test: `internal/model/id_test.go`, `internal/model/amount_test.go`, `internal/model/amount_fuzz_test.go`, `internal/model/bench_test.go`

**Interfaces:**
- Consumes: `config.Config`, `config.Ledger`.
- Produces:
  - `model.ParseID(s string) (types.Uint128, error)` и `model.FormatID(u types.Uint128) string` — UUID-строка ↔ Uint128.
  - `model.ParseAmount(s string, scale int32) (types.Uint128, error)` и `model.FormatAmount(u types.Uint128, scale int32) string`.
  - `model.Registry` с методами `Ledger(name string) (config.Ledger, error)`, `LedgerName(id uint32) (string, error)`, `Code(name string) (uint16, error)`, `CodeName(v uint16) (string, error)`, `TransferFlags(names []string) (types.TransferFlags, error)`, `AccountFlags(names []string) (types.AccountFlags, error)`; конструктор `model.NewRegistry(cfg *config.Config) *Registry`.
  - `model.Command` — результат декодинга (см. код ниже), `model.OpCreateTransfers`, `model.OpCreateAccounts`.

- [ ] **Step 1: Подтянуть зависимости**

```bash
go get github.com/tigerbeetle/tigerbeetle-go@v0.17.9
go get github.com/google/uuid@latest
```

- [ ] **Step 2: Сверить имена типов клиента TigerBeetle**

Run: `go doc github.com/tigerbeetle/tigerbeetle-go TransferFlags && go doc github.com/tigerbeetle/tigerbeetle-go Uint128`
Expected: в выводе есть поля `Linked`, `Pending`, `PostPendingTransfer`, `VoidPendingTransfer`, метод `ToUint16()`, и функции `BytesToUint128`, `BigIntToUint128`. Если имена отличаются — правь код ниже под фактические, остальной план не меняется.

- [ ] **Step 3: Написать падающие тесты сумм и id**

`internal/model/amount_test.go`:

```go
package model

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseAmount(t *testing.T) {
	cases := []struct {
		in    string
		scale int32
		want  string // десятичное представление минорных единиц
	}{
		{"0", 2, "0"},
		{"12.34", 2, "1234"},
		{"12.3", 2, "1230"},
		{"12", 2, "1200"},
		{"0.01", 2, "1"},
		{"7", 0, "7"},
		{"340282366920938463463374607431768211455", 0, "340282366920938463463374607431768211455"}, // max u128
	}
	for _, c := range cases {
		got, err := ParseAmount(c.in, c.scale)
		require.NoError(t, err, c.in)
		bi := got.BigInt()
		require.Equal(t, c.want, bi.String(), c.in)
	}
}

func TestParseAmountRejects(t *testing.T) {
	bad := []struct {
		in    string
		scale int32
	}{
		{"", 2}, {"abc", 2}, {"-1", 2}, {"1.234", 2}, {"1.2.3", 2},
		{"1e5", 2}, {" 1", 2}, {"1 ", 2}, {"+1", 2}, {".", 2}, {"1.", 2}, {".5", 2},
		{"340282366920938463463374607431768211456", 0}, // u128 overflow
		{"3402823669209384634633746074317682114.56", 2}, // overflow после масштабирования
	}
	for _, c := range bad {
		_, err := ParseAmount(c.in, c.scale)
		require.Error(t, err, c.in)
	}
}

func TestAmountRoundTrip(t *testing.T) {
	for _, s := range []string{"0.00", "1.00", "12.34", "999999.99"} {
		u, err := ParseAmount(s, 2)
		require.NoError(t, err)
		require.Equal(t, s, FormatAmount(u, 2))
	}
}

func TestFormatAmountScaleZero(t *testing.T) {
	u, err := ParseAmount("42", 0)
	require.NoError(t, err)
	require.Equal(t, "42", FormatAmount(u, 0))
}
```

`internal/model/id_test.go`:

```go
package model

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIDRoundTrip(t *testing.T) {
	const s = "0193f8a1-7c2e-7000-8000-000000000001"
	u, err := ParseID(s)
	require.NoError(t, err)
	require.Equal(t, s, FormatID(u))
}

func TestParseIDRejects(t *testing.T) {
	for _, s := range []string{"", "not-a-uuid", "0193f8a1-7c2e-7000-8000", "00000000-0000-0000-0000-000000000000"} {
		_, err := ParseID(s)
		require.Error(t, err, s)
	}
}
```

- [ ] **Step 4: Убедиться, что падают**

Run: `go test ./internal/model/ -v`
Expected: FAIL — `undefined: ParseAmount`, `undefined: ParseID`.

- [ ] **Step 5: Реализовать id и суммы**

`internal/model/id.go`:

```go
package model

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
	types "github.com/tigerbeetle/tigerbeetle-go"
)

var ErrZeroID = errors.New("id must not be zero")

// ParseID переводит UUID-строку в Uint128. Байты UUID кладутся как есть,
// обратное преобразование даёт ту же строку.
func ParseID(s string) (types.Uint128, error) {
	u, err := uuid.Parse(s)
	if err != nil {
		return types.Uint128{}, fmt.Errorf("parse id %q: %w", s, err)
	}
	if u == uuid.Nil {
		return types.Uint128{}, ErrZeroID
	}
	var b [16]byte
	copy(b[:], u[:])
	return types.BytesToUint128(b), nil
}

func FormatID(u types.Uint128) string {
	b := u.Bytes()
	var id uuid.UUID
	copy(id[:], b[:])
	return id.String()
}
```

`internal/model/amount.go`:

```go
package model

import (
	"fmt"
	"math/big"
	"strings"

	types "github.com/tigerbeetle/tigerbeetle-go"
)

// maxU128 = 2^128 - 1
var maxU128 = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 128), big.NewInt(1))

var pow10 [19]*big.Int

func init() {
	for i := range pow10 {
		pow10[i] = new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(i)), nil)
	}
}

// ParseAmount переводит десятичную строку в минорные единицы по масштабу ledger'а.
// Float не используется намеренно: округление недопустимо.
func ParseAmount(s string, scale int32) (types.Uint128, error) {
	if scale < 0 || int(scale) >= len(pow10) {
		return types.Uint128{}, fmt.Errorf("amount %q: unsupported scale %d", s, scale)
	}
	if s == "" {
		return types.Uint128{}, fmt.Errorf("amount: empty")
	}
	intPart, fracPart, hasDot := strings.Cut(s, ".")
	if intPart == "" || (hasDot && fracPart == "") {
		return types.Uint128{}, fmt.Errorf("amount %q: malformed", s)
	}
	if !isDigits(intPart) || (hasDot && !isDigits(fracPart)) {
		return types.Uint128{}, fmt.Errorf("amount %q: only digits and one dot allowed", s)
	}
	if int32(len(fracPart)) > scale {
		return types.Uint128{}, fmt.Errorf("amount %q: has %d decimals, scale is %d", s, len(fracPart), scale)
	}
	digits := intPart + fracPart + strings.Repeat("0", int(scale)-len(fracPart))
	bi, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		return types.Uint128{}, fmt.Errorf("amount %q: not a number", s)
	}
	if bi.Cmp(maxU128) > 0 {
		return types.Uint128{}, fmt.Errorf("amount %q: exceeds uint128", s)
	}
	return types.BigIntToUint128(*bi), nil
}

func FormatAmount(u types.Uint128, scale int32) string {
	bi := u.BigInt()
	if scale == 0 {
		return bi.String()
	}
	q, r := new(big.Int).QuoRem(&bi, pow10[scale], new(big.Int))
	return fmt.Sprintf("%s.%0*s", q.String(), scale, r.String())
}

func isDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return len(s) > 0
}
```

- [ ] **Step 6: Прогнать**

Run: `go test ./internal/model/ -v`
Expected: PASS.

- [ ] **Step 7: Реестр справочников и типы команд**

`internal/model/registry.go`:

```go
package model

import (
	"fmt"

	"github.com/Mi7teR/kafka-tb/internal/config"
	types "github.com/tigerbeetle/tigerbeetle-go"
)

type Registry struct {
	ledgers     map[string]config.Ledger
	ledgerNames map[uint32]string
	codes       map[string]uint16
	codeNames   map[uint16]string
}

func NewRegistry(cfg *config.Config) *Registry {
	r := &Registry{
		ledgers:     cfg.Ledgers,
		ledgerNames: make(map[uint32]string, len(cfg.Ledgers)),
		codes:       cfg.Codes,
		codeNames:   make(map[uint16]string, len(cfg.Codes)),
	}
	for name, l := range cfg.Ledgers {
		r.ledgerNames[l.ID] = name
	}
	for name, v := range cfg.Codes {
		r.codeNames[v] = name
	}
	return r
}

func (r *Registry) Ledger(name string) (config.Ledger, error) {
	l, ok := r.ledgers[name]
	if !ok {
		return config.Ledger{}, fmt.Errorf("unknown ledger %q", name)
	}
	return l, nil
}

func (r *Registry) LedgerName(id uint32) (string, error) {
	n, ok := r.ledgerNames[id]
	if !ok {
		return "", fmt.Errorf("unknown ledger id %d", id)
	}
	return n, nil
}

func (r *Registry) ScaleByLedgerID(id uint32) (int32, error) {
	name, err := r.LedgerName(id)
	if err != nil {
		return 0, err
	}
	return r.ledgers[name].Scale, nil
}

func (r *Registry) Code(name string) (uint16, error) {
	v, ok := r.codes[name]
	if !ok {
		return 0, fmt.Errorf("unknown code %q", name)
	}
	return v, nil
}

func (r *Registry) CodeName(v uint16) (string, error) {
	n, ok := r.codeNames[v]
	if !ok {
		return "", fmt.Errorf("unknown code %d", v)
	}
	return n, nil
}

func (r *Registry) TransferFlags(names []string) (types.TransferFlags, error) {
	var f types.TransferFlags
	for _, n := range names {
		switch n {
		case "linked":
			f.Linked = true
		case "pending":
			f.Pending = true
		case "post_pending_transfer":
			f.PostPendingTransfer = true
		case "void_pending_transfer":
			f.VoidPendingTransfer = true
		case "balancing_debit":
			f.BalancingDebit = true
		case "balancing_credit":
			f.BalancingCredit = true
		case "closing_debit":
			f.ClosingDebit = true
		case "closing_credit":
			f.ClosingCredit = true
		default:
			return f, fmt.Errorf("unknown transfer flag %q", n)
		}
	}
	return f, nil
}

func (r *Registry) AccountFlags(names []string) (types.AccountFlags, error) {
	var f types.AccountFlags
	for _, n := range names {
		switch n {
		case "linked":
			f.Linked = true
		case "debits_must_not_exceed_credits":
			f.DebitsMustNotExceedCredits = true
		case "credits_must_not_exceed_debits":
			f.CreditsMustNotExceedDebits = true
		case "history":
			f.History = true
		case "closed":
			f.Closed = true
		default:
			return f, fmt.Errorf("unknown account flag %q", n)
		}
	}
	return f, nil
}
```

`internal/model/command.go`:

```go
package model

import types "github.com/tigerbeetle/tigerbeetle-go"

type Op string

const (
	OpCreateTransfers Op = "create_transfers"
	OpCreateAccounts  Op = "create_accounts"
)

// Command — результат декодинга одного сообщения или одного API-вызова.
// Заполнено ровно одно из полей Transfers/Accounts, согласно Op.
type Command struct {
	Op        Op
	Transfers []types.Transfer
	Accounts  []types.Account
	// IDs хранит исходные строковые id в том же порядке — нужны для отчёта
	// об исходах без обратной конверсии.
	IDs []string
}

func (c *Command) Len() int {
	if c.Op == OpCreateAccounts {
		return len(c.Accounts)
	}
	return len(c.Transfers)
}
```

- [ ] **Step 8: Фаззинг и бенчмарки**

`internal/model/amount_fuzz_test.go`:

```go
package model

import "testing"

func FuzzParseAmount(f *testing.F) {
	for _, s := range []string{"0", "12.34", "abc", "", "-1", "1.", ".1", "999999999999999999999999999999999999999999"} {
		f.Add(s, int32(2))
	}
	f.Fuzz(func(t *testing.T, s string, scale int32) {
		if scale < 0 || scale > 18 {
			return
		}
		u, err := ParseAmount(s, scale)
		if err != nil {
			return
		}
		// Успешный парс обязан переживать round-trip.
		if got := FormatAmount(u, scale); got == "" {
			t.Fatalf("empty format for %q", s)
		}
	})
}
```

`internal/model/bench_test.go`:

```go
package model

import "testing"

func BenchmarkParseAmount(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := ParseAmount("12345.67", 2); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFormatAmount(b *testing.B) {
	u, _ := ParseAmount("12345.67", 2)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = FormatAmount(u, 2)
	}
}

func BenchmarkParseID(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := ParseID("0193f8a1-7c2e-7000-8000-000000000001"); err != nil {
			b.Fatal(err)
		}
	}
}
```

- [ ] **Step 9: Прогнать всё**

Run: `go test ./internal/model/ -race -count=1 && go test ./internal/model/ -run=^$ -bench=. -benchmem && go test ./internal/model/ -run=FuzzParseAmount -fuzz=FuzzParseAmount -fuzztime=30s`
Expected: PASS, бенчи выводят ns/op и allocs/op, фаззинг завершается без падений.

- [ ] **Step 10: Коммит**

```bash
git add internal/model
git commit -m "feat(model): add uint128 ids, decimal amounts and ledger registry"
```

---

### Task 3: Кодек — интерфейс Decoder и JSON-реализация

**Files:**
- Create: `internal/codec/codec.go`, `internal/codec/errors.go`, `internal/codec/jsonc/decoder.go`
- Test: `internal/codec/jsonc/decoder_test.go`, `internal/codec/jsonc/fuzz_test.go`, `internal/codec/jsonc/bench_test.go`

**Interfaces:**
- Consumes: `model.Registry`, `model.ParseID`, `model.ParseAmount`, `config.Limits`.
- Produces:
  - `codec.Decoder` — интерфейс `Decode(payload []byte) (*model.Command, error)`.
  - `codec.PoisonError{Detail string}` с методом `Error() string`; хелпер `codec.Poison(format string, args ...any) error`; `codec.IsPoison(err error) bool`.
  - `codec.Registry` — `map[string]Decoder` по имени топика, метод `(Registry).For(topic string) (Decoder, error)`, конструктор `codec.NewRegistry(topics []config.Topic, build func(codec string) (Decoder, error)) (Registry, error)`.
  - `jsonc.New(reg *model.Registry, lim config.Limits) *Decoder`.

- [ ] **Step 1: Написать падающие тесты декодера**

`internal/codec/jsonc/decoder_test.go`:

```go
package jsonc

import (
	"strings"
	"testing"

	"github.com/Mi7teR/kafka-tb/internal/codec"
	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/stretchr/testify/require"
)

func newDecoder(t *testing.T) *Decoder {
	t.Helper()
	cfg := &config.Config{
		Ledgers: map[string]config.Ledger{"USD": {ID: 1, Scale: 2}},
		Codes:   map[string]uint16{"payment": 1},
		Limits:  config.Limits{MaxMessageBytes: 1 << 20, MaxEventsPerMessage: 8189, MaxJSONDepth: 32},
	}
	return New(model.NewRegistry(cfg), cfg.Limits)
}

const okTransfers = `{
  "operation": "create_transfers",
  "transfers": [
    {"id":"0193f8a1-7c2e-7000-8000-000000000001",
     "debit_account_id":"0193f8a1-0000-7000-8000-000000000010",
     "credit_account_id":"0193f8a1-0000-7000-8000-000000000020",
     "amount":"12.34","ledger":"USD","code":"payment","flags":["linked"]},
    {"id":"0193f8a1-7c2e-7000-8000-000000000002",
     "debit_account_id":"0193f8a1-0000-7000-8000-000000000020",
     "credit_account_id":"0193f8a1-0000-7000-8000-000000000030",
     "amount":"12.34","ledger":"USD","code":"payment","flags":[]}
  ]}`

func TestDecodeTransfers(t *testing.T) {
	cmd, err := newDecoder(t).Decode([]byte(okTransfers))
	require.NoError(t, err)
	require.Equal(t, model.OpCreateTransfers, cmd.Op)
	require.Len(t, cmd.Transfers, 2)
	require.Equal(t, uint32(1), cmd.Transfers[0].Ledger)
	require.Equal(t, uint16(1), cmd.Transfers[0].Code)
	require.Equal(t, "1234", cmd.Transfers[0].Amount.BigInt().String())
	require.Equal(t, []string{
		"0193f8a1-7c2e-7000-8000-000000000001",
		"0193f8a1-7c2e-7000-8000-000000000002",
	}, cmd.IDs)
}

// Флаг linked на последнем элементе сообщения снимается: TigerBeetle
// запрещает открытую цепочку на границе батча.
func TestDecodeClearsTrailingLinked(t *testing.T) {
	body := strings.Replace(okTransfers, `"flags":[]`, `"flags":["linked"]`, 1)
	cmd, err := newDecoder(t).Decode([]byte(body))
	require.NoError(t, err)
	last := cmd.Transfers[len(cmd.Transfers)-1]
	require.Zero(t, last.Flags&uint16(1), "trailing linked flag must be cleared")
}

func TestDecodeAccounts(t *testing.T) {
	body := `{"operation":"create_accounts","accounts":[
	  {"id":"0193f8a1-0000-7000-8000-000000000010","ledger":"USD","code":"payment","flags":["history"]}]}`
	cmd, err := newDecoder(t).Decode([]byte(body))
	require.NoError(t, err)
	require.Equal(t, model.OpCreateAccounts, cmd.Op)
	require.Len(t, cmd.Accounts, 1)
}

func TestDecodePoison(t *testing.T) {
	cases := map[string]string{
		"not json":          `{`,
		"unknown field":     strings.Replace(okTransfers, `"amount"`, `"amont"`, 1),
		"bad amount":        strings.Replace(okTransfers, `"12.34"`, `"12.345"`, 1),
		"bad uuid":          strings.Replace(okTransfers, `"0193f8a1-7c2e-7000-8000-000000000001"`, `"nope"`, 1),
		"unknown ledger":    strings.Replace(okTransfers, `"USD"`, `"XXX"`, 1),
		"unknown code":      strings.Replace(okTransfers, `"payment"`, `"nope"`, 1),
		"unknown flag":      strings.Replace(okTransfers, `"linked"`, `"teleport"`, 1),
		"empty array":       `{"operation":"create_transfers","transfers":[]}`,
		"unknown operation": `{"operation":"drop_database","transfers":[]}`,
		"mixed operations":  `{"operation":"create_transfers","transfers":[],"accounts":[]}`,
		"zero id":           strings.Replace(okTransfers, `"0193f8a1-7c2e-7000-8000-000000000001"`, `"00000000-0000-0000-0000-000000000000"`, 1),
	}
	d := newDecoder(t)
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := d.Decode([]byte(body))
			require.Error(t, err)
			require.True(t, codec.IsPoison(err), "want poison, got %v", err)
		})
	}
}

func TestDecodeRejectsOversizedPayload(t *testing.T) {
	cfg := config.Limits{MaxMessageBytes: 16, MaxEventsPerMessage: 8189, MaxJSONDepth: 32}
	d := New(model.NewRegistry(&config.Config{
		Ledgers: map[string]config.Ledger{"USD": {ID: 1, Scale: 2}},
		Codes:   map[string]uint16{"payment": 1},
	}), cfg)
	_, err := d.Decode([]byte(okTransfers))
	require.True(t, codec.IsPoison(err))
	require.ErrorContains(t, err, "message too large")
}

func TestDecodeRejectsTooManyEvents(t *testing.T) {
	cfg := config.Limits{MaxMessageBytes: 1 << 20, MaxEventsPerMessage: 1, MaxJSONDepth: 32}
	d := New(model.NewRegistry(&config.Config{
		Ledgers: map[string]config.Ledger{"USD": {ID: 1, Scale: 2}},
		Codes:   map[string]uint16{"payment": 1},
	}), cfg)
	_, err := d.Decode([]byte(okTransfers))
	require.True(t, codec.IsPoison(err))
	require.ErrorContains(t, err, "too many events")
}
```

- [ ] **Step 2: Убедиться, что падают**

Run: `go test ./internal/codec/... -v`
Expected: FAIL — пакет не собирается, `undefined: New`.

- [ ] **Step 3: Реализовать интерфейс и ошибки**

`internal/codec/codec.go`:

```go
package codec

import (
	"fmt"

	"github.com/Mi7teR/kafka-tb/internal/model"
)

// Decoder превращает сырой payload сообщения в команду.
// Любая ошибка декодинга — poison: ретрай её не исправит.
type Decoder interface {
	Decode(payload []byte) (*model.Command, error)
}

// DecoderFor выбирает декодер по имени топика.
type Registry map[string]Decoder

func (r Registry) For(topic string) (Decoder, error) {
	d, ok := r[topic]
	if !ok {
		return nil, fmt.Errorf("no decoder registered for topic %q", topic)
	}
	return d, nil
}
```

`internal/codec/errors.go`:

```go
package codec

import (
	"errors"
	"fmt"
)

// PoisonError — данные некорректны. Ретрай бессмысленен, сообщение идёт в DLQ.
type PoisonError struct {
	Detail string
}

func (e *PoisonError) Error() string { return e.Detail }

func Poison(format string, args ...any) error {
	return &PoisonError{Detail: fmt.Sprintf(format, args...)}
}

func IsPoison(err error) bool {
	var p *PoisonError
	return errors.As(err, &p)
}
```

- [ ] **Step 4: Реализовать JSON-декодер**

`internal/codec/jsonc/decoder.go`:

```go
package jsonc

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/Mi7teR/kafka-tb/internal/codec"
	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/model"
	types "github.com/tigerbeetle/tigerbeetle-go"
)

type Decoder struct {
	reg *model.Registry
	lim config.Limits
}

func New(reg *model.Registry, lim config.Limits) *Decoder {
	return &Decoder{reg: reg, lim: lim}
}

type message struct {
	Operation string        `json:"operation"`
	Transfers []jsonTransfer `json:"transfers"`
	Accounts  []jsonAccount  `json:"accounts"`
}

type jsonTransfer struct {
	ID              string   `json:"id"`
	DebitAccountID  string   `json:"debit_account_id"`
	CreditAccountID string   `json:"credit_account_id"`
	PendingID       string   `json:"pending_id"`
	Amount          string   `json:"amount"`
	Ledger          string   `json:"ledger"`
	Code            string   `json:"code"`
	Flags           []string `json:"flags"`
	UserData128     string   `json:"user_data_128"`
	UserData64      uint64   `json:"user_data_64"`
	UserData32      uint32   `json:"user_data_32"`
	Timeout         string   `json:"timeout"`
}

type jsonAccount struct {
	ID          string   `json:"id"`
	Ledger      string   `json:"ledger"`
	Code        string   `json:"code"`
	Flags       []string `json:"flags"`
	UserData128 string   `json:"user_data_128"`
	UserData64  uint64   `json:"user_data_64"`
	UserData32  uint32   `json:"user_data_32"`
}

func (d *Decoder) Decode(payload []byte) (cmd *model.Command, err error) {
	// Паника в парсере не должна ронять процесс: превращаем её в poison.
	defer func() {
		if r := recover(); r != nil {
			cmd, err = nil, codec.Poison("panic while decoding: %v", r)
		}
	}()

	if len(payload) > d.lim.MaxMessageBytes {
		return nil, codec.Poison("message too large: %d bytes, limit %d", len(payload), d.lim.MaxMessageBytes)
	}
	if err := checkDepth(payload, d.lim.MaxJSONDepth); err != nil {
		return nil, err
	}

	var m message
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil {
		return nil, codec.Poison("json: %v", err)
	}
	if dec.More() {
		return nil, codec.Poison("json: trailing data after first value")
	}

	switch model.Op(m.Operation) {
	case model.OpCreateTransfers:
		if len(m.Accounts) > 0 {
			return nil, codec.Poison("operation create_transfers must not carry accounts")
		}
		return d.decodeTransfers(m.Transfers)
	case model.OpCreateAccounts:
		if len(m.Transfers) > 0 {
			return nil, codec.Poison("operation create_accounts must not carry transfers")
		}
		return d.decodeAccounts(m.Accounts)
	default:
		return nil, codec.Poison("unknown operation %q", m.Operation)
	}
}

func (d *Decoder) decodeTransfers(in []jsonTransfer) (*model.Command, error) {
	if len(in) == 0 {
		return nil, codec.Poison("transfers: empty")
	}
	if len(in) > d.lim.MaxEventsPerMessage {
		return nil, codec.Poison("too many events: %d, limit %d", len(in), d.lim.MaxEventsPerMessage)
	}
	cmd := &model.Command{
		Op:        model.OpCreateTransfers,
		Transfers: make([]types.Transfer, len(in)),
		IDs:       make([]string, len(in)),
	}
	for i, jt := range in {
		t, err := d.transfer(jt)
		if err != nil {
			return nil, codec.Poison("transfers[%d]: %v", i, err)
		}
		cmd.Transfers[i] = t
		cmd.IDs[i] = jt.ID
	}
	// Цепочка не может оставаться открытой на границе батча.
	clearLinked(&cmd.Transfers[len(cmd.Transfers)-1].Flags)
	return cmd, nil
}

func (d *Decoder) transfer(jt jsonTransfer) (types.Transfer, error) {
	var t types.Transfer
	var err error
	if t.ID, err = model.ParseID(jt.ID); err != nil {
		return t, err
	}
	flags, err := d.reg.TransferFlags(jt.Flags)
	if err != nil {
		return t, err
	}
	t.Flags = flags.ToUint16()

	postOrVoid := flags.PostPendingTransfer || flags.VoidPendingTransfer
	if postOrVoid {
		if jt.PendingID == "" {
			return t, fmt.Errorf("pending_id required for post/void")
		}
		if t.PendingID, err = model.ParseID(jt.PendingID); err != nil {
			return t, err
		}
	} else {
		if jt.PendingID != "" {
			return t, fmt.Errorf("pending_id only valid with post/void flags")
		}
		if t.DebitAccountID, err = model.ParseID(jt.DebitAccountID); err != nil {
			return t, err
		}
		if t.CreditAccountID, err = model.ParseID(jt.CreditAccountID); err != nil {
			return t, err
		}
	}

	ledger, err := d.reg.Ledger(jt.Ledger)
	if err != nil {
		return t, err
	}
	t.Ledger = ledger.ID
	if t.Code, err = d.reg.Code(jt.Code); err != nil {
		return t, err
	}
	if t.Amount, err = model.ParseAmount(jt.Amount, ledger.Scale); err != nil {
		return t, err
	}
	if jt.UserData128 != "" {
		if t.UserData128, err = model.ParseID(jt.UserData128); err != nil {
			return t, fmt.Errorf("user_data_128: %w", err)
		}
	}
	t.UserData64 = jt.UserData64
	t.UserData32 = jt.UserData32
	if jt.Timeout != "" {
		secs, err := parseTimeoutSeconds(jt.Timeout)
		if err != nil {
			return t, err
		}
		t.Timeout = secs
	}
	return t, nil
}

func (d *Decoder) decodeAccounts(in []jsonAccount) (*model.Command, error) {
	if len(in) == 0 {
		return nil, codec.Poison("accounts: empty")
	}
	if len(in) > d.lim.MaxEventsPerMessage {
		return nil, codec.Poison("too many events: %d, limit %d", len(in), d.lim.MaxEventsPerMessage)
	}
	cmd := &model.Command{
		Op:       model.OpCreateAccounts,
		Accounts: make([]types.Account, len(in)),
		IDs:      make([]string, len(in)),
	}
	for i, ja := range in {
		var a types.Account
		var err error
		if a.ID, err = model.ParseID(ja.ID); err != nil {
			return nil, codec.Poison("accounts[%d]: %v", i, err)
		}
		ledger, err := d.reg.Ledger(ja.Ledger)
		if err != nil {
			return nil, codec.Poison("accounts[%d]: %v", i, err)
		}
		a.Ledger = ledger.ID
		if a.Code, err = d.reg.Code(ja.Code); err != nil {
			return nil, codec.Poison("accounts[%d]: %v", i, err)
		}
		flags, err := d.reg.AccountFlags(ja.Flags)
		if err != nil {
			return nil, codec.Poison("accounts[%d]: %v", i, err)
		}
		a.Flags = flags.ToUint16()
		if ja.UserData128 != "" {
			if a.UserData128, err = model.ParseID(ja.UserData128); err != nil {
				return nil, codec.Poison("accounts[%d].user_data_128: %v", i, err)
			}
		}
		a.UserData64 = ja.UserData64
		a.UserData32 = ja.UserData32
		cmd.Accounts[i] = a
		cmd.IDs[i] = ja.ID
	}
	clearLinked(&cmd.Accounts[len(cmd.Accounts)-1].Flags)
	return cmd, nil
}

// linked — младший бит и у TransferFlags, и у AccountFlags.
const linkedBit uint16 = 1

func clearLinked(flags *uint16) { *flags &^= linkedBit }

func parseTimeoutSeconds(s string) (uint32, error) {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("timeout %q: %w", s, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("timeout %q: must be positive", s)
	}
	secs := int64(d / time.Second)
	if secs == 0 || secs > int64(^uint32(0)) {
		return 0, fmt.Errorf("timeout %q: out of range for uint32 seconds", s)
	}
	return uint32(secs), nil
}

// checkDepth считает вложенность до полноценного парсинга, чтобы
// глубоко вложенный мусор не съел стек.
func checkDepth(payload []byte, max int) error {
	depth, maxSeen, inStr, esc := 0, 0, false, false
	for _, b := range payload {
		switch {
		case esc:
			esc = false
		case inStr && b == '\\':
			esc = true
		case b == '"':
			inStr = !inStr
		case inStr:
		case b == '{' || b == '[':
			depth++
			if depth > maxSeen {
				maxSeen = depth
			}
			if maxSeen > max {
				return codec.Poison("json nesting too deep: limit %d", max)
			}
		case b == '}' || b == ']':
			depth--
		}
	}
	return nil
}
```

Добавь `"time"` в импорты файла.

- [ ] **Step 5: Реализовать сборку реестра декодеров**

Дописать в `internal/codec/codec.go`:

```go
// NewRegistry строит декодеры для всех топиков из конфига.
// Пока поддержан только json; другой codec отклоняется валидацией конфига.
func NewRegistry(topics []config.Topic, build func(name string) (Decoder, error)) (Registry, error) {
	r := make(Registry, len(topics))
	for _, t := range topics {
		d, err := build(t.Codec)
		if err != nil {
			return nil, fmt.Errorf("topic %s: %w", t.Name, err)
		}
		r[t.Name] = d
	}
	return r, nil
}
```

Добавь импорт `"github.com/Mi7teR/kafka-tb/internal/config"`.

- [ ] **Step 6: Прогнать тесты**

Run: `go test ./internal/codec/... -race -count=1 -v`
Expected: PASS, включая все подтесты `TestDecodePoison`.

- [ ] **Step 7: Фаззинг и бенчмарк**

`internal/codec/jsonc/fuzz_test.go`:

```go
package jsonc

import (
	"testing"

	"github.com/Mi7teR/kafka-tb/internal/codec"
)

// Инвариант: декодер либо возвращает команду, либо PoisonError.
// Никаких паник и никаких других классов ошибок.
func FuzzDecode(f *testing.F) {
	f.Add([]byte(okTransfers))
	f.Add([]byte(`{"operation":"create_accounts","accounts":[]}`))
	f.Add([]byte(`{`))
	f.Add([]byte(``))
	f.Fuzz(func(t *testing.T, payload []byte) {
		d := newFuzzDecoder()
		cmd, err := d.Decode(payload)
		if err != nil {
			if !codec.IsPoison(err) {
				t.Fatalf("non-poison error: %v", err)
			}
			return
		}
		if cmd.Len() == 0 {
			t.Fatal("decoded empty command")
		}
	})
}
```

Вынеси построение декодера в хелпер `newFuzzDecoder()` без `*testing.T` и переиспользуй его в `newDecoder`.

`internal/codec/jsonc/bench_test.go`:

```go
package jsonc

import (
	"fmt"
	"strings"
	"testing"
)

func benchPayload(n int) []byte {
	var sb strings.Builder
	sb.WriteString(`{"operation":"create_transfers","transfers":[`)
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `{"id":"0193f8a1-7c2e-7000-8000-%012d",`+
			`"debit_account_id":"0193f8a1-0000-7000-8000-000000000010",`+
			`"credit_account_id":"0193f8a1-0000-7000-8000-000000000020",`+
			`"amount":"12.34","ledger":"USD","code":"payment","flags":[]}`, i+1)
	}
	sb.WriteString(`]}`)
	return []byte(sb.String())
}

func BenchmarkDecodeJSON(b *testing.B) {
	for _, n := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			d := newFuzzDecoder()
			p := benchPayload(n)
			b.SetBytes(int64(len(p)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := d.Decode(p); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
```

- [ ] **Step 8: Прогнать фаззинг и бенч**

Run: `go test ./internal/codec/jsonc/ -run=FuzzDecode -fuzz=FuzzDecode -fuzztime=60s && go test ./internal/codec/jsonc/ -run=^$ -bench=. -benchmem`
Expected: фаззинг завершается без находок; бенчмарк печатает три строки с ns/op и allocs/op.

- [ ] **Step 9: Коммит**

```bash
git add internal/codec
git commit -m "feat(codec): add decoder interface and strict json decoder"
```

---

### Task 4: tbx — классификация результатов TigerBeetle

**Files:**
- Create: `internal/tbx/outcome.go`, `internal/tbx/client.go`
- Test: `internal/tbx/outcome_test.go`, `internal/tbx/bench_test.go`

**Interfaces:**
- Consumes: `model.Command`.
- Produces:
  - `tbx.Status` — `StatusOK`, `StatusRejected`; тип `tbx.Outcome{Index int; ID string; Status Status; Error string; Timestamp uint64}`.
  - `tbx.MapTransferResults(cmd *model.Command, results []types.CreateTransferResult, offset int) ([]Outcome, error)`.
  - `tbx.MapAccountResults(cmd *model.Command, results []types.CreateAccountResult, offset int) ([]Outcome, error)`.
  - `tbx.ErrResultCountMismatch` — ответ TigerBeetle не совпал по длине с отправленным батчем.
  - `tbx.Client` — интерфейс `CreateTransfers([]types.Transfer) ([]types.CreateTransferResult, error)`, `CreateAccounts([]types.Account) ([]types.CreateAccountResult, error)`, `LookupAccounts([]types.Uint128) ([]types.Account, error)`, `LookupTransfers([]types.Uint128) ([]types.Transfer, error)`, `GetAccountTransfers(types.AccountFilter) ([]types.Transfer, error)`, `GetAccountBalances(types.AccountFilter) ([]types.AccountBalance, error)`, `QueryAccounts(types.QueryFilter) ([]types.Account, error)`, `QueryTransfers(types.QueryFilter) ([]types.Transfer, error)`, `Close()`.
  - `tbx.NewClient(cfg config.TigerBeetle) (Client, error)`.

- [ ] **Step 1: Сверить контракт результатов**

Run: `go doc github.com/tigerbeetle/tigerbeetle-go CreateTransferResult && go doc github.com/tigerbeetle/tigerbeetle-go CreateTransferStatus | head -5`

Ожидается (уже проверено на v0.17.9, шаг нужен для подтверждения):

```go
type CreateTransferResult struct {
	Timestamp uint64
	Status    CreateTransferStatus
	Reserved  uint32
}
const TransferCreated CreateTransferStatus = 0xFFFFFFFF
```

Ключевое: поля `Index` нет. Массив результатов **плотный и позиционный** — `results[i]` относится к `transfers[i]`, длина совпадает с длиной запроса. Успех — `TransferCreated` или `TransferExists`; для счетов `AccountCreated`, `AccountExists`.

Открытый вопрос, который закроет интеграционный тест (Task 9): возвращает ли TigerBeetle пустой массив, когда **все** события успешны (в клиенте есть ветка `if reply == nil { return make([]CreateTransferResult, 0), nil }`). Код ниже обрабатывает оба случая: пустой ответ = всё успешно, ответ длиной с батч = позиционный разбор, любая другая длина = `ErrResultCountMismatch`.

- [ ] **Step 2: Написать падающий тест маппинга**

`internal/tbx/outcome_test.go`:

```go
package tbx

import (
	"testing"

	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/stretchr/testify/require"
	types "github.com/tigerbeetle/tigerbeetle-go"
)

func cmd3() *model.Command {
	return &model.Command{
		Op:        model.OpCreateTransfers,
		Transfers: make([]types.Transfer, 3),
		IDs:       []string{"id-0", "id-1", "id-2"},
	}
}

// Ответ плотный: results[i] относится к событию i батча.
func TestMapTransferResultsPositional(t *testing.T) {
	got, err := MapTransferResults(cmd3(), []types.CreateTransferResult{
		{Status: types.TransferCreated, Timestamp: 100},
		{Status: types.TransferExceedsCredits},
		{Status: types.TransferCreated, Timestamp: 102},
	}, 0)
	require.NoError(t, err)
	require.Len(t, got, 3)
	require.Equal(t, StatusOK, got[0].Status)
	require.Equal(t, uint64(100), got[0].Timestamp)
	require.Equal(t, StatusRejected, got[1].Status)
	require.Equal(t, "exceeds_credits", got[1].Error)
	require.Equal(t, "id-1", got[1].ID)
	require.Equal(t, StatusOK, got[2].Status)
}

// exists — идемпотентный повтор, а не отказ.
func TestMapTransferResultsExistsIsSuccess(t *testing.T) {
	got, err := MapTransferResults(cmd3(), []types.CreateTransferResult{
		{Status: types.TransferExists},
		{Status: types.TransferCreated},
		{Status: types.TransferCreated},
	}, 0)
	require.NoError(t, err)
	require.Equal(t, StatusOK, got[0].Status)
	require.Empty(t, got[0].Error)
}

// Команда занимает окно [offset, offset+Len) внутри общего батча.
func TestMapTransferResultsHonoursOffset(t *testing.T) {
	results := make([]types.CreateTransferResult, 13)
	for i := range results {
		results[i].Status = types.TransferCreated
	}
	results[11].Status = types.TransferExceedsCredits

	got, err := MapTransferResults(cmd3(), results, 10)
	require.NoError(t, err)
	require.Equal(t, StatusOK, got[0].Status)
	require.Equal(t, StatusRejected, got[1].Status)
	require.Equal(t, StatusOK, got[2].Status)
}

// Пустой ответ означает, что успешны все события.
func TestMapTransferResultsEmptyMeansAllOK(t *testing.T) {
	got, err := MapTransferResults(cmd3(), nil, 0)
	require.NoError(t, err)
	require.Len(t, got, 3)
	for _, o := range got {
		require.Equal(t, StatusOK, o.Status)
	}
}

// Ответ не той длины — нарушение контракта: молча разъезжаться нельзя.
func TestMapTransferResultsCountMismatch(t *testing.T) {
	_, err := MapTransferResults(cmd3(), []types.CreateTransferResult{
		{Status: types.TransferCreated},
	}, 0)
	require.ErrorIs(t, err, ErrResultCountMismatch)
}

func TestMapAccountResults(t *testing.T) {
	c := &model.Command{
		Op:       model.OpCreateAccounts,
		Accounts: make([]types.Account, 2),
		IDs:      []string{"a-0", "a-1"},
	}
	got, err := MapAccountResults(c, []types.CreateAccountResult{
		{Status: types.AccountExists},
		{Status: types.AccountLinkedEventFailed},
	}, 0)
	require.NoError(t, err)
	require.Equal(t, StatusOK, got[0].Status)
	require.Equal(t, StatusRejected, got[1].Status)
	require.Equal(t, "linked_event_failed", got[1].Error)
}
```

- [ ] **Step 3: Убедиться, что падают**

Run: `go test ./internal/tbx/ -v`
Expected: FAIL — `undefined: MapTransferResults`.

- [ ] **Step 4: Реализовать маппинг**

`internal/tbx/outcome.go`:

```go
package tbx

import (
	"strings"

	"github.com/Mi7teR/kafka-tb/internal/model"
	types "github.com/tigerbeetle/tigerbeetle-go"
)

type Status string

const (
	StatusOK       Status = "ok"
	StatusRejected Status = "rejected"
)

// ErrResultCountMismatch — TigerBeetle вернул ответ, не соответствующий батчу.
// Разбирать его позиционно нельзя: исходы уедут не тем командам.
var ErrResultCountMismatch = errors.New("tigerbeetle result count does not match batch size")

// Outcome — исход одного события внутри команды.
type Outcome struct {
	Index     int
	ID        string
	Status    Status
	Error     string // машиночитаемое имя статуса TigerBeetle, пусто при успехе
	Timestamp uint64
}

// MapTransferResults вырезает окно команды из плотного ответа батча.
// offset — позиция первого события команды внутри отправленного батча,
// batchSize — сколько событий было отправлено всего.
func MapTransferResults(cmd *model.Command, results []types.CreateTransferResult, offset int) ([]Outcome, error) {
	out := newOutcomes(cmd)
	if len(results) == 0 {
		return out, nil // пустой ответ = все события применены
	}
	if offset+len(out) > len(results) {
		return nil, fmt.Errorf("%w: got %d results, command needs [%d,%d)",
			ErrResultCountMismatch, len(results), offset, offset+len(out))
	}
	for i := range out {
		r := results[offset+i]
		out[i].Timestamp = r.Timestamp
		if r.Status == types.TransferCreated || r.Status == types.TransferExists {
			continue
		}
		out[i].Status = StatusRejected
		out[i].Error = errorName(r.Status.String(), "Transfer")
	}
	return out, nil
}

func MapAccountResults(cmd *model.Command, results []types.CreateAccountResult, offset int) ([]Outcome, error) {
	out := newOutcomes(cmd)
	if len(results) == 0 {
		return out, nil
	}
	if offset+len(out) > len(results) {
		return nil, fmt.Errorf("%w: got %d results, command needs [%d,%d)",
			ErrResultCountMismatch, len(results), offset, offset+len(out))
	}
	for i := range out {
		r := results[offset+i]
		out[i].Timestamp = r.Timestamp
		if r.Status == types.AccountCreated || r.Status == types.AccountExists {
			continue
		}
		out[i].Status = StatusRejected
		out[i].Error = errorName(r.Status.String(), "Account")
	}
	return out, nil
}

func newOutcomes(cmd *model.Command) []Outcome {
	out := make([]Outcome, cmd.Len())
	for i := range out {
		out[i] = Outcome{Index: i, ID: cmd.IDs[i], Status: StatusOK}
	}
	return out
}

// errorName переводит "TransferExceedsCredits" в "exceeds_credits":
// снимает префикс типа и переводит CamelCase в snake_case.
func errorName(s, prefix string) string {
	s = strings.TrimPrefix(s, prefix)
	var sb strings.Builder
	sb.Grow(len(s) + 8)
	for i, r := range s {
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				sb.WriteByte('_')
			}
			sb.WriteRune(r + ('a' - 'A'))
			continue
		}
		sb.WriteRune(r)
	}
	return sb.String()
}
```

Импорты файла: `errors`, `fmt`, `strings`, `model`, `types`.

Проверь фактический вывод `Status.String()`: если он даёт не `"TransferExceedsCredits"`, а что-то иное, поправь `errorName` под реальный формат — тест `TestMapTransferResultsPositional` это поймает.

- [ ] **Step 5: Прогнать**

Run: `go test ./internal/tbx/ -v -race`
Expected: PASS. Если `errorName` дала не `exceeds_credits` — проверь фактический вывод `String()` у константы и поправь `TrimPrefix`.

- [ ] **Step 6: Обёртка клиента**

`internal/tbx/client.go`:

```go
package tbx

import (
	"fmt"

	tb "github.com/tigerbeetle/tigerbeetle-go"
	types "github.com/tigerbeetle/tigerbeetle-go"

	"github.com/Mi7teR/kafka-tb/internal/config"
)

// Client — узкий интерфейс поверх клиента TigerBeetle.
// Существует ради подмены в тестах: настоящий клиент требует живого кластера.
type Client interface {
	CreateAccounts([]types.Account) ([]types.CreateAccountResult, error)
	CreateTransfers([]types.Transfer) ([]types.CreateTransferResult, error)
	LookupAccounts([]types.Uint128) ([]types.Account, error)
	LookupTransfers([]types.Uint128) ([]types.Transfer, error)
	GetAccountTransfers(types.AccountFilter) ([]types.Transfer, error)
	GetAccountBalances(types.AccountFilter) ([]types.AccountBalance, error)
	QueryAccounts(types.QueryFilter) ([]types.Account, error)
	QueryTransfers(types.QueryFilter) ([]types.Transfer, error)
	Nop() error
	Close()
}

func NewClient(cfg config.TigerBeetle) (Client, error) {
	c, err := tb.NewClient(types.ToUint128(cfg.ClusterID), cfg.Addresses)
	if err != nil {
		return nil, fmt.Errorf("tigerbeetle connect: %w", err)
	}
	return c, nil
}
```

- [ ] **Step 7: Бенчмарк маппинга**

`internal/tbx/bench_test.go`:

```go
package tbx

import (
	"strconv"
	"testing"

	"github.com/Mi7teR/kafka-tb/internal/model"
	types "github.com/tigerbeetle/tigerbeetle-go"
)

func BenchmarkMapResults(b *testing.B) {
	const n = 8189
	c := &model.Command{Op: model.OpCreateTransfers, Transfers: make([]types.Transfer, n), IDs: make([]string, n)}
	for i := range c.IDs {
		c.IDs[i] = strconv.Itoa(i)
	}
	res := make([]types.CreateTransferResult, n)
	for i := range res {
		res[i].Status = types.TransferCreated
		if i%100 == 0 {
			res[i].Status = types.TransferExceedsCredits
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := MapTransferResults(c, res, 0); err != nil {
			b.Fatal(err)
		}
	}
}
```

- [ ] **Step 8: Коммит**

```bash
git add internal/tbx
git commit -m "feat(tbx): map tigerbeetle batch results to per-event outcomes"
```

---

### Task 5: tbx.Batcher — упаковка, серийная отправка, разводка результатов

**Files:**
- Create: `internal/tbx/batcher.go`, `internal/tbx/fake_test.go`
- Test: `internal/tbx/batcher_test.go`, `internal/tbx/batcher_prop_test.go`, дополнение `internal/tbx/bench_test.go`

**Interfaces:**
- Consumes: `tbx.Client`, `tbx.MapTransferResults`, `tbx.MapAccountResults`, `config.Batcher`, `config.Retry`, `model.Command`.
- Produces:
  - `tbx.NewBatcher(c Client, cfg config.Batcher, retry config.Retry, log *slog.Logger) *Batcher`.
  - `(*Batcher).Start(ctx context.Context)` — поднимает два цикла отправки.
  - `(*Batcher).Submit(ctx context.Context, cmd *model.Command) ([]Outcome, error)` — блокирует до ответа TigerBeetle; ошибка возвращается только при отмене контекста или закрытии батчера.
  - `(*Batcher).Close()` — дожидается опустошения очередей.
  - `tbx.ErrCommandTooLarge`, `tbx.ErrClosed`.

**Инварианты, ради которых существует этот код:**
1. События одной команды никогда не разрезаются между батчами.
2. Размер батча не превышает `max_batch_size`.
3. Порядок команд в батче совпадает с порядком `Submit`.
4. У последнего события каждой команды снят флаг `linked`.
5. Инфраструктурная ошибка приводит к повтору того же батча, а не к потере команд.

- [ ] **Step 1: Написать фейковый клиент**

`internal/tbx/fake_test.go`:

```go
package tbx

import (
	"sync"

	types "github.com/tigerbeetle/tigerbeetle-go"
)

// fakeClient записывает батчи как они пришли и умеет отдавать
// заданные результаты и инфраструктурные ошибки.
type fakeClient struct {
	mu sync.Mutex

	transferBatches [][]types.Transfer
	accountBatches  [][]types.Account

	// failNext[i] > 0 — вернуть ошибку столько раз подряд, потом успех.
	failTimes int
	err       error

	// resultsFor вызывается на каждый батч трансферов.
	resultsFor func(batch []types.Transfer) []types.CreateTransferResult
}

func (f *fakeClient) CreateTransfers(ts []types.Transfer) ([]types.CreateTransferResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failTimes > 0 {
		f.failTimes--
		return nil, f.err
	}
	cp := make([]types.Transfer, len(ts))
	copy(cp, ts)
	f.transferBatches = append(f.transferBatches, cp)
	if f.resultsFor != nil {
		return f.resultsFor(cp), nil
	}
	return nil, nil
}

func (f *fakeClient) CreateAccounts(as []types.Account) ([]types.CreateAccountResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]types.Account, len(as))
	copy(cp, as)
	f.accountBatches = append(f.accountBatches, cp)
	return nil, nil
}

func (f *fakeClient) batches() [][]types.Transfer {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]types.Transfer, len(f.transferBatches))
	copy(out, f.transferBatches)
	return out
}

func (f *fakeClient) LookupAccounts([]types.Uint128) ([]types.Account, error)   { return nil, nil }
func (f *fakeClient) LookupTransfers([]types.Uint128) ([]types.Transfer, error) { return nil, nil }
func (f *fakeClient) GetAccountTransfers(types.AccountFilter) ([]types.Transfer, error) {
	return nil, nil
}
func (f *fakeClient) GetAccountBalances(types.AccountFilter) ([]types.AccountBalance, error) {
	return nil, nil
}
func (f *fakeClient) QueryAccounts(types.QueryFilter) ([]types.Account, error)   { return nil, nil }
func (f *fakeClient) QueryTransfers(types.QueryFilter) ([]types.Transfer, error) { return nil, nil }
func (f *fakeClient) Nop() error                                                 { return nil }
func (f *fakeClient) Close()                                                     {}
```

- [ ] **Step 2: Написать падающие тесты батчера**

`internal/tbx/batcher_test.go`:

```go
package tbx

import (
	"context"
	"errors"
	"log/slog"
	"io"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/stretchr/testify/require"
	types "github.com/tigerbeetle/tigerbeetle-go"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func transferCmd(n int, tag string) *model.Command {
	c := &model.Command{Op: model.OpCreateTransfers, Transfers: make([]types.Transfer, n), IDs: make([]string, n)}
	for i := 0; i < n; i++ {
		c.Transfers[i] = types.Transfer{ID: types.ToUint128(uint64(i + 1)), Flags: 1} // linked у всех
		c.IDs[i] = tag + "-" + strconv.Itoa(i)
	}
	return c
}

func startBatcher(t *testing.T, fc *fakeClient, maxBatch int, linger time.Duration) (*Batcher, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	b := NewBatcher(fc, config.Batcher{MaxBatchSize: maxBatch, Linger: linger, MaxQueue: 128},
		config.Retry{Initial: time.Millisecond, Max: 10 * time.Millisecond}, testLogger())
	b.Start(ctx)
	t.Cleanup(func() { cancel(); b.Close() })
	return b, cancel
}

func TestBatcherNeverSplitsCommand(t *testing.T) {
	fc := &fakeClient{}
	b, _ := startBatcher(t, fc, 10, 20*time.Millisecond)

	var wg sync.WaitGroup
	for _, n := range []int{6, 6, 6} {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_, err := b.Submit(context.Background(), transferCmd(n, "c"))
			require.NoError(t, err)
		}(n)
	}
	wg.Wait()

	for _, batch := range fc.batches() {
		require.LessOrEqual(t, len(batch), 10)
		require.Zero(t, len(batch)%6, "batch %d is not a whole number of commands", len(batch))
	}
}

func TestBatcherClearsTrailingLinkedPerCommand(t *testing.T) {
	fc := &fakeClient{}
	b, _ := startBatcher(t, fc, 100, 5*time.Millisecond)
	_, err := b.Submit(context.Background(), transferCmd(3, "c"))
	require.NoError(t, err)

	batches := fc.batches()
	require.Len(t, batches, 1)
	last := batches[0][len(batches[0])-1]
	require.Zero(t, last.Flags&1, "trailing linked must be cleared")
	require.NotZero(t, batches[0][0].Flags&1, "inner linked must survive")
}

func TestBatcherRespectsMaxBatchSize(t *testing.T) {
	fc := &fakeClient{}
	b, _ := startBatcher(t, fc, 5, 20*time.Millisecond)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = b.Submit(context.Background(), transferCmd(2, "c")) }()
	}
	wg.Wait()
	for _, batch := range fc.batches() {
		require.LessOrEqual(t, len(batch), 5)
	}
}

func TestBatcherRoutesResultsToOwner(t *testing.T) {
	// Отклоняем каждое событие, у которого Amount == 7: так проверяем,
	// что исход попал именно в ту команду, где это событие лежало.
	fc := &fakeClient{resultsFor: func(batch []types.Transfer) []types.CreateTransferResult {
		out := make([]types.CreateTransferResult, len(batch))
		for i, tr := range batch {
			out[i].Status = types.TransferCreated
			if tr.Amount == types.ToUint128(7) {
				out[i].Status = types.TransferExceedsCredits
			}
		}
		return out
	}}
	b, _ := startBatcher(t, fc, 100, 10*time.Millisecond)

	mark := transferCmd(2, "marked")
	mark.Transfers[1].Amount = types.ToUint128(7)
	plain := transferCmd(2, "plain")

	var wg sync.WaitGroup
	var markOut, plainOut []Outcome
	wg.Add(2)
	go func() { defer wg.Done(); markOut, _ = b.Submit(context.Background(), mark) }()
	go func() { defer wg.Done(); plainOut, _ = b.Submit(context.Background(), plain) }()
	wg.Wait()

	require.Equal(t, StatusOK, markOut[0].Status)
	require.Equal(t, StatusRejected, markOut[1].Status)
	require.Equal(t, "exceeds_credits", markOut[1].Error)
	for _, o := range plainOut {
		require.Equal(t, StatusOK, o.Status)
	}
}

func TestBatcherRetriesInfraError(t *testing.T) {
	fc := &fakeClient{failTimes: 3, err: errors.New("connection refused")}
	b, _ := startBatcher(t, fc, 100, time.Millisecond)
	out, err := b.Submit(context.Background(), transferCmd(1, "c"))
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, StatusOK, out[0].Status)
	require.Len(t, fc.batches(), 1, "successful batch must be sent exactly once")
}

func TestBatcherRejectsOversizedCommand(t *testing.T) {
	fc := &fakeClient{}
	b, _ := startBatcher(t, fc, 4, time.Millisecond)
	_, err := b.Submit(context.Background(), transferCmd(5, "c"))
	require.ErrorIs(t, err, ErrCommandTooLarge)
}

func TestBatcherSubmitAfterCloseFails(t *testing.T) {
	fc := &fakeClient{}
	ctx, cancel := context.WithCancel(context.Background())
	b := NewBatcher(fc, config.Batcher{MaxBatchSize: 10, Linger: time.Millisecond, MaxQueue: 8},
		config.Retry{Initial: time.Millisecond, Max: time.Millisecond}, testLogger())
	b.Start(ctx)
	cancel()
	b.Close()
	_, err := b.Submit(context.Background(), transferCmd(1, "c"))
	require.ErrorIs(t, err, ErrClosed)
}

func TestBatcherAccountsGoToSeparateBatches(t *testing.T) {
	fc := &fakeClient{}
	b, _ := startBatcher(t, fc, 100, 5*time.Millisecond)
	acc := &model.Command{Op: model.OpCreateAccounts, Accounts: make([]types.Account, 2), IDs: []string{"a", "b"}}
	_, err := b.Submit(context.Background(), acc)
	require.NoError(t, err)
	_, err = b.Submit(context.Background(), transferCmd(1, "c"))
	require.NoError(t, err)

	fc.mu.Lock()
	defer fc.mu.Unlock()
	require.Len(t, fc.accountBatches, 1)
	require.Len(t, fc.transferBatches, 1)
}
```

- [ ] **Step 3: Убедиться, что падают**

Run: `go test ./internal/tbx/ -run TestBatcher -v`
Expected: FAIL — `undefined: NewBatcher`.

- [ ] **Step 4: Реализовать батчер**

`internal/tbx/batcher.go`:

```go
package tbx

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/model"
	types "github.com/tigerbeetle/tigerbeetle-go"
)

var (
	// ErrCommandTooLarge — команда не помещается в батч целиком.
	// Разрезать нельзя: атомарность linked-цепочки важнее.
	ErrCommandTooLarge = errors.New("command exceeds max batch size")
	ErrClosed          = errors.New("batcher closed")
)

const linkedBit uint16 = 1

type job struct {
	cmd  *model.Command
	done chan submitResult
}

type submitResult struct {
	outcomes []Outcome
	err      error
}

// Batcher — единственная дверь в TigerBeetle.
// Держит по одному in-flight батчу на каждый тип операции, чем гарантирует,
// что порядок применения совпадает с порядком Submit.
type Batcher struct {
	client Client
	cfg    config.Batcher
	retry  config.Retry
	log    *slog.Logger

	transfers chan *job
	accounts  chan *job

	closeOnce sync.Once
	closed    chan struct{}
	wg        sync.WaitGroup
}

func NewBatcher(c Client, cfg config.Batcher, retry config.Retry, log *slog.Logger) *Batcher {
	return &Batcher{
		client:    c,
		cfg:       cfg,
		retry:     retry,
		log:       log,
		transfers: make(chan *job, cfg.MaxQueue),
		accounts:  make(chan *job, cfg.MaxQueue),
		closed:    make(chan struct{}),
	}
}

func (b *Batcher) Start(ctx context.Context) {
	b.wg.Add(2)
	go func() { defer b.wg.Done(); b.loop(ctx, b.transfers, b.sendTransfers) }()
	go func() { defer b.wg.Done(); b.loop(ctx, b.accounts, b.sendAccounts) }()
}

// Submit ставит команду в очередь и ждёт исход.
// Блокировка при полной очереди — это backpressure для консьюмера.
func (b *Batcher) Submit(ctx context.Context, cmd *model.Command) ([]Outcome, error) {
	if cmd.Len() == 0 {
		return nil, errors.New("empty command")
	}
	if cmd.Len() > b.cfg.MaxBatchSize {
		return nil, ErrCommandTooLarge
	}
	j := &job{cmd: cmd, done: make(chan submitResult, 1)}
	queue := b.transfers
	if cmd.Op == model.OpCreateAccounts {
		queue = b.accounts
	}
	select {
	case <-b.closed:
		return nil, ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	case queue <- j:
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-j.done:
		return res.outcomes, res.err
	}
}

// Close закрывает приём и дожидается, пока циклы разгребут очереди.
func (b *Batcher) Close() {
	b.closeOnce.Do(func() { close(b.closed) })
	b.wg.Wait()
}

// loop собирает батч по правилу «max_batch_size или linger, что раньше».
func (b *Batcher) loop(ctx context.Context, queue chan *job, send func([]*job) error) {
	var (
		batch []*job
		size  int
		timer *time.Timer
		tick  <-chan time.Time
	)
	stopTimer := func() {
		if timer != nil {
			timer.Stop()
			timer, tick = nil, nil
		}
	}
	flush := func() {
		if len(batch) == 0 {
			return
		}
		stopTimer()
		if err := send(batch); err != nil {
			b.failAll(batch, err)
		}
		batch, size = nil, 0
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			b.drain(queue, ctx.Err())
			return
		case <-b.closed:
			flush()
			b.drain(queue, ErrClosed)
			return
		case j := <-queue:
			// Команда не влезает в остаток — отправляем накопленное и начинаем новый батч.
			if size+j.cmd.Len() > b.cfg.MaxBatchSize {
				flush()
			}
			batch = append(batch, j)
			size += j.cmd.Len()
			if size >= b.cfg.MaxBatchSize {
				flush()
				continue
			}
			if timer == nil {
				timer = time.NewTimer(b.cfg.Linger)
				tick = timer.C
			}
		case <-tick:
			flush()
		}
	}
}

func (b *Batcher) drain(queue chan *job, err error) {
	for {
		select {
		case j := <-queue:
			j.done <- submitResult{err: err}
		default:
			return
		}
	}
}

func (b *Batcher) failAll(jobs []*job, err error) {
	for _, j := range jobs {
		j.done <- submitResult{err: err}
	}
}

func (b *Batcher) sendTransfers(jobs []*job) error {
	events := make([]types.Transfer, 0, b.cfg.MaxBatchSize)
	offsets := make([]int, len(jobs))
	for i, j := range jobs {
		offsets[i] = len(events)
		events = append(events, j.cmd.Transfers...)
		// Цепочка не должна оставаться открытой на стыке команд.
		events[len(events)-1].Flags &^= linkedBit
	}

	results, err := b.call(func() (any, error) { return b.client.CreateTransfers(events) })
	if err != nil {
		return err
	}
	typed, _ := results.([]types.CreateTransferResult)
	for i, j := range jobs {
		outcomes, mapErr := MapTransferResults(j.cmd, typed, offsets[i])
		j.done <- submitResult{outcomes: outcomes, err: mapErr}
	}
	return nil
}

func (b *Batcher) sendAccounts(jobs []*job) error {
	events := make([]types.Account, 0, b.cfg.MaxBatchSize)
	offsets := make([]int, len(jobs))
	for i, j := range jobs {
		offsets[i] = len(events)
		events = append(events, j.cmd.Accounts...)
		events[len(events)-1].Flags &^= linkedBit
	}

	results, err := b.call(func() (any, error) { return b.client.CreateAccounts(events) })
	if err != nil {
		return err
	}
	typed, _ := results.([]types.CreateAccountResult)
	for i, j := range jobs {
		outcomes, mapErr := MapAccountResults(j.cmd, typed, offsets[i])
		j.done <- submitResult{outcomes: outcomes, err: mapErr}
	}
	return nil
}

// call повторяет вызов, пока TigerBeetle не ответит или батчер не закроют.
// Ошибка вызова — всегда инфраструктурная: отказ по бизнесу приходит в результатах.
func (b *Batcher) call(fn func() (any, error)) (any, error) {
	delay := b.retry.Initial
	for attempt := 1; ; attempt++ {
		res, err := fn()
		if err == nil {
			return res, nil
		}
		b.log.Warn("tigerbeetle call failed, retrying",
			slog.Int("attempt", attempt), slog.String("error", err.Error()), slog.Duration("in", delay))

		select {
		case <-b.closed:
			return nil, ErrClosed
		case <-time.After(b.jitter(delay)):
		}
		if delay < b.retry.Max {
			delay *= 2
			if delay > b.retry.Max {
				delay = b.retry.Max
			}
		}
	}
}

func (b *Batcher) jitter(d time.Duration) time.Duration {
	if !b.retry.Jitter {
		return d
	}
	return d/2 + time.Duration(rand.Int63n(int64(d/2)+1))
}
```

- [ ] **Step 5: Прогнать тесты батчера**

Run: `go test ./internal/tbx/ -race -count=1 -v`
Expected: PASS, все восемь тестов `TestBatcher*`.

- [ ] **Step 6: Property-тест упаковки**

`internal/tbx/batcher_prop_test.go`:

```go
package tbx

import (
	"context"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Инвариант упаковки: каждый батч состоит из целых команд,
// не превышает лимит, и суммарно уходят все события.
func TestBatcherPackingInvariants(t *testing.T) {
	const maxBatch = 64
	seed := time.Now().UnixNano()
	t.Logf("seed=%d", seed)
	rng := rand.New(rand.NewSource(seed))

	fc := &fakeClient{}
	b, _ := startBatcher(t, fc, maxBatch, 5*time.Millisecond)

	sizes := make([]int, 200)
	total := 0
	for i := range sizes {
		sizes[i] = 1 + rng.Intn(maxBatch)
		total += sizes[i]
	}

	var wg sync.WaitGroup
	for i, n := range sizes {
		wg.Add(1)
		go func(i, n int) {
			defer wg.Done()
			out, err := b.Submit(context.Background(), transferCmd(n, "c"+strconv.Itoa(i)))
			require.NoError(t, err)
			require.Len(t, out, n)
		}(i, n)
	}
	wg.Wait()

	sent := 0
	for _, batch := range fc.batches() {
		require.LessOrEqual(t, len(batch), maxBatch)
		require.NotEmpty(t, batch)
		require.Zero(t, batch[len(batch)-1].Flags&linkedBit, "batch must not end with an open chain")
		sent += len(batch)
	}
	require.Equal(t, total, sent, "every submitted event must reach TigerBeetle exactly once")
}
```

- [ ] **Step 7: Бенчмарк сборки батча**

Дописать в `internal/tbx/bench_test.go`:

```go
func BenchmarkBatcherAssemble(b *testing.B) {
	fc := &fakeClient{}
	bt := NewBatcher(fc, config.Batcher{MaxBatchSize: 8189, Linger: time.Millisecond, MaxQueue: 1024},
		config.Retry{Initial: time.Millisecond, Max: time.Millisecond}, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); bt.Close() }()
	bt.Start(ctx)

	cmd := transferCmdBench(10)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := bt.Submit(ctx, cmd); err != nil {
				b.Fatal(err)
			}
		}
	})
}
```

Добавь `transferCmdBench(n int) *model.Command` — копия `transferCmd` без `*testing.T`, и нужные импорты (`context`, `time`, `config`).

- [ ] **Step 8: Прогнать всё**

Run: `go test ./internal/tbx/ -race -count=1 && go test ./internal/tbx/ -run=^$ -bench=. -benchmem`
Expected: PASS; бенчи печатают `BenchmarkMapResults`, `BenchmarkBatcherAssemble`.

- [ ] **Step 9: Коммит**

```bash
git add internal/tbx
git commit -m "feat(tbx): add serial batcher preserving command atomicity"
```

---

### Task 6: emit — продюсер DLQ и топика результатов

**Files:**
- Create: `internal/emit/emitter.go`, `internal/emit/headers.go`
- Test: `internal/emit/emitter_test.go`

**Interfaces:**
- Consumes: `config.Kafka`, `tbx.Outcome`, `codec.IsPoison`.
- Produces:
  - `emit.Reason` — `ReasonPoison`, `ReasonReject`.
  - `emit.Emitter` — интерфейс `DLQ(ctx context.Context, rec *kgo.Record, reason Reason, errName, detail string) error`, `Results(ctx context.Context, rec *kgo.Record, outcomes []tbx.Outcome) error`, `Flush(ctx context.Context) error`, `Close()`.
  - `emit.New(cl *kgo.Client, cfg config.Kafka) Emitter`.
  - Константы имён хедеров: `emit.HeaderReason`, `HeaderError`, `HeaderDetail`, `HeaderSrcTopic`, `HeaderSrcPartition`, `HeaderSrcOffset`, `HeaderSrcTimestamp`.

- [ ] **Step 1: Подтянуть franz-go**

```bash
go get github.com/twmb/franz-go@latest
go get github.com/twmb/franz-go/pkg/kfake@latest
```

- [ ] **Step 2: Написать падающий тест**

`internal/emit/emitter_test.go`:

```go
package emit

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/tbx"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func newTestEmitter(t *testing.T) (Emitter, *kgo.Client, config.Kafka) {
	t.Helper()
	fake, err := kfake.NewCluster(kfake.NumBrokers(1),
		kfake.SeedTopics(1, "src", "src.dlq", "results"))
	require.NoError(t, err)
	t.Cleanup(fake.Close)

	cl, err := kgo.NewClient(kgo.SeedBrokers(fake.ListenAddrs()...))
	require.NoError(t, err)
	t.Cleanup(cl.Close)

	cfg := config.Kafka{DLQTopic: "src.dlq", ResultsTopic: "results"}
	return New(cl, cfg), cl, cfg
}

func consumeOne(t *testing.T, brokers []string, topic string) *kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic))
	require.NoError(t, err)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fetches := cl.PollRecords(ctx, 1)
	require.NoError(t, fetches.Err())
	recs := fetches.Records()
	require.Len(t, recs, 1)
	return recs[0]
}

func TestDLQPreservesPayloadAndAddsHeaders(t *testing.T) {
	em, cl, _ := newTestEmitter(t)
	src := &kgo.Record{
		Topic: "src", Partition: 3, Offset: 42,
		Key: []byte("k"), Value: []byte(`{"broken":`),
		Timestamp: time.Unix(1700000000, 0),
	}
	require.NoError(t, em.DLQ(context.Background(), src, ReasonPoison, "json", "unexpected end of input"))
	require.NoError(t, em.Flush(context.Background()))

	got := consumeOne(t, cl.OptValue(kgo.SeedBrokers).([]string), "src.dlq")
	require.Equal(t, src.Value, got.Value, "payload must be byte-identical for replay")
	require.Equal(t, src.Key, got.Key)

	h := headerMap(got)
	require.Equal(t, "poison", h[HeaderReason])
	require.Equal(t, "json", h[HeaderError])
	require.Equal(t, "unexpected end of input", h[HeaderDetail])
	require.Equal(t, "src", h[HeaderSrcTopic])
	require.Equal(t, "3", h[HeaderSrcPartition])
	require.Equal(t, "42", h[HeaderSrcOffset])
}

func TestResultsCarryOutcomes(t *testing.T) {
	em, cl, _ := newTestEmitter(t)
	src := &kgo.Record{Topic: "src", Partition: 0, Offset: 7, Key: []byte("k")}
	outcomes := []tbx.Outcome{
		{Index: 0, ID: "id-0", Status: tbx.StatusOK},
		{Index: 1, ID: "id-1", Status: tbx.StatusRejected, Error: "exceeds_credits"},
	}
	require.NoError(t, em.Results(context.Background(), src, outcomes))
	require.NoError(t, em.Flush(context.Background()))

	got := consumeOne(t, cl.OptValue(kgo.SeedBrokers).([]string), "results")
	var payload ResultsMessage
	require.NoError(t, json.Unmarshal(got.Value, &payload))
	require.Equal(t, "src", payload.Source.Topic)
	require.Equal(t, int64(7), payload.Source.Offset)
	require.Len(t, payload.Results, 2)
	require.Equal(t, "rejected", payload.Results[1].Status)
	require.Equal(t, "exceeds_credits", payload.Results[1].Error)
}

func TestResultsDisabledWhenTopicEmpty(t *testing.T) {
	fake, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "src.dlq"))
	require.NoError(t, err)
	t.Cleanup(fake.Close)
	cl, err := kgo.NewClient(kgo.SeedBrokers(fake.ListenAddrs()...))
	require.NoError(t, err)
	t.Cleanup(cl.Close)

	em := New(cl, config.Kafka{DLQTopic: "src.dlq"})
	err = em.Results(context.Background(), &kgo.Record{Topic: "src"}, nil)
	require.NoError(t, err, "disabled results topic must be a no-op, not an error")
}

func headerMap(r *kgo.Record) map[string]string {
	m := make(map[string]string, len(r.Headers))
	for _, h := range r.Headers {
		m[h.Key] = string(h.Value)
	}
	return m
}
```

- [ ] **Step 3: Убедиться, что падают**

Run: `go test ./internal/emit/ -v`
Expected: FAIL — `undefined: New`.

- [ ] **Step 4: Реализовать хедеры и продюсер**

`internal/emit/headers.go`:

```go
package emit

const (
	HeaderReason       = "x-kafkatb-reason"
	HeaderError        = "x-kafkatb-error"
	HeaderDetail       = "x-kafkatb-detail"
	HeaderSrcTopic     = "x-kafkatb-src-topic"
	HeaderSrcPartition = "x-kafkatb-src-partition"
	HeaderSrcOffset    = "x-kafkatb-src-offset"
	HeaderSrcTimestamp = "x-kafkatb-src-timestamp"
	HeaderAttemptTS    = "x-kafkatb-attempt-ts"
)

type Reason string

const (
	ReasonPoison Reason = "poison"
	ReasonReject Reason = "reject"
)
```

`internal/emit/emitter.go`:

```go
package emit

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/tbx"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Emitter interface {
	DLQ(ctx context.Context, rec *kgo.Record, reason Reason, errName, detail string) error
	Results(ctx context.Context, rec *kgo.Record, outcomes []tbx.Outcome) error
	Flush(ctx context.Context) error
	Close()
}

type ResultsMessage struct {
	Source  Source         `json:"source"`
	Results []ResultEntry  `json:"results"`
	EmitTS  string         `json:"emitted_at"`
}

type Source struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

type ResultEntry struct {
	Index  int    `json:"index"`
	ID     string `json:"id"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type emitter struct {
	cl  *kgo.Client
	cfg config.Kafka
}

func New(cl *kgo.Client, cfg config.Kafka) Emitter {
	return &emitter{cl: cl, cfg: cfg}
}

// DLQ публикует исходные байты без изменений: реплей должен быть возможен
// без обратной сборки сообщения.
func (e *emitter) DLQ(ctx context.Context, rec *kgo.Record, reason Reason, errName, detail string) error {
	out := &kgo.Record{
		Topic: e.cfg.DLQTopic,
		Key:   rec.Key,
		Value: rec.Value,
		Headers: []kgo.RecordHeader{
			{Key: HeaderReason, Value: []byte(reason)},
			{Key: HeaderError, Value: []byte(errName)},
			{Key: HeaderDetail, Value: []byte(detail)},
			{Key: HeaderSrcTopic, Value: []byte(rec.Topic)},
			{Key: HeaderSrcPartition, Value: []byte(strconv.FormatInt(int64(rec.Partition), 10))},
			{Key: HeaderSrcOffset, Value: []byte(strconv.FormatInt(rec.Offset, 10))},
			{Key: HeaderSrcTimestamp, Value: []byte(rec.Timestamp.UTC().Format(time.RFC3339Nano))},
			{Key: HeaderAttemptTS, Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
		},
	}
	if err := e.cl.ProduceSync(ctx, out).FirstErr(); err != nil {
		return fmt.Errorf("produce dlq: %w", err)
	}
	return nil
}

func (e *emitter) Results(ctx context.Context, rec *kgo.Record, outcomes []tbx.Outcome) error {
	if e.cfg.ResultsTopic == "" {
		return nil
	}
	msg := ResultsMessage{
		Source:  Source{Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset},
		Results: make([]ResultEntry, len(outcomes)),
		EmitTS:  time.Now().UTC().Format(time.RFC3339Nano),
	}
	for i, o := range outcomes {
		msg.Results[i] = ResultEntry{Index: o.Index, ID: o.ID, Status: string(o.Status), Error: o.Error}
	}
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal results: %w", err)
	}
	out := &kgo.Record{Topic: e.cfg.ResultsTopic, Key: rec.Key, Value: body}
	if err := e.cl.ProduceSync(ctx, out).FirstErr(); err != nil {
		return fmt.Errorf("produce results: %w", err)
	}
	return nil
}

func (e *emitter) Flush(ctx context.Context) error { return e.cl.Flush(ctx) }
func (e *emitter) Close()                          { e.cl.Close() }
```

- [ ] **Step 5: Прогнать**

Run: `go test ./internal/emit/ -race -count=1 -v`
Expected: PASS. Если `cl.OptValue(kgo.SeedBrokers)` вернул не `[]string`, замени в тесте на сохранённый в замыкании `fake.ListenAddrs()`.

- [ ] **Step 6: Коммит**

```bash
git add internal/emit
git commit -m "feat(emit): add dlq and results producers with replayable payloads"
```

---

### Task 7: sink — менеджер офсетов

**Files:**
- Create: `internal/sink/offsets.go`
- Test: `internal/sink/offsets_test.go`

**Interfaces:**
- Consumes: `kgo.Record`, `kgo.EpochOffset`.
- Produces:
  - `sink.NewOffsets() *Offsets`.
  - `(*Offsets).Track(rec *kgo.Record)` — регистрирует запись как «в работе».
  - `(*Offsets).Done(rec *kgo.Record)` — отмечает завершённую.
  - `(*Offsets).Commitable() map[string]map[int32]kgo.EpochOffset` — офсеты для коммита: непрерывный префикс завершённых, `+1` по конвенции Kafka.
  - `(*Offsets).MarkCommitted(committed map[string]map[int32]kgo.EpochOffset)` — фиксирует успешно закоммиченное, чтобы не слать то же повторно.
  - `(*Offsets).Forget(topic string, partition int32)` — сброс состояния партиции при отзыве.
  - `(*Offsets).InFlight() int`.

- [ ] **Step 1: Написать падающий тест**

`internal/sink/offsets_test.go`:

```go
package sink

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func rec(offset int64) *kgo.Record {
	return &kgo.Record{Topic: "t", Partition: 0, Offset: offset, LeaderEpoch: 5}
}

func commitOffset(t *testing.T, o *Offsets) (int64, bool) {
	t.Helper()
	m := o.Commitable()
	tp, ok := m["t"]
	if !ok {
		return 0, false
	}
	eo, ok := tp[0]
	return eo.Offset, ok
}

// Коммитим только непрерывный префикс: дырка останавливает watermark.
func TestCommitableStopsAtGap(t *testing.T) {
	o := NewOffsets()
	for _, r := range []*kgo.Record{rec(0), rec(1), rec(2)} {
		o.Track(r)
	}
	o.Done(rec(0))
	o.Done(rec(2)) // 1 ещё в работе

	got, ok := commitOffset(t, o)
	require.True(t, ok)
	require.Equal(t, int64(1), got, "commit offset is last done + 1")

	o.Done(rec(1))
	got, ok = commitOffset(t, o)
	require.True(t, ok)
	require.Equal(t, int64(3), got)
}

func TestCommitableEmptyWhenNothingDone(t *testing.T) {
	o := NewOffsets()
	o.Track(rec(10))
	_, ok := commitOffset(t, o)
	require.False(t, ok)
}

func TestCommitableIsIdempotent(t *testing.T) {
	o := NewOffsets()
	o.Track(rec(0))
	o.Done(rec(0))
	first, _ := commitOffset(t, o)
	second, ok := commitOffset(t, o)
	require.True(t, ok)
	require.Equal(t, first, second, "repeated Commitable must not rewind")
}

func TestCommitableCarriesLeaderEpoch(t *testing.T) {
	o := NewOffsets()
	o.Track(rec(0))
	o.Done(rec(0))
	require.Equal(t, int32(5), o.Commitable()["t"][0].Epoch)
}

func TestForgetDropsPartitionState(t *testing.T) {
	o := NewOffsets()
	o.Track(rec(0))
	o.Done(rec(0))
	o.Forget("t", 0)
	_, ok := commitOffset(t, o)
	require.False(t, ok)
	require.Zero(t, o.InFlight())
}

func TestInFlightCounts(t *testing.T) {
	o := NewOffsets()
	o.Track(rec(0))
	o.Track(rec(1))
	require.Equal(t, 2, o.InFlight())
	o.Done(rec(0))
	require.Equal(t, 1, o.InFlight())
}

// Партиции не влияют друг на друга.
func TestPartitionsAreIndependent(t *testing.T) {
	o := NewOffsets()
	a := &kgo.Record{Topic: "t", Partition: 0, Offset: 0}
	b := &kgo.Record{Topic: "t", Partition: 1, Offset: 0}
	o.Track(a)
	o.Track(b)
	o.Done(b)
	m := o.Commitable()
	_, hasA := m["t"][0]
	_, hasB := m["t"][1]
	require.False(t, hasA)
	require.True(t, hasB)
}
```

- [ ] **Step 2: Убедиться, что падают**

Run: `go test ./internal/sink/ -v`
Expected: FAIL — `undefined: NewOffsets`.

- [ ] **Step 3: Реализовать**

`internal/sink/offsets.go`:

```go
package sink

import (
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

type partitionKey struct {
	topic     string
	partition int32
}

type partitionState struct {
	// next — первый ещё не завершённый офсет; всё до него готово к коммиту.
	next    int64
	hasNext bool
	// done — завершённые офсеты выше next, ждущие закрытия дырки.
	done  map[int64]struct{}
	epoch int32
	// inflight — сколько записей взято в работу и ещё не завершено.
	inflight int
}

// Offsets отслеживает завершённость записей и отдаёт для коммита
// только непрерывный префикс. Коммит дырки означал бы потерю сообщения.
type Offsets struct {
	mu sync.Mutex
	p  map[partitionKey]*partitionState
}

func NewOffsets() *Offsets {
	return &Offsets{p: make(map[partitionKey]*partitionState)}
}

func (o *Offsets) Track(rec *kgo.Record) {
	o.mu.Lock()
	defer o.mu.Unlock()
	st := o.state(rec)
	if !st.hasNext {
		st.next, st.hasNext = rec.Offset, true
	}
	st.inflight++
}

func (o *Offsets) Done(rec *kgo.Record) {
	o.mu.Lock()
	defer o.mu.Unlock()
	st := o.state(rec)
	st.epoch = rec.LeaderEpoch
	if st.inflight > 0 {
		st.inflight--
	}
	if !st.hasNext {
		st.next, st.hasNext = rec.Offset, true
	}
	if rec.Offset < st.next {
		return // уже учтён
	}
	st.done[rec.Offset] = struct{}{}
	for {
		if _, ok := st.done[st.next]; !ok {
			return
		}
		delete(st.done, st.next)
		st.next++
	}
}

// Commitable отдаёт офсет следующей необработанной записи — ровно то,
// что Kafka ожидает в OffsetCommit.
func (o *Offsets) Commitable() map[string]map[int32]kgo.EpochOffset {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make(map[string]map[int32]kgo.EpochOffset)
	for k, st := range o.p {
		if !st.hasNext || st.committed == st.next {
			continue
		}
		tp, ok := out[k.topic]
		if !ok {
			tp = make(map[int32]kgo.EpochOffset)
			out[k.topic] = tp
		}
		tp[k.partition] = kgo.EpochOffset{Epoch: st.epoch, Offset: st.next}
	}
	return out
}

// MarkCommitted вызывается после успешного коммита, чтобы не слать
// один и тот же офсет повторно.
func (o *Offsets) MarkCommitted(committed map[string]map[int32]kgo.EpochOffset) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for topic, parts := range committed {
		for part, eo := range parts {
			if st, ok := o.p[partitionKey{topic, part}]; ok {
				st.committed = eo.Offset
			}
		}
	}
}

func (o *Offsets) Forget(topic string, partition int32) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.p, partitionKey{topic, partition})
}

func (o *Offsets) InFlight() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	n := 0
	for _, st := range o.p {
		n += st.inflight
	}
	return n
}

func (o *Offsets) state(rec *kgo.Record) *partitionState {
	k := partitionKey{rec.Topic, rec.Partition}
	st, ok := o.p[k]
	if !ok {
		st = &partitionState{done: make(map[int64]struct{}), committed: -1}
		o.p[k] = st
	}
	return st
}
```

Добавь поле `committed int64` в `partitionState` — оно используется в `Commitable` и `MarkCommitted`.

- [ ] **Step 4: Поправить тест идемпотентности под MarkCommitted**

`TestCommitableIsIdempotent` должен проверять, что повторный `Commitable` без новых `Done` возвращает то же значение, а после `MarkCommitted` — пустую карту:

```go
func TestCommitableIsIdempotent(t *testing.T) {
	o := NewOffsets()
	o.Track(rec(0))
	o.Done(rec(0))
	first := o.Commitable()
	require.Equal(t, int64(1), first["t"][0].Offset)
	require.Equal(t, first, o.Commitable(), "repeated Commitable must not rewind")

	o.MarkCommitted(first)
	require.Empty(t, o.Commitable(), "nothing new to commit")
}
```

- [ ] **Step 5: Прогнать**

Run: `go test ./internal/sink/ -race -count=1 -v`
Expected: PASS, все семь тестов.

- [ ] **Step 6: Коммит**

```bash
git add internal/sink
git commit -m "feat(sink): track offsets and commit only contiguous prefix"
```

---

### Task 8: sink — цикл консьюмера

**Files:**
- Create: `internal/sink/sink.go`
- Test: `internal/sink/sink_test.go`

**Interfaces:**
- Consumes: `codec.Registry`, `codec.IsPoison`, `tbx.Batcher` (через интерфейс `sink.Submitter`), `emit.Emitter`, `sink.Offsets`, `config.Config`.
- Produces:
  - `sink.Submitter` — интерфейс `Submit(ctx context.Context, cmd *model.Command) ([]tbx.Outcome, error)`.
  - `sink.New(cfg *config.Config, cl *kgo.Client, decoders codec.Registry, sub Submitter, em emit.Emitter, log *slog.Logger) *Sink`.
  - `sink.NewKafkaClient(cfg *config.Config, onRevoked func(context.Context, map[string][]int32)) (*kgo.Client, error)`.
  - `(*Sink).Run(ctx context.Context) error` — блокирует до отмены контекста.
  - `(*Sink).OnRevoked(ctx context.Context, revoked map[string][]int32)` — передаётся в `NewKafkaClient`.

- [ ] **Step 1: Написать тест обработки одной записи**

Цикл polling'а тестируется в интеграции (Task 9). Здесь покрываем чистую функцию обработки записи, где живёт вся логика классификации.

`internal/sink/sink_test.go`:

```go
package sink

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/Mi7teR/kafka-tb/internal/codec"
	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/Mi7teR/kafka-tb/internal/tbx"
	"github.com/stretchr/testify/require"
	types "github.com/tigerbeetle/tigerbeetle-go"
	"github.com/twmb/franz-go/pkg/kgo"
)

type stubDecoder struct {
	cmd *model.Command
	err error
}

func (s stubDecoder) Decode([]byte) (*model.Command, error) { return s.cmd, s.err }

type stubSubmitter struct {
	outcomes []tbx.Outcome
	err      error
	calls    int
}

func (s *stubSubmitter) Submit(context.Context, *model.Command) ([]tbx.Outcome, error) {
	s.calls++
	return s.outcomes, s.err
}

type recordingEmitter struct {
	dlq     []dlqCall
	results int
	failDLQ error
}

type dlqCall struct {
	reason  string
	errName string
}

func (r *recordingEmitter) DLQ(_ context.Context, _ *kgo.Record, reason emitReason, errName, _ string) error {
	if r.failDLQ != nil {
		return r.failDLQ
	}
	r.dlq = append(r.dlq, dlqCall{reason: string(reason), errName: errName})
	return nil
}
func (r *recordingEmitter) Results(context.Context, *kgo.Record, []tbx.Outcome) error {
	r.results++
	return nil
}
func (r *recordingEmitter) Flush(context.Context) error { return nil }
func (r *recordingEmitter) Close()                      {}

func oneTransferCmd() *model.Command {
	return &model.Command{
		Op:        model.OpCreateTransfers,
		Transfers: []types.Transfer{{ID: types.ToUint128(1)}},
		IDs:       []string{"id-0"},
	}
}

func newSink(t *testing.T, d codec.Decoder, sub Submitter, em emitterIface) *Sink {
	t.Helper()
	s, err := newForTest(codec.Registry{"src": d}, sub, em,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	return s
}

func TestHandlePoisonGoesToDLQ(t *testing.T) {
	em := &recordingEmitter{}
	sub := &stubSubmitter{}
	s := newSink(t, stubDecoder{err: codec.Poison("bad json")}, sub, em)

	done, err := s.handle(context.Background(), &kgo.Record{Topic: "src", Value: []byte("x")})
	require.NoError(t, err)
	require.True(t, done, "poison record must be marked done")
	require.Len(t, em.dlq, 1)
	require.Equal(t, "poison", em.dlq[0].reason)
	require.Zero(t, sub.calls, "poison must never reach TigerBeetle")
}

func TestHandleRejectGoesToDLQAndResults(t *testing.T) {
	em := &recordingEmitter{}
	sub := &stubSubmitter{outcomes: []tbx.Outcome{
		{Index: 0, ID: "id-0", Status: tbx.StatusRejected, Error: "exceeds_credits"},
	}}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, em)

	done, err := s.handle(context.Background(), &kgo.Record{Topic: "src"})
	require.NoError(t, err)
	require.True(t, done)
	require.Len(t, em.dlq, 1)
	require.Equal(t, "reject", em.dlq[0].reason)
	require.Equal(t, "exceeds_credits", em.dlq[0].errName)
	require.Equal(t, 1, em.results)
}

func TestHandleSuccessEmitsResultsOnly(t *testing.T) {
	em := &recordingEmitter{}
	sub := &stubSubmitter{outcomes: []tbx.Outcome{{Index: 0, ID: "id-0", Status: tbx.StatusOK}}}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, em)

	done, err := s.handle(context.Background(), &kgo.Record{Topic: "src"})
	require.NoError(t, err)
	require.True(t, done)
	require.Empty(t, em.dlq)
	require.Equal(t, 1, em.results)
}

// Инфраструктурная ошибка не даёт двигать офсет.
func TestHandleInfraErrorBlocks(t *testing.T) {
	em := &recordingEmitter{}
	sub := &stubSubmitter{err: errors.New("tigerbeetle unavailable")}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, em)

	done, err := s.handle(context.Background(), &kgo.Record{Topic: "src"})
	require.Error(t, err)
	require.False(t, done)
	require.Empty(t, em.dlq)
}

// Если DLQ не пишется — это тоже инфраструктура, офсет стоит.
func TestHandleDLQFailureBlocks(t *testing.T) {
	em := &recordingEmitter{failDLQ: errors.New("broker down")}
	s := newSink(t, stubDecoder{err: codec.Poison("bad json")}, &stubSubmitter{}, em)

	done, err := s.handle(context.Background(), &kgo.Record{Topic: "src"})
	require.Error(t, err)
	require.False(t, done, "must not commit offset if DLQ write failed")
}

// Паника в обработке не роняет процесс.
func TestHandleRecoversPanic(t *testing.T) {
	em := &recordingEmitter{}
	s := newSink(t, panicDecoder{}, &stubSubmitter{}, em)
	done, err := s.handle(context.Background(), &kgo.Record{Topic: "src"})
	require.NoError(t, err)
	require.True(t, done)
	require.Len(t, em.dlq, 1)
	require.Equal(t, "poison", em.dlq[0].reason)
}

type panicDecoder struct{}

func (panicDecoder) Decode([]byte) (*model.Command, error) { panic("boom") }

// Неизвестный топик — конфигурационная ошибка, но убивать процесс из-за
// одного сообщения нельзя: пишем в DLQ.
func TestHandleUnknownTopic(t *testing.T) {
	em := &recordingEmitter{}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, &stubSubmitter{}, em)
	done, err := s.handle(context.Background(), &kgo.Record{Topic: "other"})
	require.NoError(t, err)
	require.True(t, done)
	require.Len(t, em.dlq, 1)
}
```

В тесте используются `emitReason` и `emitterIface` — алиасы, объявленные в `sink.go` (см. ниже), чтобы не тянуть в тест лишние импорты.

- [ ] **Step 2: Убедиться, что падают**

Run: `go test ./internal/sink/ -run TestHandle -v`
Expected: FAIL — `undefined: newForTest`.

- [ ] **Step 3: Реализовать sink**

`internal/sink/sink.go`:

```go
package sink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Mi7teR/kafka-tb/internal/codec"
	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/emit"
	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/Mi7teR/kafka-tb/internal/tbx"
	"github.com/twmb/franz-go/pkg/kgo"
)

type emitReason = emit.Reason
type emitterIface = emit.Emitter

// Submitter — то, что умеет применять команду. В проде это *tbx.Batcher.
type Submitter interface {
	Submit(ctx context.Context, cmd *model.Command) ([]tbx.Outcome, error)
}

type Sink struct {
	cl       *kgo.Client
	decoders codec.Registry
	sub      Submitter
	em       emitterIface
	offsets  *Offsets
	log      *slog.Logger

	pollSize     int
	commitPeriod time.Duration
}

func New(cfg *config.Config, cl *kgo.Client, decoders codec.Registry, sub Submitter, em emitterIface, log *slog.Logger) *Sink {
	return &Sink{
		cl:           cl,
		decoders:     decoders,
		sub:          sub,
		em:           em,
		offsets:      NewOffsets(),
		log:          log,
		pollSize:     cfg.Batcher.MaxBatchSize,
		commitPeriod: time.Second,
	}
}

func newForTest(decoders codec.Registry, sub Submitter, em emitterIface, log *slog.Logger) (*Sink, error) {
	return &Sink{decoders: decoders, sub: sub, em: em, offsets: NewOffsets(), log: log}, nil
}

// Run крутит цикл до отмены контекста.
func (s *Sink) Run(ctx context.Context) error {
	commitTicker := time.NewTicker(s.commitPeriod)
	defer commitTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.commit(context.WithoutCancel(ctx))
			return nil
		case <-commitTicker.C:
			s.commit(ctx)
		default:
		}

		fetches := s.cl.PollRecords(ctx, s.pollSize)
		if fetches.IsClientClosed() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			s.commit(context.WithoutCancel(ctx))
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			s.log.Error("fetch error", slog.String("topic", t), slog.Int("partition", int(p)), slog.String("error", err.Error()))
		})

		records := fetches.Records()
		for _, rec := range records {
			s.offsets.Track(rec)
		}
		// Записи одной партиции обрабатываются строго по порядку,
		// поэтому идём последовательно.
		for _, rec := range records {
			done, err := s.handle(ctx, rec)
			if err != nil {
				// Инфраструктура: не двигаем офсет, ждём и пробуем снова.
				s.log.Error("record blocked", slog.String("topic", rec.Topic),
					slog.Int64("offset", rec.Offset), slog.String("error", err.Error()))
				if !s.backoff(ctx) {
					return nil
				}
				continue
			}
			if done {
				s.offsets.Done(rec)
			}
		}
		s.cl.AllowRebalance()
	}
}

func (s *Sink) backoff(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(time.Second):
		return true
	}
}

// handle возвращает (true, nil), если запись обработана окончательно и её
// офсет можно коммитить. Ошибка означает инфраструктурную проблему:
// офсет остаётся на месте, запись будет обработана снова.
func (s *Sink) handle(ctx context.Context, rec *kgo.Record) (done bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			// Паника — дефект в обработке этого сообщения, а не всего потока.
			s.log.Error("panic handling record", slog.Any("panic", r),
				slog.String("topic", rec.Topic), slog.Int64("offset", rec.Offset))
			if e := s.em.DLQ(ctx, rec, emit.ReasonPoison, "panic", fmt.Sprint(r)); e != nil {
				done, err = false, e
				return
			}
			done, err = true, nil
		}
	}()

	dec, derr := s.decoders.For(rec.Topic)
	if derr != nil {
		if e := s.em.DLQ(ctx, rec, emit.ReasonPoison, "unknown_topic", derr.Error()); e != nil {
			return false, e
		}
		return true, nil
	}

	cmd, derr := dec.Decode(rec.Value)
	if derr != nil {
		if !codec.IsPoison(derr) {
			return false, derr
		}
		if e := s.em.DLQ(ctx, rec, emit.ReasonPoison, "decode", derr.Error()); e != nil {
			return false, e
		}
		return true, nil
	}

	outcomes, serr := s.sub.Submit(ctx, cmd)
	if serr != nil {
		if errors.Is(serr, tbx.ErrCommandTooLarge) {
			if e := s.em.DLQ(ctx, rec, emit.ReasonPoison, "command_too_large", serr.Error()); e != nil {
				return false, e
			}
			return true, nil
		}
		return false, serr
	}

	if e := s.em.Results(ctx, rec, outcomes); e != nil {
		return false, e
	}
	for _, o := range outcomes {
		if o.Status != tbx.StatusRejected {
			continue
		}
		detail := fmt.Sprintf("event %d (id %s): %s", o.Index, o.ID, o.Error)
		if e := s.em.DLQ(ctx, rec, emit.ReasonReject, o.Error, detail); e != nil {
			return false, e
		}
	}
	return true, nil
}

func (s *Sink) commit(ctx context.Context) {
	offsets := s.offsets.Commitable()
	if len(offsets) == 0 {
		return
	}
	if err := s.em.Flush(ctx); err != nil {
		s.log.Error("flush before commit failed", slog.String("error", err.Error()))
		return
	}
	var failed bool
	s.cl.CommitOffsetsSync(ctx, offsets, func(_ *kgo.Client, _ any, _ any, err error) {
		if err != nil {
			failed = true
			s.log.Error("commit failed", slog.String("error", err.Error()))
		}
	})
	if !failed {
		s.offsets.MarkCommitted(offsets)
	}
}

// OnRevoked дренирует и коммитит перед отдачей партиций.
func (s *Sink) OnRevoked(ctx context.Context, revoked map[string][]int32) {
	s.commit(ctx)
	for topic, parts := range revoked {
		for _, p := range parts {
			s.offsets.Forget(topic, p)
		}
	}
}
```

Сигнатура колбэка `CommitOffsetsSync` в franz-go типизирована через `*kmsg.OffsetCommitRequest`/`*kmsg.OffsetCommitResponse` — подставь фактические типы вместо `any`, подсмотрев их в `go doc github.com/twmb/franz-go/pkg/kgo Client.CommitOffsetsSync`.

- [ ] **Step 4: Прогнать unit-тесты**

Run: `go test ./internal/sink/ -race -count=1 -v`
Expected: PASS, все тесты `TestHandle*` и `TestCommitable*`.

- [ ] **Step 5: Собрать клиента Kafka с ручным коммитом**

Дописать в `internal/sink/sink.go`:

```go
// NewKafkaClient собирает консьюмера с ручным коммитом и блокировкой
// ребаланса на время обработки: иначе можно закоммитить чужие партиции.
func NewKafkaClient(cfg *config.Config, onRevoked func(context.Context, map[string][]int32)) (*kgo.Client, error) {
	topics := make([]string, 0, len(cfg.Kafka.Topics))
	for _, t := range cfg.Kafka.Topics {
		topics = append(topics, t.Name)
	}
	return kgo.NewClient(
		kgo.SeedBrokers(cfg.Kafka.Brokers...),
		kgo.ConsumerGroup(cfg.Kafka.Group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.OnPartitionsRevoked(func(ctx context.Context, _ *kgo.Client, revoked map[string][]int32) {
			onRevoked(ctx, revoked)
		}),
	)
}
```

- [ ] **Step 6: Коммит**

```bash
git add internal/sink
git commit -m "feat(sink): add consumer loop with poison, reject and infra handling"
```

---

### Task 9: Интеграционные тесты — Redpanda и TigerBeetle в контейнерах

**Files:**
- Create: `test/integration/harness_test.go`, `test/integration/sink_test.go`
- Modify: `Makefile` (цель `integration` уже добавлена в Task 1)

**Interfaces:**
- Consumes: всё из Task 1-8.
- Produces: хелперы `startRedpanda(t) []string`, `startTigerBeetle(t) string`, `runSink(t, ctx, cfg)`, `balanceOf(t, client, id) string`.

- [ ] **Step 1: Подтянуть testcontainers**

```bash
go get github.com/testcontainers/testcontainers-go@latest
go get github.com/testcontainers/testcontainers-go/modules/redpanda@latest
```

- [ ] **Step 2: Написать харнесс**

`test/integration/harness_test.go` (файл начинается со строки `//go:build integration`):

```go
//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/testcontainers/testcontainers-go/wait"
)

func startRedpanda(t *testing.T) []string {
	t.Helper()
	ctx := context.Background()
	c, err := redpanda.Run(ctx, "redpandadata/redpanda:latest")
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	addr, err := c.KafkaSeedBroker(ctx)
	require.NoError(t, err)
	return []string{addr}
}

// TigerBeetle требует форматирования файла данных перед стартом,
// поэтому контейнер поднимается через shell в два шага.
func startTigerBeetle(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:      "ghcr.io/tigerbeetle/tigerbeetle:0.17.9",
		Entrypoint: []string{"/bin/sh", "-c"},
		Cmd: []string{
			"./tigerbeetle format --cluster=0 --replica=0 --replica-count=1 /data/0.tigerbeetle && " +
				"./tigerbeetle start --addresses=0.0.0.0:3000 /data/0.tigerbeetle",
		},
		ExposedPorts: []string{"3000/tcp"},
		Privileged:   true, // TigerBeetle использует io_uring
		WaitingFor:   wait.ForListeningPort("3000/tcp").WithStartupTimeout(60 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req, Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	host, err := c.Host(ctx)
	require.NoError(t, err)
	port, err := c.MappedPort(ctx, "3000")
	require.NoError(t, err)
	return fmt.Sprintf("%s:%s", host, port.Port())
}
```

- [ ] **Step 3: Убедиться, что контейнеры поднимаются**

Run: `go test ./test/integration/ -tags=integration -run TestHarness -v -timeout=10m`

Добавь временный тест:

```go
func TestHarnessBoots(t *testing.T) {
	brokers := startRedpanda(t)
	addr := startTigerBeetle(t)
	require.NotEmpty(t, brokers[0])
	require.NotEmpty(t, addr)
}
```

Expected: PASS. Если TigerBeetle не стартует — проверь, что Docker разрешает `--privileged`; без io_uring контейнер не поднимется.

- [ ] **Step 4: Тесты сценариев**

`test/integration/sink_test.go` (тоже с `//go:build integration`). Каждый тест поднимает Redpanda + TigerBeetle, запускает `sink` в горутине и проверяет исход. Реализуй эти семь:

```go
// 1. Валидные сообщения применяются, баланс сходится.
func TestSinkAppliesTransfers(t *testing.T)

// 2. Идемпотентность: тот же топик прочитан дважды с нуля —
//    баланс не меняется, второй проход весь в exists.
func TestSinkIsIdempotentOnReplay(t *testing.T)

// 3. Мусор вперемешку: битые сообщения в DLQ, валидные применены,
//    лаг не встаёт.
func TestSinkQuarantinesGarbageAndKeepsGoing(t *testing.T)

// 4. Бизнес-отказ: списание сверх баланса даёт DLQ с exceeds_credits,
//    следующие сообщения обрабатываются.
func TestSinkSendsRejectToDLQ(t *testing.T)

// 5. Реплей из DLQ: те же id дают exists, баланс не меняется.
func TestDLQReplayIsSafe(t *testing.T)

// 6. Перезапуск посреди потока: ни потерь, ни двойного списания.
func TestSinkSurvivesRestart(t *testing.T)

// 7. Атомарность цепочки: во второй проводке недостаточно средств,
//    вся цепочка не применяется.
func TestLinkedChainIsAtomic(t *testing.T)
```

Шаблон одного теста, остальные строятся по нему:

```go
func TestSinkAppliesTransfers(t *testing.T) {
	brokers := startRedpanda(t)
	tbAddr := startTigerBeetle(t)

	cfg := testConfig(brokers, tbAddr)
	tbClient, err := tbx.NewClient(cfg.TigerBeetle)
	require.NoError(t, err)
	t.Cleanup(tbClient.Close)

	debit, credit := seedAccounts(t, tbClient, cfg)

	produce(t, brokers, cfg.Kafka.Topics[0].Name, []string{
		transferJSON(uuid.NewString(), debit, credit, "10.00"),
		transferJSON(uuid.NewString(), debit, credit, "5.50"),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	go runSink(t, ctx, cfg, tbClient)

	require.Eventually(t, func() bool {
		return balanceOf(t, tbClient, credit) == "15.50"
	}, 60*time.Second, 500*time.Millisecond)
}
```

Хелперы, которые нужно написать в `harness_test.go`:

- `testConfig(brokers []string, tbAddr string) *config.Config` — конфиг с уникальным именем группы и топиков на каждый тест (`t.Name()` в имени), `linger: 5ms`, `max_batch_size: 8189`.
- `seedAccounts(t, client, cfg) (debitID, creditID string)` — создаёт два счёта напрямую через `client.CreateAccounts`, дебетовый с флагом `debits_must_not_exceed_credits` там, где тест проверяет отказ; оба с флагом `history`.
- `transferJSON(id, debit, credit, amount string) string` — собирает сообщение.
- `produce(t, brokers []string, topic string, payloads []string)` — синхронная запись через `kgo.Client.ProduceSync`.
- `runSink(t, ctx, cfg, tbClient)` — собирает `tbx.NewBatcher` + `codec.Registry` + `emit.New` + `sink.New` и запускает `Run`.
- `balanceOf(t, client, id string) string` — `LookupAccounts` + `model.FormatAmount(credits_posted - debits_posted, scale)`.
- `dlqRecords(t, brokers []string, topic string, n int, timeout time.Duration) []*kgo.Record` — вычитывает ожидаемое число записей DLQ.

- [ ] **Step 5: Прогнать интеграцию**

Run: `make integration`
Expected: PASS, все семь тестов. Первый прогон долгий — тянутся образы.

- [ ] **Step 6: Коммит**

```bash
git add test/integration go.mod go.sum
git commit -m "test: add end-to-end integration suite on redpanda and tigerbeetle"
```

---

### Task 10: proto и gRPC read-API

**Files:**
- Create: `proto/kafkatb/v1/kafkatb.proto`, `buf.yaml`, `buf.gen.yaml`
- Create: `internal/api/server.go`, `internal/api/read.go`, `internal/api/convert.go`
- Test: `internal/api/read_test.go`

**Interfaces:**
- Consumes: `tbx.Client`, `model.Registry`, `config.API`.
- Produces:
  - Сгенерированный пакет `gen/kafkatb/v1` с `LedgerServer`, `RegisterLedgerHandlerFromEndpoint`.
  - `api.NewServer(c tbx.Client, sub Submitter, reg *model.Registry, cfg config.API) *Server`.
  - `(*Server).Serve(ctx context.Context) error` — поднимает gRPC и HTTP-gateway, останавливает по контексту.
  - `api.Submitter` — тот же интерфейс, что `sink.Submitter`.

- [ ] **Step 1: Написать proto**

`proto/kafkatb/v1/kafkatb.proto` описывает: `Transfer`, `Account`, `Balance`, `WriteResult`, запросы/ответы восьми методов из спеки, аннотации `google.api.http`. Все денежные поля — `string`, все идентификаторы — `string`, `ledger` и `code` — `string`, `flags` — `repeated string`.

```protobuf
syntax = "proto3";
package kafkatb.v1;

import "google/api/annotations.proto";

option go_package = "github.com/Mi7teR/kafka-tb/gen/kafkatb/v1;kafkatbv1";

message Account {
  string id = 1;
  string ledger = 2;
  string code = 3;
  repeated string flags = 4;
  string debits_pending = 5;
  string debits_posted = 6;
  string credits_pending = 7;
  string credits_posted = 8;
  string balance = 9;
  string user_data_128 = 10;
  uint64 user_data_64 = 11;
  uint32 user_data_32 = 12;
  uint64 timestamp = 13;
}

message Transfer {
  string id = 1;
  string debit_account_id = 2;
  string credit_account_id = 3;
  string amount = 4;
  string ledger = 5;
  string code = 6;
  repeated string flags = 7;
  string pending_id = 8;
  string user_data_128 = 9;
  uint64 user_data_64 = 10;
  uint32 user_data_32 = 11;
  uint32 timeout = 12;
  uint64 timestamp = 13;
}

message WriteResult {
  string id = 1;
  string status = 2;  // ok | rejected
  string error = 3;
  string detail = 4;
}

message GetAccountsRequest { repeated string id = 1; }
message GetAccountsResponse { repeated Account accounts = 1; }

message GetTransfersRequest { repeated string id = 1; }
message GetTransfersResponse { repeated Transfer transfers = 1; }

message ListAccountTransfersRequest {
  string account_id = 1;
  uint32 limit = 2;
  uint64 cursor = 3;     // timestamp_min
  bool reversed = 4;
}
message ListAccountTransfersResponse {
  repeated Transfer transfers = 1;
  uint64 next_cursor = 2;
}

message Balance {
  string debits_pending = 1;
  string debits_posted = 2;
  string credits_pending = 3;
  string credits_posted = 4;
  uint64 timestamp = 5;
}
message ListAccountBalancesRequest {
  string account_id = 1;
  uint32 limit = 2;
  uint64 cursor = 3;
  bool reversed = 4;
}
message ListAccountBalancesResponse {
  repeated Balance balances = 1;
  uint64 next_cursor = 2;
}

message QueryTransfersRequest {
  string user_data_128 = 1;
  uint64 user_data_64 = 2;
  uint32 user_data_32 = 3;
  string ledger = 4;
  string code = 5;
  uint32 limit = 6;
  uint64 cursor = 7;
}
message QueryTransfersResponse {
  repeated Transfer transfers = 1;
  uint64 next_cursor = 2;
}

message QueryAccountsRequest {
  string user_data_128 = 1;
  uint64 user_data_64 = 2;
  uint32 user_data_32 = 3;
  string ledger = 4;
  string code = 5;
  uint32 limit = 6;
  uint64 cursor = 7;
}
message QueryAccountsResponse {
  repeated Account accounts = 1;
  uint64 next_cursor = 2;
}

message CreateAccountsRequest { repeated Account accounts = 1; }
message CreateAccountsResponse { repeated WriteResult results = 1; }
message CreateTransfersRequest { repeated Transfer transfers = 1; }
message CreateTransfersResponse { repeated WriteResult results = 1; }

service Ledger {
  rpc GetAccounts(GetAccountsRequest) returns (GetAccountsResponse) {
    option (google.api.http) = {get: "/v1/accounts"};
  }
  rpc GetTransfers(GetTransfersRequest) returns (GetTransfersResponse) {
    option (google.api.http) = {get: "/v1/transfers"};
  }
  rpc ListAccountTransfers(ListAccountTransfersRequest) returns (ListAccountTransfersResponse) {
    option (google.api.http) = {get: "/v1/accounts/{account_id}/transfers"};
  }
  rpc ListAccountBalances(ListAccountBalancesRequest) returns (ListAccountBalancesResponse) {
    option (google.api.http) = {get: "/v1/accounts/{account_id}/balances"};
  }
  rpc QueryTransfers(QueryTransfersRequest) returns (QueryTransfersResponse) {
    option (google.api.http) = {get: "/v1/transfers:query"};
  }
  rpc QueryAccounts(QueryAccountsRequest) returns (QueryAccountsResponse) {
    option (google.api.http) = {get: "/v1/accounts:query"};
  }
  rpc CreateAccounts(CreateAccountsRequest) returns (CreateAccountsResponse) {
    option (google.api.http) = {post: "/v1/accounts" body: "*"};
  }
  rpc CreateTransfers(CreateTransfersRequest) returns (CreateTransfersResponse) {
    option (google.api.http) = {post: "/v1/transfers" body: "*"};
  }
}
```

- [ ] **Step 2: Настроить buf и сгенерировать**

`buf.yaml`:

```yaml
version: v2
modules:
  - path: proto
deps:
  - buf.build/googleapis/googleapis
lint:
  use: [STANDARD]
```

`buf.gen.yaml`:

```yaml
version: v2
managed:
  enabled: true
plugins:
  - remote: buf.build/protocolbuffers/go
    out: gen
    opt: paths=source_relative
  - remote: buf.build/grpc/go
    out: gen
    opt: paths=source_relative
  - remote: buf.build/grpc-ecosystem/gateway
    out: gen
    opt: paths=source_relative
```

Run: `buf dep update && make proto && go build ./gen/...`
Expected: в `gen/kafkatb/v1/` появились `kafkatb.pb.go`, `kafkatb_grpc.pb.go`, `kafkatb.pb.gw.go`; сборка проходит.

- [ ] **Step 3: Написать падающие тесты конвертеров и чтения**

`internal/api/read_test.go` проверяет самое неочевидное — вычисление баланса по флагам счёта:

```go
package api

import (
	"testing"

	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/stretchr/testify/require"
	types "github.com/tigerbeetle/tigerbeetle-go"
)

func testRegistry() *model.Registry {
	return model.NewRegistry(&config.Config{
		Ledgers: map[string]config.Ledger{"USD": {ID: 1, Scale: 2}},
		Codes:   map[string]uint16{"customer": 1},
	})
}

// Кредитовый счёт: баланс = credits - debits.
func TestAccountBalanceCreditNormal(t *testing.T) {
	acc := types.Account{
		ID: types.ToUint128(1), Ledger: 1, Code: 1,
		DebitsPosted: types.ToUint128(125000), CreditsPosted: types.ToUint128(140000),
	}
	got, err := accountToProto(acc, testRegistry())
	require.NoError(t, err)
	require.Equal(t, "150.00", got.Balance)
	require.Equal(t, "1250.00", got.DebitsPosted)
	require.Equal(t, "USD", got.Ledger)
	require.Equal(t, "customer", got.Code)
}

// Дебетовый счёт (credits_must_not_exceed_debits): баланс = debits - credits.
func TestAccountBalanceDebitNormal(t *testing.T) {
	acc := types.Account{
		ID: types.ToUint128(1), Ledger: 1, Code: 1,
		Flags:        types.AccountFlags{CreditsMustNotExceedDebits: true}.ToUint16(),
		DebitsPosted: types.ToUint128(140000), CreditsPosted: types.ToUint128(125000),
	}
	got, err := accountToProto(acc, testRegistry())
	require.NoError(t, err)
	require.Equal(t, "150.00", got.Balance)
	require.Contains(t, got.Flags, "credits_must_not_exceed_debits")
}

func TestAccountUnknownLedgerIsError(t *testing.T) {
	_, err := accountToProto(types.Account{Ledger: 99, Code: 1}, testRegistry())
	require.ErrorContains(t, err, "unknown ledger")
}
```

- [ ] **Step 4: Реализовать конвертеры и read-хендлеры**

`internal/api/convert.go` — `accountToProto`, `transferToProto`, `balanceToProto`, `protoToTransfer`, `protoToAccount`, `flagNamesFromAccount`, `flagNamesFromTransfer`. Баланс:

```go
// balance считает «человеческий» остаток по направлению счёта,
// чтобы клиенту не надо было помнить, дебетовый счёт или кредитовый.
func balance(a types.Account, scale int32) string {
	debits, credits := a.DebitsPosted.BigInt(), a.CreditsPosted.BigInt()
	var diff big.Int
	if a.Flags&types.AccountFlags{CreditsMustNotExceedDebits: true}.ToUint16() != 0 {
		diff.Sub(&debits, &credits)
	} else {
		diff.Sub(&credits, &debits)
	}
	neg := diff.Sign() < 0
	if neg {
		diff.Abs(&diff)
	}
	s := model.FormatAmount(types.BigIntToUint128(diff), scale)
	if neg {
		return "-" + s
	}
	return s
}
```

`internal/api/read.go` — шесть read-методов. Каждый: валидирует лимит против `cfg.MaxPageSize`, строит `types.AccountFilter`/`types.QueryFilter`, вызывает клиента, конвертирует, выставляет `next_cursor` как `timestamp` последнего элемента `+1`. Невалидный id — `codes.InvalidArgument`; ошибка клиента — `codes.Unavailable`.

- [ ] **Step 5: Прогнать**

Run: `go test ./internal/api/ -race -count=1 -v`
Expected: PASS.

- [ ] **Step 6: Коммит**

```bash
git add proto buf.yaml buf.gen.yaml gen internal/api
git commit -m "feat(api): add proto schema and grpc read handlers"
```

---

### Task 11: API — запись и HTTP-gateway

**Files:**
- Create: `internal/api/write.go`, `internal/api/gateway.go`
- Test: `internal/api/write_test.go`

**Interfaces:**
- Consumes: `api.Submitter` (тот же `Submit(ctx, *model.Command) ([]tbx.Outcome, error)`), `model.Registry`.
- Produces: методы `CreateTransfers`, `CreateAccounts` на `*Server`; `(*Server).Serve(ctx)`.

- [ ] **Step 1: Написать падающие тесты**

`internal/api/write_test.go`:

```go
package api

import (
	"context"
	"testing"

	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/Mi7teR/kafka-tb/internal/tbx"
	kafkatbv1 "github.com/Mi7teR/kafka-tb/gen/kafkatb/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type stubSubmitter struct {
	outcomes []tbx.Outcome
	err      error
	got      *model.Command
}

func (s *stubSubmitter) Submit(_ context.Context, cmd *model.Command) ([]tbx.Outcome, error) {
	s.got = cmd
	return s.outcomes, s.err
}

func req() *kafkatbv1.CreateTransfersRequest {
	return &kafkatbv1.CreateTransfersRequest{Transfers: []*kafkatbv1.Transfer{{
		Id:              "0193f8a1-7c2e-7000-8000-000000000001",
		DebitAccountId:  "0193f8a1-0000-7000-8000-000000000010",
		CreditAccountId: "0193f8a1-0000-7000-8000-000000000020",
		Amount:          "12.34", Ledger: "USD", Code: "customer",
	}}}
}

// Бизнес-отказ — не ошибка транспорта: 200 с исходом по элементу.
func TestCreateTransfersReturnsRejectionInBody(t *testing.T) {
	sub := &stubSubmitter{outcomes: []tbx.Outcome{
		{Index: 0, ID: "0193f8a1-7c2e-7000-8000-000000000001", Status: tbx.StatusRejected, Error: "exceeds_credits"},
	}}
	s := newTestServer(sub)
	resp, err := s.CreateTransfers(context.Background(), req())
	require.NoError(t, err)
	require.Len(t, resp.Results, 1)
	require.Equal(t, "rejected", resp.Results[0].Status)
	require.Equal(t, "exceeds_credits", resp.Results[0].Error)
}

func TestCreateTransfersInvalidInputIsInvalidArgument(t *testing.T) {
	s := newTestServer(&stubSubmitter{})
	r := req()
	r.Transfers[0].Amount = "12.345"
	_, err := s.CreateTransfers(context.Background(), r)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestCreateTransfersInfraErrorIsUnavailable(t *testing.T) {
	s := newTestServer(&stubSubmitter{err: context.DeadlineExceeded})
	_, err := s.CreateTransfers(context.Background(), req())
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestCreateTransfersEmptyIsInvalidArgument(t *testing.T) {
	s := newTestServer(&stubSubmitter{})
	_, err := s.CreateTransfers(context.Background(), &kafkatbv1.CreateTransfersRequest{})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}
```

Хелпер `newTestServer` объявляется в этом же файле:

```go
func newTestServer(sub Submitter) *Server {
	return NewServer(nil, sub, testRegistry(), config.API{MaxPageSize: 1000})
}
```

- [ ] **Step 2: Убедиться, что падают**

Run: `go test ./internal/api/ -run TestCreateTransfers -v`
Expected: FAIL — `undefined: newTestServer`.

- [ ] **Step 3: Реализовать запись**

`internal/api/write.go` переиспользует ту же конверсию, что кодек — иначе контракт разъедется:

```go
func (s *Server) CreateTransfers(ctx context.Context, in *kafkatbv1.CreateTransfersRequest) (*kafkatbv1.CreateTransfersResponse, error) {
	if len(in.Transfers) == 0 {
		return nil, status.Error(codes.InvalidArgument, "transfers: empty")
	}
	cmd, err := s.commandFromTransfers(in.Transfers)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	outcomes, err := s.sub.Submit(ctx, cmd)
	if err != nil {
		if errors.Is(err, tbx.ErrCommandTooLarge) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &kafkatbv1.CreateTransfersResponse{Results: writeResults(outcomes)}, nil
}
```

`commandFromTransfers` использует `model.ParseID`, `model.ParseAmount`, `Registry.TransferFlags` — те же функции, что JSON-декодер, и снимает `linked` с последнего элемента.

- [ ] **Step 4: Gateway и Serve**

`internal/api/gateway.go`:

```go
// Serve поднимает gRPC и REST на разных портах и гасит оба по контексту.
func (s *Server) Serve(ctx context.Context, cfg config.API) error {
	grpcSrv := grpc.NewServer()
	kafkatbv1.RegisterLedgerServer(grpcSrv, s)

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen grpc: %w", err)
	}

	mux := runtime.NewServeMux()
	if err := kafkatbv1.RegisterLedgerHandlerFromEndpoint(ctx, mux, cfg.GRPCAddr,
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}); err != nil {
		return fmt.Errorf("register gateway: %w", err)
	}
	httpSrv := &http.Server{Addr: cfg.HTTPAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return grpcSrv.Serve(lis) })
	g.Go(func() error {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		grpcSrv.GracefulStop()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(gctx), 10*time.Second)
		defer cancel()
		return httpSrv.Shutdown(shutdownCtx)
	})
	return g.Wait()
}
```

- [ ] **Step 5: Прогнать**

Run: `go test ./internal/api/ -race -count=1 -v && go build ./...`
Expected: PASS.

- [ ] **Step 6: Коммит**

```bash
git add internal/api
git commit -m "feat(api): add write handlers and http gateway"
```

---

### Task 12: Наблюдаемость и точка входа

**Files:**
- Create: `internal/obs/metrics.go`, `internal/obs/health.go`
- Create: `cmd/kafkatb/main.go`
- Modify: `internal/sink/sink.go` (инкремент метрик), `internal/tbx/batcher.go` (гистограммы)
- Test: `internal/obs/metrics_test.go`

**Interfaces:**
- Consumes: `config.Config`, `tbx.Client`, `kgo.Client`.
- Produces:
  - `obs.Metrics` со счётчиками `RecordsTotal *prometheus.CounterVec` (метка `result`: `ok|rejected|poison|blocked`), `DLQTotal *prometheus.CounterVec` (метки `reason`, `error`), `BatchSize prometheus.Histogram`, `TBLatency *prometheus.HistogramVec` (метка `op`), `CommitLag prometheus.Gauge`.
  - `obs.NewMetrics(reg prometheus.Registerer) *Metrics`.
  - `obs.Serve(ctx context.Context, addr string, ready func() error) error` — `/metrics`, `/healthz`, `/readyz`.

- [ ] **Step 1: Написать тест метрик**

`/readyz` использует `tbx.Client.Nop()` — метод уже есть и в интерфейсе (Task 4), и в настоящем клиенте, добавлять ничего не нужно.

`internal/obs/metrics_test.go`:

```go
package obs

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestMetricsRegisterAndCount(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	m.RecordsTotal.WithLabelValues("ok").Add(2)
	m.DLQTotal.WithLabelValues("poison", "decode").Inc()

	require.Equal(t, 2.0, testutil.ToFloat64(m.RecordsTotal.WithLabelValues("ok")))
	require.Equal(t, 1.0, testutil.ToFloat64(m.DLQTotal.WithLabelValues("poison", "decode")))
}

func TestNewMetricsTwiceOnSameRegistryPanicsNot(t *testing.T) {
	reg := prometheus.NewRegistry()
	NewMetrics(reg)
	require.NotPanics(t, func() { NewMetrics(prometheus.NewRegistry()) })
}
```

- [ ] **Step 2: Реализовать метрики и health**

```bash
go get github.com/prometheus/client_golang@latest
```

`internal/obs/metrics.go` — определения через `promauto.With(reg)`. Бакеты `TBLatency`: `prometheus.ExponentialBuckets(0.001, 2, 14)` (от 1мс до ~8с). Бакеты `BatchSize`: `[]float64{1, 10, 100, 500, 1000, 2000, 4000, 8189}`.

`internal/obs/health.go` — HTTP-сервер с тремя ручками. `/readyz` вызывает переданный `ready func() error`; в проде это `client.Nop()` для TigerBeetle плюс проверка, что консьюмер в группе.

- [ ] **Step 3: Прогнать**

Run: `go test ./internal/obs/ -race -count=1 -v`
Expected: PASS.

- [ ] **Step 4: Проставить метрики в sink и batcher**

В `sink.handle` — инкремент `RecordsTotal` по исходу и `DLQTotal` при записи в DLQ. В `Batcher.sendTransfers`/`sendAccounts` — `BatchSize.Observe(float64(len(events)))` и замер длительности вызова в `TBLatency`. Метрики передаются конструкторами; при `nil` код работает без учёта (для тестов).

- [ ] **Step 5: Написать main**

`cmd/kafkatb/main.go`:

```go
func main() {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", "configs/example.yaml", "path to config file")
	mode := flag.String("mode", "", "override mode: sink|api|all")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Error("config", slog.String("error", err.Error()))
		os.Exit(1)
	}
	if *mode != "" {
		cfg.Mode = config.Mode(*mode)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, log); err != nil {
		log.Error("shutdown with error", slog.String("error", err.Error()))
		os.Exit(1)
	}
	log.Info("stopped cleanly")
}
```

`run` собирает граф зависимостей: `tbx.NewClient` → `tbx.NewBatcher` + `Start` → по режиму поднимает sink и/или API → `errgroup`. Останов: по контексту гасим consumer (`cl.Close()` после финального коммита), затем `batcher.Close()`, затем `emitter.Flush` + `Close`, затем `tbClient.Close()`. Дедлайн — `cfg.ShutdownTimeout`, по истечении выходим без коммита: at-least-once это допускает.

- [ ] **Step 6: Проверить сборку и запуск**

Run: `make build && ./bin/kafkatb -config configs/example.yaml -mode api & sleep 2 && curl -sf localhost:8080/healthz && kill %1`
Expected: `make build` без ошибок, `/healthz` отвечает 200.

- [ ] **Step 7: Коммит**

```bash
git add internal/obs cmd internal/sink internal/tbx
git commit -m "feat: add metrics, health endpoints and service entrypoint"
```

---

### Task 13: Нагрузочный харнесс и фиксация бенчмарков

**Files:**
- Create: `cmd/loadgen/main.go`
- Create: `docs/benchmarks/README.md`
- Create: `scripts/bench.sh`

**Interfaces:**
- Consumes: `config.Config`, `emit`-независимый продюсер на franz-go.
- Produces: бинарь `loadgen` с флагами `-brokers`, `-topic`, `-count`, `-accounts`, `-hot-account`, `-chain`, `-rate`, `-garbage-pct`.

- [ ] **Step 1: Написать loadgen**

`cmd/loadgen/main.go` генерирует сообщения и льёт их в топик:

- `-count` — сколько transfer'ов всего.
- `-accounts` — размер пула счетов; ключ сообщения = `debit_account_id`, чтобы шардирование по счёту работало.
- `-hot-account` — все проводки в один счёт (сценарий конкуренции).
- `-chain N` — упаковывать по N transfer'ов в одно сообщение с флагом `linked`.
- `-garbage-pct` — доля намеренно битых сообщений.
- `-rate` — ограничение скорости, 0 = без ограничения.

Каждое сообщение несёт `user_data_64` со временем публикации в наносекундах — это даёт end-to-end задержку при разборе results-топика.

- [ ] **Step 2: Скрипт замера**

`scripts/bench.sh` запускает микробенчмарки и сохраняет сырой вывод:

```bash
#!/usr/bin/env bash
set -euo pipefail
out="docs/benchmarks/$(git rev-parse --short HEAD).txt"
go test ./... -run='^$' -bench=. -benchmem -count=6 | tee "$out"
echo "saved to $out"
echo "compare: benchstat docs/benchmarks/<old>.txt $out"
```

- [ ] **Step 3: Снять базовые цифры**

Run: `chmod +x scripts/bench.sh && ./scripts/bench.sh`
Expected: файл в `docs/benchmarks/` с результатами `BenchmarkDecodeJSON`, `BenchmarkParseAmount`, `BenchmarkFormatAmount`, `BenchmarkParseID`, `BenchmarkMapResults`, `BenchmarkBatcherAssemble`.

- [ ] **Step 4: Прогнать нагрузочные сценарии**

Подними стенд (Redpanda + TigerBeetle single-replica локально) и сними шесть сценариев из спеки:

| Сценарий | Команда |
|---|---|
| Потолок пропускной способности | `loadgen -count=1000000 -accounts=10000` |
| Горячий счёт | `loadgen -count=200000 -hot-account` |
| linger 1мс | тот же прогон с `batcher.linger: 1ms` |
| linger 50мс | тот же прогон с `batcher.linger: 50ms` |
| 5% мусора | `loadgen -count=200000 -accounts=1000 -garbage-pct=5` |
| Цепочки по 10 | `loadgen -count=200000 -accounts=1000 -chain=10` |

Для каждого запиши: transfers/сек, p50/p95/p99 end-to-end, средний размер батча (`tb_batch_size`), p99 `tb_latency_seconds`, поведение лага.

- [ ] **Step 5: Записать результаты**

`docs/benchmarks/README.md` — таблица со столбцами: сценарий, коммит, железо, конфиг (linger, max_batch_size), transfers/сек, p50, p95, p99, средний батч. Плюс абзац о том, какой `linger` выбран дефолтом и почему.

- [ ] **Step 6: Коммит**

```bash
git add cmd/loadgen scripts docs/benchmarks
git commit -m "feat(loadgen): add load harness and record baseline benchmarks"
```

---

## Порядок и зависимости

```
1 config ──► 2 model ──► 3 codec ──┐
                │                  ├──► 8 sink ──► 9 integration
                ├──► 4 tbx outcome ┤       ▲
                │        └─► 5 batcher ────┘
                └──► 6 emit ──► 7 offsets ─┘
                         │
5 batcher ──► 10 api read ──► 11 api write ──► 12 main+obs ──► 13 loadgen
```

Задачи 6 и 7 не зависят друг от друга и могут идти параллельно. Задача 9 требует всего от 1 до 8. Задача 13 требует 12.

## Definition of Done

- `make test` зелёный с `-race`.
- `make integration` зелёный: все семь сценариев.
- `make lint` без замечаний.
- Фаззинг `FuzzDecode` и `FuzzParseAmount` отработали минимум по 60 секунд без находок.
- `docs/benchmarks/README.md` содержит цифры хотя бы одного полного прогона.
- `make build` даёт бинарь, который поднимается во всех трёх режимах и отвечает на `/healthz`.
