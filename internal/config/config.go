package config

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	mapstructure "github.com/go-viper/mapstructure/v2"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

const MaxBatchSize = 8189

// DefaultMaxInFlightPerPartition is used when sink.max_in_flight_per_partition
// is left unset.
const DefaultMaxInFlightPerPartition = 1000

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
	// MaxQueue is the depth of the batcher's single queue, which is also the
	// batcher as a whole: one worker serves both operation types, so commands
	// of every operation share this one queue. The worker holds MaxQueue
	// commands queued plus up to MaxBatchSize events in the batch it is
	// assembling or sending. The process can therefore hold up to
	// MaxQueue + MaxBatchSize in the batcher: 9,189 at the defaults.
	MaxQueue int `yaml:"max_queue"`
}

type Sink struct {
	// MaxInFlightPerPartition caps how many of one partition's records the sink
	// hands to the batcher in a single pass before it starts collecting their
	// outcomes. It is a throughput/replay trade-off, not a safety bound: on an
	// ungraceful stop up to MaxInFlightPerPartition × (assigned partitions)
	// records may be applied to TigerBeetle but neither published nor
	// committed, and will be replayed after restart — duplicate results-topic
	// messages included. Correctness of the replay rests on stable command ids
	// and on TransferExists/AccountExists being mapped to StatusOK, not on this
	// number.
	MaxInFlightPerPartition int `yaml:"max_in_flight_per_partition"`
}

type Topic struct {
	Name  string `yaml:"name"`
	Codec string `yaml:"codec"`
}

// Partition key names accepted by cdc.partition_key. The key decides which
// ordering a consumer of the CDC topic gets — per debit account, per credit
// account, per ledger or per transfer — so it is the consumer's choice, not
// ours.
const (
	PartitionKeyDebitAccountID  = "debit_account_id"
	PartitionKeyCreditAccountID = "credit_account_id"
	PartitionKeyLedger          = "ledger"
	PartitionKeyTransferID      = "transfer_id"
)

// Defaults for the CDC job, applied when the config file omits them.
const (
	DefaultCDCBatchSize    = 1000
	DefaultCDCPollInterval = time.Second
)

// CDC configures the TigerBeetle -> Kafka change-event job. An empty Topic
// disables the job: kafkatb run then starts the sink alone, and kafkatb cdc
// refuses to start. The remaining fields always carry a valid value — Load
// registers defaults for them — so they are validated whether the job is
// enabled or not.
type CDC struct {
	Topic        string        `yaml:"topic"`
	BatchSize    int           `yaml:"batch_size"`
	PollInterval time.Duration `yaml:"poll_interval"`
	PartitionKey string        `yaml:"partition_key"`
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

type Retry struct {
	Initial time.Duration `yaml:"initial"`
	Max     time.Duration `yaml:"max"`
	Jitter  bool          `yaml:"jitter"`
}

type Config struct {
	TigerBeetle     TigerBeetle       `yaml:"tigerbeetle"`
	Batcher         Batcher           `yaml:"batcher"`
	Sink            Sink              `yaml:"sink"`
	CDC             CDC               `yaml:"cdc"`
	Kafka           Kafka             `yaml:"kafka"`
	Limits          Limits            `yaml:"limits"`
	Ledgers         map[string]Ledger `yaml:"ledgers"`
	Codes           map[string]uint16 `yaml:"codes"`
	Retry           Retry             `yaml:"retry"`
	ShutdownTimeout time.Duration     `yaml:"shutdown_timeout"`
	// MetricsAddr serves /metrics, /healthz and /readyz.
	MetricsAddr string `yaml:"metrics_addr"`
	// Pprof adds net/http/pprof's endpoints to the metrics server and turns
	// on the block and mutex profilers. Off by default and meant to stay
	// that way outside an investigation: /debug/pprof/ exposes the process's
	// command line, its goroutine stacks and its heap to anyone who can
	// reach MetricsAddr, and the two profilers it enables are not free.
	Pprof bool `yaml:"pprof"`
}

func (c *Config) LedgerByName(name string) (Ledger, bool) {
	l, ok := c.Ledgers[name]
	return l, ok
}

func (c *Config) CodeByName(name string) (uint16, bool) {
	v, ok := c.Codes[name]
	return v, ok
}

// Option customises the *viper.Viper Load builds internally, before the
// config file and environment are read. Its only use today is WithFlag,
// which lets a caller (cmd/kafkatb) make a command-line flag take precedence
// over everything else.
type Option func(*viper.Viper)

// WithFlag binds a pflag to a config key, giving it top priority: an
// explicitly-set flag overrides KAFKATB_* env vars, which override the
// config file, which overrides defaults. A flag left at its default is
// ignored, exactly like viper.Viper.BindPFlag.
func WithFlag(key string, flag *pflag.Flag) Option {
	return func(v *viper.Viper) {
		_ = v.BindPFlag(key, flag) // only errors when flag is nil
	}
}

// Load reads and validates the config file at path, in this order:
// flags (via opts) > KAFKATB_* environment variables > the file > defaults.
// It is the single place that produces a validated *Config.
func Load(path string, opts ...Option) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	// Viper silently ignores keys it doesn't recognise, so a typo like
	// max_batch_sze would quietly keep its default. Catch that ourselves,
	// against the shape of Config, before viper ever sees the file.
	var rawTree map[string]any
	if err := yaml.Unmarshal(raw, &rawTree); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := checkKnownKeys(rawTree, reflect.TypeOf(Config{}), ""); err != nil {
		return nil, err
	}

	v := viper.New()
	for _, opt := range opts {
		opt(v)
	}
	v.SetConfigType("yaml")
	if err := v.ReadConfig(bytes.NewReader(raw)); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	v.SetEnvPrefix("KAFKATB")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	// sink.max_in_flight_per_partition is the one field that may be absent
	// from the file (see the clamp below). Registering it means an env
	// override for it still works even when the file never mentions it.
	v.SetDefault("sink.max_in_flight_per_partition", 0)
	// The CDC job's knobs are optional in the file — only cdc.topic decides
	// whether the job runs at all — so they are defaulted here rather than
	// left at Go's zero value, which validate would (rightly) reject.
	v.SetDefault("cdc.batch_size", DefaultCDCBatchSize)
	v.SetDefault("cdc.poll_interval", DefaultCDCPollInterval)
	v.SetDefault("cdc.partition_key", PartitionKeyDebitAccountID)

	var cfg Config
	err = v.Unmarshal(&cfg, func(c *mapstructure.DecoderConfig) { c.TagName = "yaml" })
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	// Viper lowercases every key it reads for case-insensitive lookups,
	// including these two maps' keys — "USD" would become "usd". Ledger and
	// code names are caller-chosen identifiers, not config knob names, and
	// must keep their case, so decode just these two fields straight from
	// the file instead of through viper.
	var names struct {
		Ledgers map[string]Ledger `yaml:"ledgers"`
		Codes   map[string]uint16 `yaml:"codes"`
	}
	if err := yaml.Unmarshal(raw, &names); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	cfg.Ledgers, cfg.Codes = names.Ledgers, names.Codes

	if cfg.Sink.MaxInFlightPerPartition == 0 {
		// The default is clamped to the batcher's ceiling rather than imposed outright:
		// a config written before this field existed might have a queue smaller
		// than the default, and it must not fail to load over a value it never
		// specified. An explicitly set value above the ceiling is still
		// rejected — that is the config author's mistake, not our default's.
		cfg.Sink.MaxInFlightPerPartition = min(
			DefaultMaxInFlightPerPartition, cfg.Batcher.MaxQueue+cfg.Batcher.MaxBatchSize)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// checkKnownKeys walks node (as decoded by yaml.v3 into plain maps/slices)
// against t, the Go type it is destined to fill, and fails on the first key
// with no corresponding yaml-tagged field — at any depth, not just the top
// level. path is the dotted/indexed location of node, for the error message.
func checkKnownKeys(node any, t reflect.Type, path string) error {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Struct:
		m, ok := node.(map[string]any)
		if !ok {
			return nil // shape mismatch: the decode step below will report it
		}
		fields := map[string]reflect.StructField{}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			name, _, _ := strings.Cut(f.Tag.Get("yaml"), ",")
			if name == "" || name == "-" {
				continue
			}
			fields[name] = f
		}
		for k, v := range m {
			f, ok := fields[k]
			child := joinPath(path, k)
			if !ok {
				return fmt.Errorf("config: unknown key %q", child)
			}
			if err := checkKnownKeys(v, f.Type, child); err != nil {
				return err
			}
		}
	case reflect.Map:
		m, ok := node.(map[string]any)
		if !ok {
			return nil
		}
		for k, v := range m {
			if err := checkKnownKeys(v, t.Elem(), joinPath(path, k)); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		s, ok := node.([]any)
		if !ok {
			return nil
		}
		for i, v := range s {
			if err := checkKnownKeys(v, t.Elem(), fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func joinPath(prefix, key string) string {
	if prefix == "" {
		return key
	}
	return prefix + "." + key
}

func (c *Config) validate() error {
	if len(c.TigerBeetle.Addresses) == 0 {
		return fmt.Errorf("tigerbeetle.addresses: must not be empty")
	}
	for _, a := range c.TigerBeetle.Addresses {
		if err := validateTBAddress(a); err != nil {
			return fmt.Errorf("tigerbeetle.addresses: %w", err)
		}
	}
	if c.Batcher.MaxBatchSize <= 0 || c.Batcher.MaxBatchSize > MaxBatchSize {
		return fmt.Errorf("batcher.max_batch_size: want 1..%d, got %d", MaxBatchSize, c.Batcher.MaxBatchSize)
	}
	if c.Batcher.MaxQueue <= 0 {
		return fmt.Errorf("batcher.max_queue: must be > 0")
	}
	// max_queue + max_batch_size is what a single partition, alone in the
	// process, could ever get in flight: a job dequeued into the batch being
	// assembled frees its queue slot while its caller stays parked for the whole
	// TigerBeetle round trip. It is not a protective limit — the batcher's queue
	// is global while this setting is per-partition, so with several assigned
	// partitions the enqueue blocks at a fraction of it. The check only rejects
	// a number that could never describe anything real.
	if ceiling := c.Batcher.MaxQueue + c.Batcher.MaxBatchSize; c.Sink.MaxInFlightPerPartition <= 0 ||
		c.Sink.MaxInFlightPerPartition > ceiling {
		return fmt.Errorf(
			"sink.max_in_flight_per_partition: want 1..%d — no single partition can hold more"+
				" than batcher.max_queue + batcher.max_batch_size in flight, and with several"+
				" partitions the shared batcher queue blocks the enqueue well below that; got %d",
			ceiling, c.Sink.MaxInFlightPerPartition)
	}
	// cdc.topic may be empty (the job is then disabled), but the knobs are
	// checked either way: a bad value must be reported when it is written, not
	// on the day someone adds the topic.
	if c.CDC.BatchSize <= 0 || c.CDC.BatchSize > MaxBatchSize {
		return fmt.Errorf("cdc.batch_size: want 1..%d, got %d", MaxBatchSize, c.CDC.BatchSize)
	}
	if c.CDC.PollInterval <= 0 {
		return fmt.Errorf("cdc.poll_interval: must be > 0")
	}
	switch c.CDC.PartitionKey {
	case PartitionKeyDebitAccountID, PartitionKeyCreditAccountID, PartitionKeyLedger, PartitionKeyTransferID:
	default:
		return fmt.Errorf("cdc.partition_key: want one of %s|%s|%s|%s, got %q",
			PartitionKeyDebitAccountID, PartitionKeyCreditAccountID,
			PartitionKeyLedger, PartitionKeyTransferID, c.CDC.PartitionKey)
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
	if c.MetricsAddr == "" {
		return fmt.Errorf("metrics_addr: must not be empty")
	}
	return nil
}

// validateTBAddress rejects anything the TigerBeetle client would reject
// only at connect time. TigerBeetle's address parser takes a bare port or an
// IP:port with an IP literal — never a hostname: "localhost:3000" fails at
// connect with an opaque "invalid client cluster address", long after config
// load succeeded.
func validateTBAddress(addr string) error {
	if _, err := strconv.ParseUint(addr, 10, 16); err == nil {
		return nil
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%q: want a bare port or IP:port", addr)
	}
	if net.ParseIP(host) == nil {
		return fmt.Errorf("%q: hostnames are not supported, TigerBeetle requires an IP literal", addr)
	}
	return nil
}
