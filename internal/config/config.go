package config

import (
	"fmt"
	"net"
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
	// MetricsAddr serves /metrics, /healthz and /readyz. It is separate from
	// api.http_addr: that address is already owned by the grpc-gateway REST
	// mux in api/sink modes, and binding both to the same port would fail
	// at startup.
	MetricsAddr string `yaml:"metrics_addr"`
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
	if c.MetricsAddr == "" {
		return fmt.Errorf("metrics_addr: must not be empty")
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
