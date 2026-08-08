package codec

import (
	"fmt"

	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/model"
)

// Decoder turns a message's raw payload into a command.
// Any decoding error is poison: a retry will not fix it.
type Decoder interface {
	Decode(payload []byte) (*model.Command, error)
}

// Registry selects a decoder by topic name.
type Registry map[string]Decoder

func (r Registry) For(topic string) (Decoder, error) {
	d, ok := r[topic]
	if !ok {
		return nil, fmt.Errorf("no decoder registered for topic %q", topic)
	}
	return d, nil
}

// NewRegistry builds decoders for every topic from the config.
// Only json is supported for now; any other codec is rejected by config validation.
func NewRegistry(topics []config.Topic, build func(codec string) (Decoder, error)) (Registry, error) {
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
