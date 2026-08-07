package codec

import (
	"fmt"

	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/model"
)

// Decoder превращает сырой payload сообщения в команду.
// Любая ошибка декодинга — poison: ретрай её не исправит.
type Decoder interface {
	Decode(payload []byte) (*model.Command, error)
}

// Registry выбирает декодер по имени топика.
type Registry map[string]Decoder

func (r Registry) For(topic string) (Decoder, error) {
	d, ok := r[topic]
	if !ok {
		return nil, fmt.Errorf("no decoder registered for topic %q", topic)
	}
	return d, nil
}

// NewRegistry строит декодеры для всех топиков из конфига.
// Пока поддержан только json; другой codec отклоняется валидацией конфига.
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
