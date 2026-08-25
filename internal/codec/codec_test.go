package codec

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/stretchr/testify/require"
)

// stubDecoder is a Decoder identified by name, so that a test can tell which
// decoder a registry handed back.
type stubDecoder struct{ name string }

func (stubDecoder) Decode([]byte) (*model.Command, error) { return nil, nil }

// A topic with no decoder must be an error rather than a nil Decoder: the
// sink calls For before every decode, and a nil returned as "fine" would
// panic one frame later instead of reporting an unknown topic.
func TestRegistryForUnknownTopic(t *testing.T) {
	r := Registry{"payments": stubDecoder{"json"}}
	d, err := r.For("refunds")
	require.Nil(t, d)
	require.ErrorContains(t, err, "no decoder registered")
	require.ErrorContains(t, err, `"refunds"`, "the error must name the topic")
}

func TestRegistryForReturnsThePerTopicDecoder(t *testing.T) {
	a, b := stubDecoder{"a"}, stubDecoder{"b"}
	r := Registry{"payments": a, "refunds": b}

	got, err := r.For("payments")
	require.NoError(t, err)
	require.Equal(t, a, got)

	got, err = r.For("refunds")
	require.NoError(t, err)
	require.Equal(t, b, got, "each topic keeps its own decoder")
}

// NewRegistry builds one decoder per topic, passing that topic's codec name.
func TestNewRegistryBuildsOneDecoderPerTopic(t *testing.T) {
	var asked []string
	r, err := NewRegistry(
		[]config.Topic{{Name: "payments", Codec: "json"}, {Name: "refunds", Codec: "json"}},
		func(c string) (Decoder, error) {
			asked = append(asked, c)
			return stubDecoder{c}, nil
		})
	require.NoError(t, err)
	require.Len(t, r, 2)
	require.Equal(t, []string{"json", "json"}, asked)

	for _, topic := range []string{"payments", "refunds"} {
		d, ferr := r.For(topic)
		require.NoError(t, ferr)
		require.NotNil(t, d)
	}
}

// A codec the builder rejects fails the whole registry, and the error says
// which topic caused it: config validation runs before this, so reaching here
// means a build-time defect an operator has to be able to locate.
func TestNewRegistryReportsTheOffendingTopic(t *testing.T) {
	r, err := NewRegistry(
		[]config.Topic{{Name: "payments", Codec: "json"}, {Name: "refunds", Codec: "avro"}},
		func(c string) (Decoder, error) {
			if c != "json" {
				return nil, fmt.Errorf("unsupported codec %q", c)
			}
			return stubDecoder{c}, nil
		})
	require.Nil(t, r, "a partially built registry must not be returned")
	require.ErrorContains(t, err, "topic refunds")
	require.ErrorContains(t, err, `unsupported codec "avro"`)
}

func TestNewRegistryWithNoTopics(t *testing.T) {
	r, err := NewRegistry(nil, func(string) (Decoder, error) {
		t.Fatal("build must not be called when there are no topics")
		return nil, nil
	})
	require.NoError(t, err)
	require.Empty(t, r)
}

func TestPoisonFormatsItsMessage(t *testing.T) {
	err := Poison("transfers[%d]: %v", 3, "bad amount")
	require.EqualError(t, err, "transfers[3]: bad amount")

	var p *PoisonError
	require.ErrorAs(t, err, &p)
	require.Equal(t, "transfers[3]: bad amount", p.Detail)
}

// IsPoison has to see through wrapping: an error is routed by what it is, not
// by how many layers of context were added on the way up.
func TestIsPoison(t *testing.T) {
	require.True(t, IsPoison(Poison("bad json")))
	require.True(t, IsPoison(fmt.Errorf("decoding record: %w", Poison("bad json"))),
		"wrapping must not hide a PoisonError")
	require.True(t, IsPoison(&PoisonError{Detail: "constructed directly"}))

	require.False(t, IsPoison(nil))
	require.False(t, IsPoison(errors.New("broker unreachable")),
		"an infrastructural error must not be mistaken for poison")
	require.False(t, IsPoison(fmt.Errorf("wrapped: %w", errors.New("broker unreachable"))))
}

// IsPoison is deliberately not consulted by the sink, which routes every
// decoder error to the DLQ regardless (see Decoder's contract and
// sink.TestHandleAnyDecodeErrorIsPoison). That is the safe direction: a
// decoder that broke its contract and returned a plain error would, under an
// IsPoison check, be retried forever and wedge the partition, whereas the
// unconditional rule costs at worst one DLQ entry for a record that was never
// going to decode. What IsPoison is for is the other side of that contract --
// letting a decoder's own tests assert that everything it returns really is
// poison, which is what makes the sink's shortcut sound. This test pins the
// property those callers rely on: the predicate is exact, matching only
// PoisonError and not merely any error carrying a message.
func TestIsPoisonIsExactRatherThanHeuristic(t *testing.T) {
	lookalike := errors.New("json: cannot unmarshal string into uint64")
	require.False(t, IsPoison(lookalike),
		"IsPoison must key on the type, not on what the message looks like")
}
