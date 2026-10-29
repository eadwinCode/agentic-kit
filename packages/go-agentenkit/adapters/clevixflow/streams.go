package clevixflow

import (
	"context"
	"encoding/json"
	"errors"
	"iter"
	"time"

	flow "github.com/eadwinCode/flow-sdk"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// StreamID is the Flow stream a run stream is kept in.
func StreamID(streamID string) string { return flowName("agentenkit.", streamID) }

// Flow's limits on one stream call.
const (
	maxAppendItems = 100
	maxAppendBytes = 900_000
	maxStreamTime  = 7 * 24 * time.Hour
)

// streamRef is an item too big for Flow: the kv key that holds the event.
type streamRef struct {
	AgentenkitRef string `json:"agentenkitRef"`
}

// StreamsOptions tune the run streams.
type StreamsOptions struct {
	// Kv holds items too big for one stream item (over about 60 KB).
	// Required.
	Kv ports.Kv
	// RefTTL is how long such an item stays in the kv. Default 8 days: past
	// Flow's longest time to live, so it outlives its stream.
	RefTTL time.Duration
}

// Streams is RunStreams over Clevix Flow streams.
//
// Flow keeps the items, gives offsets, and lets a reader start from any
// offset and follow live, which is what a run stream needs. An item over
// about 60 KB is kept in the kv and the stream carries its key; readers get
// the event back. Flow keeps a stream at most 7 days and a grace window at
// most 7 days, so longer times are cut to that. A stream holds at most
// 20,000 items and 32 MB; an append past that fails.
type Streams struct {
	client *flow.Client
	opts   StreamsOptions
}

// NewStreams makes the run streams. opts.Kv is required.
func NewStreams(client *flow.Client, opts StreamsOptions) (*Streams, error) {
	if opts.Kv == nil {
		return nil, errors.New("clevixflow: NewStreams needs a Kv")
	}
	if opts.RefTTL <= 0 {
		opts.RefTTL = 8 * 24 * time.Hour
	}
	return &Streams{client: client, opts: opts}, nil
}

// Open opens the stream. Opening one that exists is a no-op.
func (s *Streams) Open(ctx context.Context, streamID string, meta ports.StreamMeta, ttl time.Duration) error {
	ttl = min(maxStreamTime, max(time.Second, ttl))
	_, err := s.client.Streams().Open(ctx, StreamID(streamID), flow.StreamOpenOpts{Meta: meta, TTL: ttl})
	return mapStreamError(err)
}

// Append appends in order, in batches Flow takes: 100 items and under 1 MB
// a call.
func (s *Streams) Append(ctx context.Context, streamID string, events []ports.StreamEvent) ([]string, error) {
	if len(events) == 0 {
		return nil, nil
	}
	id := StreamID(streamID)
	var offsets []string
	var batch []any
	size := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		got, err := s.client.Streams().Append(ctx, id, batch...)
		if err != nil {
			return mapStreamError(err)
		}
		offsets = append(offsets, got...)
		batch, size = nil, 0
		return nil
	}
	for _, e := range events {
		raw, err := ports.EncodeStreamEvent(e)
		if err != nil {
			return nil, err
		}
		item := json.RawMessage(raw)
		if len(raw) > maxInlineBytes {
			ref := streamRef{AgentenkitRef: "agentenkit:flowstream:" + streamID + ":" + randomKey()}
			if _, err := s.opts.Kv.Set(ctx, ref.AgentenkitRef, string(raw), ports.SetOptions{Expiry: s.opts.RefTTL}); err != nil {
				return nil, err
			}
			if item, err = json.Marshal(ref); err != nil {
				return nil, err
			}
		}
		if len(batch) == maxAppendItems || size+len(item) > maxAppendBytes {
			if err := flush(); err != nil {
				return nil, err
			}
		}
		batch = append(batch, item)
		size += len(item)
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return offsets, nil
}

// Read yields the items after `after`, then live ones, until the end item.
func (s *Streams) Read(ctx context.Context, streamID, after string) iter.Seq2[ports.StreamItem, error] {
	return func(yield func(ports.StreamItem, error) bool) {
		for item, err := range s.client.Streams().Read(ctx, StreamID(streamID), after) {
			if err != nil {
				yield(ports.StreamItem{}, mapStreamError(err))
				return
			}
			decoded, ok, err := s.decode(ctx, item)
			if err != nil {
				yield(ports.StreamItem{}, err)
				return
			}
			if ok && !yield(decoded, nil) {
				return
			}
		}
	}
}

// Snapshot returns the items after `after` now. Nil, nil when the stream
// does not exist.
func (s *Streams) Snapshot(ctx context.Context, streamID, after string) (*ports.StreamSnapshot, error) {
	snap, err := s.client.Streams().Snapshot(ctx, StreamID(streamID), after)
	if err != nil || snap == nil {
		return nil, mapStreamError(err)
	}
	out := &ports.StreamSnapshot{Items: []ports.StreamItem{}}
	_ = json.Unmarshal(snap.Meta, &out.Meta)
	for _, item := range snap.Items {
		decoded, ok, err := s.decode(ctx, item)
		if err != nil {
			return nil, err
		}
		if ok {
			out.Items = append(out.Items, decoded)
		}
	}
	if n := len(out.Items); snap.Ended && n > 0 {
		if end, ok := out.Items[n-1].Event.(ports.StreamEnd); ok {
			out.End = end
		}
	}
	return out, nil
}

// Close writes the end item and starts the grace window.
func (s *Streams) Close(ctx context.Context, streamID string, end ports.StreamEnd, grace time.Duration) error {
	raw, err := ports.EncodeStreamEvent(end)
	if err != nil {
		return err
	}
	grace = min(maxStreamTime, max(0, grace))
	_, err = s.client.Streams().Close(ctx, StreamID(streamID), json.RawMessage(raw), grace)
	return mapStreamError(err)
}

// Delete removes the stream now. A missing one is a no-op.
func (s *Streams) Delete(ctx context.Context, streamID string) error {
	_, err := s.client.Streams().Delete(ctx, StreamID(streamID))
	return mapStreamError(err)
}

// decode turns a Flow item into a run stream item: the event, read back from
// the kv when the stream holds its key. ok is false when that kv copy has
// expired.
func (s *Streams) decode(ctx context.Context, item flow.StreamItem) (ports.StreamItem, bool, error) {
	raw := []byte(item.Item)
	var ref streamRef
	if json.Unmarshal(raw, &ref) == nil && ref.AgentenkitRef != "" {
		value, found, err := s.opts.Kv.Get(ctx, ref.AgentenkitRef)
		if err != nil {
			return ports.StreamItem{}, false, err
		}
		if !found {
			return ports.StreamItem{}, false, nil
		}
		raw = []byte(value)
	}
	e, err := ports.DecodeStreamEvent(raw)
	if err != nil {
		return ports.StreamItem{}, false, err
	}
	return ports.StreamItem{Offset: item.Offset, Event: e}, true, nil
}

// mapStreamError turns Flow's stream errors into the port's.
func mapStreamError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, flow.ErrStreamGone):
		return ports.ErrStreamGone
	case errors.Is(err, flow.ErrStreamClosed):
		return ports.ErrStreamClosed
	}
	return err
}
