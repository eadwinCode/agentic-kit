package memory

import (
	"context"
	"iter"
	"strconv"
	"sync"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

type memStream struct {
	meta      ports.StreamMeta
	items     []ports.StreamItem
	end       ports.StreamEnd
	expiresAt time.Time
	// changed is closed, and replaced, whenever the stream changes or goes:
	// every waiting reader wakes at once.
	changed chan struct{}
}

// RunStreams is an in-memory RunStreams for tests and local prototyping.
// The offset is the item's 1-based position. Expiry is checked on every
// call, and an Open drops every expired stream, so a dev server that runs
// for days holds only live and recent streams.
type RunStreams struct {
	mu      sync.Mutex
	streams map[string]*memStream
}

var _ ports.RunStreams = (*RunStreams)(nil)

// NewRunStreams makes an empty RunStreams.
func NewRunStreams() *RunStreams { return &RunStreams{streams: map[string]*memStream{}} }

// live returns the stream, or nil when it is missing or expired. Called
// with s.mu held.
func (r *RunStreams) live(streamID string) *memStream {
	s := r.streams[streamID]
	if s == nil {
		return nil
	}
	if !time.Now().Before(s.expiresAt) {
		r.drop(streamID, s)
		return nil
	}
	return s
}

// drop removes a stream and wakes its readers, who find it gone. Called
// with r.mu held.
func (r *RunStreams) drop(streamID string, s *memStream) {
	delete(r.streams, streamID)
	close(s.changed)
}

func (s *memStream) wake() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func (r *RunStreams) Open(_ context.Context, streamID string, meta ports.StreamMeta, ttl time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	for id, s := range r.streams {
		if !now.Before(s.expiresAt) {
			r.drop(id, s)
		}
	}
	if _, ok := r.streams[streamID]; ok {
		return nil
	}
	r.streams[streamID] = &memStream{meta: meta, expiresAt: now.Add(ttl), changed: make(chan struct{})}
	return nil
}

func (r *RunStreams) Append(_ context.Context, streamID string, events []ports.StreamEvent) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.live(streamID)
	if s == nil {
		return nil, ports.ErrStreamGone
	}
	if s.end != nil {
		return nil, ports.ErrStreamClosed
	}
	offsets := make([]string, 0, len(events))
	for _, e := range events {
		offset := strconv.Itoa(len(s.items) + 1)
		s.items = append(s.items, ports.StreamItem{Offset: offset, Event: e})
		offsets = append(offsets, offset)
	}
	s.wake()
	return offsets, nil
}

func (r *RunStreams) Read(ctx context.Context, streamID, after string) iter.Seq2[ports.StreamItem, error] {
	return func(yield func(ports.StreamItem, error) bool) {
		next, _ := strconv.Atoi(after)
		for {
			if ctx.Err() != nil {
				return
			}
			r.mu.Lock()
			s := r.live(streamID)
			if s == nil {
				r.mu.Unlock()
				yield(ports.StreamItem{}, ports.ErrStreamGone)
				return
			}
			var batch []ports.StreamItem
			if next < len(s.items) {
				batch = append(batch, s.items[next:]...)
				next = len(s.items)
			}
			ended, changed := s.end != nil, s.changed
			r.mu.Unlock()
			for _, item := range batch {
				if !yield(item, nil) || ports.IsStreamEnd(item.Event) {
					return
				}
			}
			// Read from past the end item: nothing more will ever come.
			if ended {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-changed:
			}
		}
	}
}

func (r *RunStreams) Snapshot(_ context.Context, streamID, after string) (*ports.StreamSnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.live(streamID)
	if s == nil {
		return nil, nil
	}
	from, _ := strconv.Atoi(after)
	from = min(from, len(s.items))
	return &ports.StreamSnapshot{Meta: s.meta, Items: append([]ports.StreamItem{}, s.items[from:]...), End: s.end}, nil
}

func (r *RunStreams) Close(_ context.Context, streamID string, end ports.StreamEnd, grace time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.live(streamID)
	if s == nil {
		return ports.ErrStreamGone
	}
	if s.end != nil {
		return nil
	}
	s.end = end
	s.items = append(s.items, ports.StreamItem{Offset: strconv.Itoa(len(s.items) + 1), Event: end})
	s.expiresAt = time.Now().Add(grace)
	s.wake()
	return nil
}

func (r *RunStreams) Delete(_ context.Context, streamID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s := r.streams[streamID]; s != nil {
		r.drop(streamID, s)
	}
	return nil
}

// Len is how many streams are held now, expired ones included until the
// next sweep. Lets a test prove a stream was deleted.
func (r *RunStreams) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.streams)
}
