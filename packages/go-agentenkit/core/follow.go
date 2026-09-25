package core

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// FollowOptions tunes FollowEvents. The context passed to FollowEvents is
// the abort signal: cancel it, or the subscription outlives the client.
type FollowOptions struct {
	// Since resumes after this seq. 0 replays the thread from the start. It
	// comes from the client's cursor: Last-Event-ID for SSE.
	Since int64
}

// EventStream is every event on a thread, replay first and then live, as one
// sequence. Read Events() until it closes, then check Err().
type EventStream struct {
	ch  chan ports.AgentEvent
	err error
	mu  sync.Mutex
}

// Events is the channel of admitted events. It closes when the context is
// cancelled or the replay failed.
func (s *EventStream) Events() <-chan ports.AgentEvent { return s.ch }

// Err reports why the stream stopped, once Events() is closed. Nil for a
// plain cancellation.
func (s *EventStream) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *EventStream) setErr(err error) {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
}

// maxLiveQueue caps the events a follower holds for a slow consumer. Past
// it the queue is dropped and the follower reads what it missed back from
// storage instead: the log has every durable event, so nothing is lost but
// notices, which nobody needs twice.
const maxLiveQueue = 10_000

// gapRetries and gapWait bound how long a follower waits for a missing seq
// to reach storage: another process may have taken seq N, and still be
// writing it, when seq N+1 arrives on the bus.
const (
	gapRetries = 3
	gapWait    = 50 * time.Millisecond
)

// FollowEvents streams a thread's events, replay then live.
//
// The ordering here is the whole point, and it is easy to get wrong in a
// route handler:
//
//  1. Subscribe before replaying. An event published between the replay
//     finishing and the tail starting is otherwise lost for ever.
//  2. Never emit at or below the cursor. The client would render it twice.
//  3. seq == 0 is a bus-only notice (heartbeats, death notices). Always
//     forward it, never let it move the cursor.
//  4. Never skip a seq. The bus is at-most-once and does not promise order:
//     an event can be dropped, or arrive after the one published behind it.
//     An event that jumps past the next seq first has the gap read back from
//     storage, in order.
func FollowEvents(ctx context.Context, deps ports.RuntimePorts, threadID string, opts FollowOptions) (*EventStream, error) {
	var (
		mu       sync.Mutex
		live     bool
		pending  []ports.AgentEvent // published while the replay is still running
		queue    []ports.AgentEvent // published once live, waiting for the consumer
		overflow bool               // the queue was dropped: read storage instead
	)
	wake := make(chan struct{}, 1)
	notify := func() {
		select {
		case wake <- struct{}{}:
		default:
		}
	}

	// Rule 1: subscribe FIRST.
	unsubscribe, err := deps.Bus.Subscribe(ctx, threadID, func(e ports.AgentEvent) {
		mu.Lock()
		if !live {
			pending = append(pending, e)
			mu.Unlock()
			return
		}
		if len(queue) >= maxLiveQueue {
			queue, overflow = nil, true
		}
		queue = append(queue, e)
		mu.Unlock()
		notify()
	})
	if err != nil {
		return nil, err
	}

	s := &EventStream{ch: make(chan ports.AgentEvent)}
	go func() {
		// Runs on cancel, on error, and on a finished replay alike: a
		// subscription that outlives its reader is a leak on every reconnect.
		defer func() {
			_ = unsubscribe()
			close(s.ch)
		}()
		lastSeq := opts.Since
		emit := func(e ports.AgentEvent) bool {
			select {
			case s.ch <- e:
				return true
			case <-ctx.Done():
				return false
			}
		}
		// fromStorage emits every stored event after lastSeq and before
		// upTo (0 = all of them), in order.
		fromStorage := func(upTo int64) (bool, error) {
			stored, err := deps.Storage.Events.ListSince(ctx, threadID, lastSeq)
			if err != nil {
				return false, err
			}
			for _, e := range stored {
				if upTo > 0 && e.Seq >= upTo {
					break
				}
				if e.Seq > lastSeq {
					if !emit(e) {
						return false, nil
					}
					lastSeq = e.Seq
				}
			}
			return true, nil
		}
		// Rules 2 to 4 in one place, so no caller has to remember them.
		deliver := func(e ports.AgentEvent) bool {
			if e.Seq == 0 {
				return emit(e) // a notice: forward, but do not advance
			}
			for try := 0; e.Seq > max(lastSeq, 0)+1 && try < gapRetries; try++ {
				if try > 0 {
					select {
					case <-time.After(gapWait):
					case <-ctx.Done():
						return false
					}
				}
				ok, err := fromStorage(e.Seq)
				if err != nil {
					break // the live event still goes out; the gap stays
				}
				if !ok {
					return false
				}
			}
			if e.Seq <= lastSeq {
				return true
			}
			lastSeq = e.Seq
			return emit(e)
		}
		if ctx.Err() != nil {
			return
		}
		// …then the durable log…
		if ok, err := fromStorage(0); err != nil {
			s.setErr(err)
			return
		} else if !ok {
			return
		}
		// …then whatever arrived behind it, in order.
		mu.Lock()
		behind := pending
		pending = nil
		live = true
		mu.Unlock()
		sort.SliceStable(behind, func(i, j int) bool { return behind[i].Seq < behind[j].Seq })
		for _, e := range behind {
			if !deliver(e) {
				return
			}
		}
		for {
			mu.Lock()
			batch, dropped := queue, overflow
			queue, overflow = nil, false
			mu.Unlock()
			if dropped {
				// The consumer fell too far behind: catch up from the log.
				if ok, err := fromStorage(0); err != nil || !ok {
					if err != nil {
						s.setErr(err)
					}
					return
				}
			}
			for _, e := range batch {
				if !deliver(e) {
					return
				}
			}
			select {
			case <-wake:
			case <-ctx.Done():
				return
			}
		}
	}()
	return s, nil
}

// SSEHeaders are the headers an SSE response needs. X-Accel-Buffering is for
// nginx, which otherwise buffers the stream.
var SSEHeaders = map[string]string{
	"Content-Type":      "text/event-stream; charset=utf-8",
	"Cache-Control":     "no-cache, no-transform",
	"Connection":        "keep-alive",
	"X-Accel-Buffering": "no",
}

// SSEFrame encodes one event as an SSE frame.
//
// A bus-only notice (seq 0) is sent WITHOUT an id line. EventSource stores
// any id it sees and sends it back as Last-Event-ID on reconnect, so
// stamping id: 0 on a heartbeat would rewind the client's cursor to the
// beginning of the thread and replay everything.
func SSEFrame(e ports.AgentEvent) string {
	data, _ := json.Marshal(e)
	if e.Seq == 0 {
		return fmt.Sprintf("data: %s\n\n", data)
	}
	return fmt.Sprintf("id: %d\ndata: %s\n\n", e.Seq, data)
}

// SSEOptions tunes ToSSEStream.
type SSEOptions struct {
	FollowOptions
	// RetryMs is emitted once, up front: how long a browser waits before
	// reconnecting. Zero omits it.
	RetryMs int
}

// SSEStream is the event sequence, encoded as Server-Sent Events. It is a
// WriterTo and an http.Handler rather than a Response, because half the
// ecosystem has its own response type.
type SSEStream struct {
	Headers map[string]string
	events  *EventStream
	retryMs int
}

// ToSSEStream wraps an EventStream for SSE.
func ToSSEStream(events *EventStream, opts SSEOptions) *SSEStream {
	return &SSEStream{Headers: SSEHeaders, events: events, retryMs: opts.RetryMs}
}

// Err reports why the underlying stream stopped.
func (s *SSEStream) Err() error { return s.events.Err() }

// WriteTo writes frames until the stream ends, flushing after each one when
// w is an http.Flusher.
func (s *SSEStream) WriteTo(w io.Writer) (int64, error) {
	flusher, _ := w.(http.Flusher)
	var total int64
	write := func(frame string) error {
		n, err := io.WriteString(w, frame)
		total += int64(n)
		if err != nil {
			return err
		}
		if flusher != nil {
			flusher.Flush()
		}
		return nil
	}
	if s.retryMs > 0 {
		if err := write(fmt.Sprintf("retry: %d\n\n", s.retryMs)); err != nil {
			return total, err
		}
	}
	for e := range s.events.Events() {
		if err := write(SSEFrame(e)); err != nil {
			return total, err
		}
	}
	return total, s.events.Err()
}

// ServeHTTP sets the SSE headers and writes the stream. Create the stream
// with the request's context so a hang-up unsubscribes.
func (s *SSEStream) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	for k, v := range s.Headers {
		w.Header().Set(k, v)
	}
	w.WriteHeader(http.StatusOK)
	_, _ = s.WriteTo(w)
}
