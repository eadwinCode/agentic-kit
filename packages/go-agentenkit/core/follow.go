package core

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"

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
func FollowEvents(ctx context.Context, deps ports.RuntimePorts, threadID string, opts FollowOptions) (*EventStream, error) {
	var (
		mu      sync.Mutex
		live    bool
		pending []ports.AgentEvent // published while the replay is still running
		queue   []ports.AgentEvent // published once live, waiting for the consumer
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
		// Rules 2 and 3 in one place, so no caller has to remember them.
		admit := func(e ports.AgentEvent) bool {
			if e.Seq == 0 {
				return true // a notice: forward, but do not advance
			}
			if e.Seq <= lastSeq {
				return false
			}
			lastSeq = e.Seq
			return true
		}
		emit := func(e ports.AgentEvent) bool {
			select {
			case s.ch <- e:
				return true
			case <-ctx.Done():
				return false
			}
		}
		if ctx.Err() != nil {
			return
		}
		// …then the durable log…
		replay, err := deps.Storage.Events.ListSince(ctx, threadID, opts.Since)
		if err != nil {
			s.setErr(err)
			return
		}
		for _, e := range replay {
			if admit(e) && !emit(e) {
				return
			}
		}
		// …then whatever arrived behind it, in order.
		mu.Lock()
		behind := pending
		pending = nil
		live = true
		mu.Unlock()
		sort.SliceStable(behind, func(i, j int) bool { return behind[i].Seq < behind[j].Seq })
		for _, e := range behind {
			if admit(e) && !emit(e) {
				return
			}
		}
		for {
			mu.Lock()
			batch := queue
			queue = nil
			mu.Unlock()
			for _, e := range batch {
				if admit(e) && !emit(e) {
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
