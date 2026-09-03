package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// NotifyChannel is the one Postgres channel every thread's events travel on.
// LISTEN is per connection, not per channel-pattern, so one channel and an
// in-process fan-out by thread id costs one connection per process however
// many threads it watches.
const NotifyChannel = "agentenkit_events"

// notifyPayloadCap is Postgres's NOTIFY limit (8000 bytes) with room for the
// envelope. A frame that does not fit travels as a reference instead.
const notifyPayloadCap = 7_500

// referenceTTL is how long an oversized notice waits in the kv for its
// subscribers.
const referenceTTL = time.Minute

// Listener is what the bus needs from a driver: one long-lived LISTEN
// connection that hands over every payload on a channel. The package never
// imports a driver; pgxlisten implements this over pgx.
type Listener interface {
	// Listen subscribes to channel and calls handler for every notification
	// until ctx ends. It reconnects on its own; a payload lost while it was
	// down is recovered by the client's cursor replay, which is what an
	// at-most-once bus is for (§2.2).
	Listen(ctx context.Context, channel string, handler func(payload string)) error
}

// Bus is an EventBus over LISTEN/NOTIFY.
//
// At-most-once by contract, so a dropped notification is fine; the durable
// log is the record. What the adapter must get right is the size cap: a
// CHUNK carrying a tool result can exceed 8000 bytes, and NOTIFY refuses
// it. Such an event is sent as {threadId, seq} and the subscriber reads it
// back from Storage.Events; a notice (seq 0) that is too large is parked in
// the kv under a short TTL and referenced by key.
type Bus struct {
	db       *sql.DB
	kv       *Kv
	events   ports.EventStore
	listener Listener

	mu      sync.Mutex
	subs    map[string]map[int]func(ports.AgentEvent)
	nextID  int
	started bool
	// heartbeat drives the §2.5 watchdog on every subscription.
	heartbeat time.Duration
}

// BusOptions tunes NewBus.
type BusOptions struct {
	// Heartbeat is the interval of the bus-only HEARTBEAT notice each
	// subscription emits. Zero means one minute.
	Heartbeat time.Duration
}

// NewBus builds the bus. Publishing goes through db (pg_notify); listening
// through listener. events resolves oversized durable frames and kv parks
// oversized notices; pass the storage's Events() and the NewKv Kv.
func NewBus(db *sql.DB, listener Listener, events ports.EventStore, kv *Kv, opts BusOptions) *Bus {
	hb := opts.Heartbeat
	if hb <= 0 {
		hb = time.Minute
	}
	return &Bus{db: db, kv: kv, events: events, listener: listener, subs: map[string]map[int]func(ports.AgentEvent){}, heartbeat: hb}
}

// frame is what travels over NOTIFY: the event itself when it fits, else a
// pointer to where it can be read.
type frame struct {
	Event    *ports.AgentEvent `json:"event,omitempty"`
	ThreadID string            `json:"threadId,omitempty"`
	Seq      int64             `json:"seq,omitempty"`
	Ref      string            `json:"ref,omitempty"`
}

func referenceKey(threadID string) string {
	return "agentenkit:notice:" + threadID + ":" + fmt.Sprint(time.Now().UnixNano())
}

func (b *Bus) Publish(ctx context.Context, threadID string, event ports.AgentEvent) error {
	body, err := json.Marshal(frame{Event: &event})
	if err != nil {
		return err
	}
	if len(body) > notifyPayloadCap {
		f := frame{ThreadID: threadID, Seq: event.Seq}
		if event.Seq == 0 {
			// A notice is nowhere durable: park it so the subscriber can
			// still read it, briefly.
			raw, _ := json.Marshal(event)
			f.Ref = referenceKey(threadID)
			if _, err := b.kv.Set(ctx, f.Ref, string(raw), ports.SetOptions{Expiry: referenceTTL}); err != nil {
				return err
			}
		}
		if body, err = json.Marshal(f); err != nil {
			return err
		}
	}
	_, err = b.db.ExecContext(ctx, `SELECT pg_notify($1, $2)`, NotifyChannel, string(body))
	return err
}

// resolve turns a frame back into the event it stands for.
func (b *Bus) resolve(ctx context.Context, f frame) (ports.AgentEvent, bool) {
	if f.Event != nil {
		return *f.Event, true
	}
	if f.Ref != "" {
		raw, found, err := b.kv.Get(ctx, f.Ref)
		if err != nil || !found {
			return ports.AgentEvent{}, false
		}
		var e ports.AgentEvent
		if json.Unmarshal([]byte(raw), &e) != nil {
			return ports.AgentEvent{}, false
		}
		return e, true
	}
	if f.ThreadID != "" && f.Seq > 0 {
		rows, err := b.events.ListSince(ctx, f.ThreadID, f.Seq-1, ports.StorageContext{})
		if err != nil {
			return ports.AgentEvent{}, false
		}
		for _, e := range rows {
			if e.Seq == f.Seq {
				return e, true
			}
		}
	}
	return ports.AgentEvent{}, false
}

// start opens the one LISTEN connection, on first subscribe.
func (b *Bus) start() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.started {
		return
	}
	b.started = true
	go func() {
		_ = b.listener.Listen(context.Background(), NotifyChannel, func(payload string) {
			var f frame
			if json.Unmarshal([]byte(payload), &f) != nil {
				return // a malformed frame never kills the listener
			}
			threadID := f.ThreadID
			if f.Event != nil {
				threadID = f.Event.ThreadID
			}
			b.mu.Lock()
			handlers := make([]func(ports.AgentEvent), 0, len(b.subs[threadID]))
			for _, h := range b.subs[threadID] {
				handlers = append(handlers, h)
			}
			b.mu.Unlock()
			if len(handlers) == 0 {
				return // nobody here is watching that thread
			}
			e, ok := b.resolve(context.Background(), f)
			if !ok {
				return
			}
			for _, h := range handlers {
				h(e)
			}
		})
	}()
}

func (b *Bus) Subscribe(ctx context.Context, threadID string, handler func(ports.AgentEvent)) (func() error, error) {
	b.start()
	b.mu.Lock()
	b.nextID++
	id := b.nextID
	if b.subs[threadID] == nil {
		b.subs[threadID] = map[int]func(ports.AgentEvent){}
	}
	b.subs[threadID][id] = handler
	b.mu.Unlock()

	done := make(chan struct{})
	var once sync.Once
	go func() {
		ticker := time.NewTicker(b.heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				handler(ports.AgentEvent{ThreadID: threadID, Seq: 0, Type: "HEARTBEAT", Payload: json.RawMessage("null"), CreatedAt: time.Now()})
			}
		}
	}()
	return func() error {
		once.Do(func() {
			close(done)
			b.mu.Lock()
			delete(b.subs[threadID], id)
			if len(b.subs[threadID]) == 0 {
				delete(b.subs, threadID)
			}
			b.mu.Unlock()
		})
		return nil
	}, nil
}
