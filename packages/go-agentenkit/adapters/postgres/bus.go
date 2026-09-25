package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/core"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// NotifyChannel is the default Postgres channel every thread's events travel
// on. LISTEN is per connection, not per channel-pattern, so one channel and
// an in-process fan-out by thread id costs one connection per process
// however many threads it watches. BusOptions.Channel picks another one.
const NotifyChannel = "agentenkit_events"

// notifyPayloadCap is Postgres's NOTIFY limit (8000 bytes) with room for the
// envelope. A frame that does not fit travels as a reference instead.
const notifyPayloadCap = 7_500

// referenceTTL is how long an oversized event waits in the kv for its
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

// ConnectNotifier is what a Listener can add to that: a callback for every
// LISTEN connection it brings up, the first one included. The bus uses it
// to replay, from the durable log, whatever a dropped connection missed, so
// a missed NOTIFY can never stall an open stream. pgxlisten implements it.
type ConnectNotifier interface {
	OnConnect(fn func())
}

// Bus is an EventBus over LISTEN/NOTIFY.
//
// At-most-once by contract, so a dropped notification is fine; the durable
// log is the record. What the adapter must get right is the size cap: a
// CHUNK carrying a tool result can exceed 8000 bytes, and NOTIFY refuses
// it. Such an event, durable or not, is parked in the kv under a short TTL
// and travels as a reference to that key. The subscriber reads it back from
// the kv, which needs no storage scope: a storage that requires a tenant
// (§2.10) cannot be read from the listener, which serves every tenant.
//
// Each subscription remembers the last durable seq it delivered. When the
// LISTEN connection comes back after a drop, the bus replays what each
// subscriber missed from the durable store, scoped with the subscriber's
// own state, and delivers nothing twice.
type Bus struct {
	db       *sql.DB
	kv       *Kv
	events   ports.EventStore
	listener Listener

	mu      sync.Mutex
	subs    map[string]map[int]*subscription
	nextID  int
	started bool
	// heartbeat drives the §2.5 watchdog on every subscription.
	heartbeat time.Duration
	// channel is the NOTIFY channel, per deployment.
	channel string
}

// subQueueCap is how many events a subscription holds for a slow handler.
// Past it the subscription stops taking events and, once it catches up,
// reads what it missed from the durable log: at-most-once for notices,
// nothing lost that was stored.
const subQueueCap = 1024

// sweepEvery is how often the bus clears expired kv rows, its parked frames
// among them.
const sweepEvery = 5 * time.Minute

// subscription is one handler on one thread, with its own queue and its own
// goroutine: the one listener goroutine only ever hands events over, so a
// slow handler, or a slow replay, holds up nobody else.
type subscription struct {
	threadID string
	// ctx is the subscriber's own context. Its run state (§2.10) scopes the
	// replay's storage reads, and its end ends the subscription.
	ctx     context.Context
	handler func(ports.AgentEvent)
	queue   chan item
	done    chan struct{}
	// behind is set when the queue was full: the next item first catches up
	// from the log.
	behind atomic.Bool

	// lastSeq is the highest durable seq delivered so far; 0 before the
	// first. Only the subscription's goroutine touches it.
	lastSeq int64
	// anchor is the thread's seq counter when the subscription opened: where
	// a replay starts when nothing durable was delivered yet.
	anchor int64
}

// item is one thing for a subscription to do: deliver an event, read a
// frame whose kv copy is gone back from the log, or replay what a dropped
// connection missed.
type item struct {
	event  *ports.AgentEvent
	lookup *frame
	replay bool
}

// offer queues an item without ever blocking the listener.
func (s *subscription) offer(it item) {
	select {
	case s.queue <- it:
	default:
		s.behind.Store(true)
	}
}

// deliver hands an event to the handler, once: a durable event at or below
// the cursor was delivered already, live or by a replay. Notices (seq 0)
// always go through and never move the cursor.
func (s *subscription) deliver(e ports.AgentEvent) {
	if e.Seq > 0 {
		if e.Seq <= s.lastSeq {
			return
		}
		s.lastSeq = e.Seq
	}
	s.handler(e)
}

// replayFrom is the cursor a replay resumes after.
func (s *subscription) replayFrom() int64 {
	if s.lastSeq > 0 {
		return s.lastSeq
	}
	// Nothing delivered yet: start just before the thread's seq at subscribe
	// time. The event at that seq may not have been durable yet when the
	// subscriber replayed the log itself, so it is included; a consumer
	// keeping a cursor (FollowEvents does) drops the copy it already has.
	return max(s.anchor-1, 0)
}

// scope is the storage context the subscriber's replay reads with.
func (s *subscription) scope() ports.StorageContext {
	return ports.StorageContext{State: core.RunStateFromContext(s.ctx)}
}

// run is the subscription's goroutine.
func (b *Bus) run(s *subscription) {
	for {
		select {
		case <-s.done:
			return
		case <-s.ctx.Done():
			return
		case it := <-s.queue:
			if it.replay || s.behind.Swap(false) {
				b.replay(s)
			}
			switch {
			case it.event != nil:
				s.deliver(*it.event)
			case it.lookup != nil:
				if e, ok := b.lookup(s, *it.lookup); ok {
					s.deliver(e)
				}
			}
		}
	}
}

// replay reads what the subscription missed from the log, from its own
// cursor, with its own scope.
func (b *Bus) replay(s *subscription) {
	if s.ctx.Err() != nil {
		return
	}
	rows, err := b.events.ListSince(s.ctx, s.threadID, s.replayFrom(), s.scope())
	if err != nil {
		return // the next event, or the client's own reconnect, catches up
	}
	for _, e := range rows {
		s.deliver(e)
	}
}

// BusOptions tunes NewBus.
type BusOptions struct {
	// Heartbeat is the interval of the bus-only HEARTBEAT notice each
	// subscription emits. Zero means one minute.
	Heartbeat time.Duration
	// Channel is the NOTIFY channel this bus publishes and listens on. Two
	// deployments sharing one database must not share a channel, or each
	// delivers the other's events. Empty means NotifyChannel.
	Channel string
}

// NewBus builds the bus. Publishing goes through db (pg_notify); listening
// through listener. kv parks oversized events; events is the durable store
// a reconnect replays from, and the fallback for a reference the kv no
// longer has. Pass the storage's Events() and the NewKv Kv.
func NewBus(db *sql.DB, listener Listener, events ports.EventStore, kv *Kv, opts BusOptions) *Bus {
	hb := opts.Heartbeat
	if hb <= 0 {
		hb = time.Minute
	}
	channel := opts.Channel
	if channel == "" {
		channel = NotifyChannel
	}
	return &Bus{db: db, kv: kv, events: events, listener: listener, subs: map[string]map[int]*subscription{}, heartbeat: hb, channel: channel}
}

// frame is what travels over NOTIFY: the event itself when it fits, else a
// reference to the kv key holding it. ThreadID and Seq name the event for
// the fallback read from the log, should the reference have expired.
type frame struct {
	Event    *ports.AgentEvent `json:"event,omitempty"`
	ThreadID string            `json:"threadId,omitempty"`
	Seq      int64             `json:"seq,omitempty"`
	Ref      string            `json:"ref,omitempty"`
}

func referenceKey(threadID string) string {
	return "agentenkit:frame:" + threadID + ":" + fmt.Sprint(time.Now().UnixNano())
}

func (b *Bus) Publish(ctx context.Context, threadID string, event ports.AgentEvent) error {
	body, err := json.Marshal(frame{Event: &event})
	if err != nil {
		return err
	}
	if len(body) > notifyPayloadCap {
		// Too big for NOTIFY: park it, and send the key. The kv needs no
		// storage scope to read back, which the listener could not supply.
		raw, _ := json.Marshal(event)
		f := frame{ThreadID: threadID, Seq: event.Seq, Ref: referenceKey(threadID)}
		if _, err := b.kv.Set(ctx, f.Ref, string(raw), ports.SetOptions{Expiry: referenceTTL}); err != nil {
			return err
		}
		if body, err = json.Marshal(f); err != nil {
			return err
		}
	}
	_, err = b.db.ExecContext(ctx, `SELECT pg_notify($1, $2)`, b.channel, string(body))
	return err
}

// resolve turns a frame back into the event it stands for: the event
// itself, or the kv copy it references.
func (b *Bus) resolve(ctx context.Context, f frame) (ports.AgentEvent, bool) {
	if f.Event != nil {
		return *f.Event, true
	}
	if f.Ref == "" {
		return ports.AgentEvent{}, false
	}
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

// lookup is the fallback for a durable frame whose kv copy is gone: read it
// from the log, scoped the way this subscriber reads.
func (b *Bus) lookup(s *subscription, f frame) (ports.AgentEvent, bool) {
	if f.ThreadID == "" || f.Seq <= 0 || s.ctx.Err() != nil {
		return ports.AgentEvent{}, false
	}
	rows, err := b.events.ListSince(s.ctx, f.ThreadID, f.Seq-1, s.scope())
	if err != nil {
		return ports.AgentEvent{}, false
	}
	for _, e := range rows {
		if e.Seq == f.Seq {
			return e, true
		}
	}
	return ports.AgentEvent{}, false
}

// watching is a snapshot of the subscriptions on a thread, or on every
// thread when threadID is empty.
func (b *Bus) watching(threadID string) []*subscription {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []*subscription
	for id, subs := range b.subs {
		if threadID != "" && id != threadID {
			continue
		}
		for _, s := range subs {
			out = append(out, s)
		}
	}
	return out
}

// start opens the one LISTEN connection, on first subscribe.
func (b *Bus) start() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.started {
		return
	}
	b.started = true
	if n, ok := b.listener.(ConnectNotifier); ok {
		n.OnConnect(b.connected)
	}
	go func() {
		_ = b.listener.Listen(context.Background(), b.channel, b.receive)
	}()
	// One heartbeat for every subscription in the process, not a ticker each.
	go func() {
		ticker := time.NewTicker(b.heartbeat)
		defer ticker.Stop()
		for range ticker.C {
			for _, s := range b.watching("") {
				s.offer(item{event: &ports.AgentEvent{ThreadID: s.threadID, Seq: 0, Type: "HEARTBEAT", Payload: json.RawMessage("null"), CreatedAt: time.Now()}})
			}
		}
	}()
	// Expired kv rows, parked frames among them, are cleared now and then.
	// A frame cannot be deleted once read: another process may still need it.
	go func() {
		ticker := time.NewTicker(sweepEvery)
		defer ticker.Stop()
		for range ticker.C {
			_, _ = b.kv.DeleteExpired(context.Background())
		}
	}()
}

// receive is one notification off the wire, handed to the thread's
// subscriptions. It never blocks on a handler: each subscription has its own
// queue and goroutine.
func (b *Bus) receive(payload string) {
	var f frame
	if json.Unmarshal([]byte(payload), &f) != nil {
		return // a malformed frame never kills the listener
	}
	threadID := f.ThreadID
	if f.Event != nil {
		threadID = f.Event.ThreadID
	}
	subs := b.watching(threadID)
	if len(subs) == 0 {
		return // nobody here is watching that thread
	}
	e, ok := b.resolve(context.Background(), f)
	for _, s := range subs {
		if ok {
			ev := e
			s.offer(item{event: &ev})
		} else {
			// The reference expired before the notification arrived. The
			// log still has a durable event; each subscriber reads it its
			// own way, on its own goroutine.
			fr := f
			s.offer(item{lookup: &fr})
		}
	}
}

// connected runs on every fresh LISTEN connection. Anything published while
// the previous one was down never reached this process, so each
// subscription is told to read what it missed from the log. The replay
// itself runs on the subscription's goroutine, in order with the events
// queued behind it, so the listener goes straight back to reading.
func (b *Bus) connected() {
	for _, s := range b.watching("") {
		if s.ctx.Err() == nil {
			s.offer(item{replay: true})
		}
	}
}

func (b *Bus) Subscribe(ctx context.Context, threadID string, handler func(ports.AgentEvent)) (func() error, error) {
	b.start()
	sub := &subscription{
		threadID: threadID, ctx: ctx, handler: handler,
		queue: make(chan item, subQueueCap), done: make(chan struct{}),
	}
	// Where the thread's log stands right now, for a replay that has nothing
	// delivered to resume from. Best effort: an unreadable counter only
	// means such a replay starts from the beginning.
	if raw, found, err := b.kv.Get(ctx, core.SeqKey(threadID)); err == nil && found {
		sub.anchor, _ = strconv.ParseInt(raw, 10, 64)
	}
	b.mu.Lock()
	b.nextID++
	id := b.nextID
	if b.subs[threadID] == nil {
		b.subs[threadID] = map[int]*subscription{}
	}
	b.subs[threadID][id] = sub
	b.mu.Unlock()
	go b.run(sub)

	var once sync.Once
	return func() error {
		once.Do(func() {
			close(sub.done)
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
