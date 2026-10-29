package clevixflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"sync"
	"time"

	flow "github.com/eadwinCode/flow-sdk"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// nameRE is Flow's charset for channel and stream names.
var nameRE = regexp.MustCompile(`^[A-Za-z0-9:_.-]{1,256}$`)

// maxInlineBytes is under Flow's 64 KB cap on one message or one stream
// item, with room for the rest of the request. A value over it goes through
// the kv.
const maxInlineBytes = 60_000

// flowName is prefix+id when Flow allows it, else prefix+"h."+sha256(id).
func flowName(prefix, id string) string {
	name := prefix + id
	if nameRE.MatchString(name) && name != "." && name != ".." && name != "token" && name != "connect" {
		return name
	}
	sum := sha256.Sum256([]byte(id))
	return prefix + "h." + hex.EncodeToString(sum[:])
}

// ThreadChannel is the channel a thread's events go on.
func ThreadChannel(threadID string) string { return flowName("agentenkit.thread.", threadID) }

const busTopic = "events"

// busRefTTL is how long an event too big for one message waits in the kv.
const busRefTTL = time.Minute

// busFrame is what travels on the channel: the event, or the kv key that
// holds it.
type busFrame struct {
	Event *ports.AgentEvent `json:"event,omitempty"`
	Ref   string            `json:"ref,omitempty"`
}

// BusOptions tune the bus.
type BusOptions struct {
	// Kv holds events too big for one realtime message (over about 60 KB).
	// Required.
	Kv ports.Kv
	// Heartbeat is how often each subscriber gets a HEARTBEAT notice.
	// Default one minute.
	Heartbeat time.Duration
	// OpenTimeout is how long Subscribe waits for its socket to open before
	// it returns anyway. Default 10 seconds.
	OpenTimeout time.Duration
}

// Bus is an EventBus over Clevix Flow realtime.
//
// Each thread has its own channel, and a process opens one socket per thread
// it follows, shared by every subscriber in the process. Each subscription
// has its own queue and goroutine, so one slow handler holds up nobody else.
// Realtime keeps nothing and drops what comes while a socket reconnects: the
// bus is at-most-once anyway, and a follower fills any gap from the log. An
// event over about 60 KB waits in the kv for a minute, and the channel
// carries its key. While anyone subscribes, each subscriber gets a HEARTBEAT
// notice (seq 0) every Heartbeat, as with the other buses.
type Bus struct {
	client *flow.Client
	opts   BusOptions

	mu       sync.Mutex
	channels map[string]*busChannel
	nextID   int
	ticking  chan struct{} // closed to stop the heartbeat; nil when it is not running
}

type busChannel struct {
	subs     map[int]*busSub
	open     chan struct{}
	openOnce sync.Once
	stop     func()
}

func (c *busChannel) opened() { c.openOnce.Do(func() { close(c.open) }) }

// busSubQueue is how many events a subscription holds for a slow handler;
// past it, events are dropped (at-most-once) and the follower refills from
// the log.
const busSubQueue = 1024

type busSub struct {
	handler func(ports.AgentEvent)
	queue   chan ports.AgentEvent
	done    chan struct{}
}

func (s *busSub) offer(e ports.AgentEvent) {
	select {
	case s.queue <- e:
	default: // full: dropped; the log has every durable event
	}
}

func (s *busSub) run() {
	for {
		select {
		case <-s.done:
			return
		case e := <-s.queue:
			func() {
				defer func() { _ = recover() }() // a panicking handler must not kill the bus
				s.handler(e)
			}()
		}
	}
}

// NewBus makes a bus. opts.Kv is required.
func NewBus(client *flow.Client, opts BusOptions) (*Bus, error) {
	if opts.Kv == nil {
		return nil, errors.New("clevixflow: NewBus needs a Kv")
	}
	if opts.Heartbeat <= 0 {
		opts.Heartbeat = time.Minute
	}
	if opts.OpenTimeout <= 0 {
		opts.OpenTimeout = 10 * time.Second
	}
	return &Bus{client: client, opts: opts, channels: map[string]*busChannel{}}, nil
}

// Publish sends the event on the thread's channel.
func (b *Bus) Publish(ctx context.Context, threadID string, event ports.AgentEvent) error {
	raw, err := json.Marshal(event)
	if err != nil {
		return err
	}
	frame := busFrame{Event: &event}
	if len(raw) > maxInlineBytes {
		frame = busFrame{Ref: "agentenkit:flowbus:" + threadID + ":" + randomKey()}
		if _, err := b.opts.Kv.Set(ctx, frame.Ref, string(raw), ports.SetOptions{Expiry: busRefTTL}); err != nil {
			return err
		}
	}
	return b.client.Realtime().Publish(ctx, ThreadChannel(threadID), busTopic, frame)
}

// Subscribe adds a handler for the thread's events. It returns once the
// thread's socket is open, or after OpenTimeout: an event published before
// then may be missed, which a follower's gap read covers.
func (b *Bus) Subscribe(ctx context.Context, threadID string, handler func(ports.AgentEvent)) (func() error, error) {
	b.mu.Lock()
	ch := b.channels[threadID]
	if ch == nil {
		var err error
		if ch, err = b.connect(threadID); err != nil {
			b.mu.Unlock()
			return nil, err
		}
		b.channels[threadID] = ch
	}
	id := b.nextID
	b.nextID++
	sub := &busSub{handler: handler, queue: make(chan ports.AgentEvent, busSubQueue), done: make(chan struct{})}
	ch.subs[id] = sub
	b.startHeartbeat()
	b.mu.Unlock()
	go sub.run()

	timer := time.NewTimer(b.opts.OpenTimeout)
	defer timer.Stop()
	select {
	case <-ch.open:
	case <-timer.C:
	case <-ctx.Done():
	}

	var once sync.Once
	return func() error {
		once.Do(func() {
			close(sub.done)
			b.mu.Lock()
			delete(ch.subs, id)
			var stop func()
			if len(ch.subs) == 0 && b.channels[threadID] == ch {
				delete(b.channels, threadID)
				stop = ch.stop
			}
			if len(b.channels) == 0 {
				b.stopHeartbeat()
			}
			b.mu.Unlock()
			if stop != nil {
				stop()
			}
		})
		return nil
	}, nil
}

// connect opens the thread's socket. Called with b.mu held.
func (b *Bus) connect(threadID string) (*busChannel, error) {
	ch := &busChannel{subs: map[int]*busSub{}, open: make(chan struct{})}
	stop, err := b.client.Realtime().Subscribe(context.Background(), flow.SubscribeOpts{
		Channel: ThreadChannel(threadID),
		Topics:  []string{busTopic},
		OnState: func(state flow.RealtimeState, _ error) {
			if state == flow.RealtimeOpen {
				ch.opened()
			}
		},
		OnMessage: func(m flow.RealtimeMessage) { b.receive(ch, m.Data) },
	})
	if err != nil {
		return nil, err
	}
	ch.stop = func() {
		ch.opened()
		stop()
	}
	return ch, nil
}

func (b *Bus) receive(ch *busChannel, data json.RawMessage) {
	var frame busFrame
	if json.Unmarshal(data, &frame) != nil {
		return // a malformed frame never kills the subscription
	}
	event := frame.Event
	if event == nil {
		if frame.Ref == "" {
			return
		}
		raw, found, err := b.opts.Kv.Get(context.Background(), frame.Ref)
		if err != nil || !found {
			return // expired: the follower reads it from the log
		}
		event = &ports.AgentEvent{}
		if json.Unmarshal([]byte(raw), event) != nil {
			return
		}
	}
	b.mu.Lock()
	subs := make([]*busSub, 0, len(ch.subs))
	for _, s := range ch.subs {
		subs = append(subs, s)
	}
	b.mu.Unlock()
	for _, s := range subs {
		s.offer(*event)
	}
}

// startHeartbeat starts the one heartbeat ticker. Called with b.mu held.
func (b *Bus) startHeartbeat() {
	if b.ticking != nil {
		return
	}
	stop := make(chan struct{})
	b.ticking = stop
	go func() {
		ticker := time.NewTicker(b.opts.Heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				b.mu.Lock()
				for threadID, ch := range b.channels {
					for _, s := range ch.subs {
						s.offer(ports.AgentEvent{ThreadID: threadID, Seq: 0, Type: "HEARTBEAT", CreatedAt: time.Now()})
					}
				}
				b.mu.Unlock()
			}
		}
	}()
}

// stopHeartbeat stops the ticker. Called with b.mu held.
func (b *Bus) stopHeartbeat() {
	if b.ticking != nil {
		close(b.ticking)
		b.ticking = nil
	}
}
