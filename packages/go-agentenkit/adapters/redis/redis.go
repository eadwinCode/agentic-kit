// Package redis holds the reference Kv and EventBus adapters over Redis
// (github.com/redis/go-redis/v9). Works against any Redis: local Docker,
// self-hosted, or managed.
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// ThreadChannel is the pub/sub channel for a thread's events. Shared with
// the Upstash adapter and the TypeScript package.
func ThreadChannel(threadID string) string { return "thread:" + threadID + ":events" }

// Kv is a Kv over Redis.
type Kv struct{ client goredis.UniversalClient }

// NewKv wraps a client.
func NewKv(client goredis.UniversalClient) *Kv { return &Kv{client: client} }

func (k *Kv) Get(ctx context.Context, key string) (string, bool, error) {
	v, err := k.client.Get(ctx, key).Result()
	if errors.Is(err, goredis.Nil) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return v, true, nil
}

func (k *Kv) Set(ctx context.Context, key, value string, opts ports.SetOptions) (bool, error) {
	if opts.OnlyIfNotExists {
		// SET NX: true when written, false when the key already existed (§3.4)
		return k.client.SetNX(ctx, key, value, opts.Expiry).Result()
	}
	if err := k.client.Set(ctx, key, value, opts.Expiry).Err(); err != nil {
		return false, err
	}
	return true, nil
}

func (k *Kv) Del(ctx context.Context, key string) error { return k.client.Del(ctx, key).Err() }

func (k *Kv) Incr(ctx context.Context, key string) (int64, error) {
	return k.client.Incr(ctx, key).Result()
}

// The compare-and-act calls run as scripts so the read and the write are
// one atomic step on the server (§3.4).
var (
	setIfValueScript = goredis.NewScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if tonumber(ARGV[3]) > 0 then redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3]) else redis.call('SET', KEYS[1], ARGV[2]) end
return 1`)
	delIfValueScript = goredis.NewScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('DEL', KEYS[1])`)
	incrWithExpiryScript = goredis.NewScript(`
local n = redis.call('INCR', KEYS[1])
if n == 1 and tonumber(ARGV[1]) > 0 then redis.call('PEXPIRE', KEYS[1], ARGV[1]) end
return n`)
)

func (k *Kv) SetIfValue(ctx context.Context, key, expected, value string, ttl time.Duration) (bool, error) {
	n, err := setIfValueScript.Run(ctx, k.client, []string{key}, expected, value, ttl.Milliseconds()).Int64()
	return n == 1, err
}

func (k *Kv) DelIfValue(ctx context.Context, key, expected string) (bool, error) {
	n, err := delIfValueScript.Run(ctx, k.client, []string{key}, expected).Int64()
	return n == 1, err
}

func (k *Kv) IncrWithExpiry(ctx context.Context, key string, ttl time.Duration) (int64, error) {
	return incrWithExpiryScript.Run(ctx, k.client, []string{key}, ttl.Milliseconds()).Int64()
}

// Bus is an EventBus over Redis Pub/Sub.
//
// One subscriber connection per process, shared by every subscription: a
// channel is subscribed when its first handler arrives and unsubscribed
// when its last one leaves. A connection per viewer would run a busy
// deployment into Redis's maxclients. Each subscription has its own queue
// and goroutine, so one slow handler holds up nobody else, and go-redis's
// own buffer never fills and drops.
//
// While subscribed, a bus-only HEARTBEAT notice (seq 0, never persisted)
// reaches every subscription each heartbeat interval, from one ticker per
// process: the §2.5 watchdog pattern. Pub/sub is at-most-once, so a
// distributor treats heartbeats as a trigger to re-check for orphaned HITL
// waits, and a follower fills any gap from the log.
type Bus struct {
	client    goredis.UniversalClient
	heartbeat time.Duration

	mu     sync.Mutex
	pubsub *goredis.PubSub
	subs   map[string]map[int]*busSub
	nextID int
}

// busSubQueue is how many events a subscription holds for a slow handler;
// past it, events are dropped (at-most-once) and the follower refills from
// the log.
const busSubQueue = 1024

type busSub struct {
	threadID string
	handler  func(ports.AgentEvent)
	queue    chan ports.AgentEvent
	done     chan struct{}
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

// NewBus wraps a client. A zero heartbeat means one minute.
func NewBus(client goredis.UniversalClient, heartbeat time.Duration) *Bus {
	if heartbeat <= 0 {
		heartbeat = time.Minute
	}
	return &Bus{client: client, heartbeat: heartbeat, subs: map[string]map[int]*busSub{}}
}

func (b *Bus) Publish(ctx context.Context, threadID string, event ports.AgentEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return b.client.Publish(ctx, ThreadChannel(threadID), body).Err()
}

// start opens the shared subscriber connection and its two goroutines, on
// the first subscription. Called with b.mu held.
func (b *Bus) start() {
	if b.pubsub != nil {
		return
	}
	b.pubsub = b.client.Subscribe(context.Background())
	messages := b.pubsub.Channel()
	go func() {
		for msg := range messages {
			var e ports.AgentEvent
			if err := json.Unmarshal([]byte(msg.Payload), &e); err != nil {
				continue // malformed frame: never kill the subscription
			}
			for _, s := range b.watching(msg.Channel) {
				s.offer(e)
			}
		}
	}()
	go func() {
		ticker := time.NewTicker(b.heartbeat)
		defer ticker.Stop()
		for range ticker.C {
			for _, s := range b.watching("") {
				s.offer(ports.AgentEvent{ThreadID: s.threadID, Seq: 0, Type: "HEARTBEAT", Payload: json.RawMessage("null"), CreatedAt: time.Now()})
			}
		}
	}()
}

// watching is a snapshot of the subscriptions on a channel, or on every
// channel when channel is empty.
func (b *Bus) watching(channel string) []*busSub {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []*busSub
	for ch, subs := range b.subs {
		if channel != "" && ch != channel {
			continue
		}
		for _, s := range subs {
			out = append(out, s)
		}
	}
	return out
}

func (b *Bus) Subscribe(ctx context.Context, threadID string, handler func(ports.AgentEvent)) (func() error, error) {
	channel := ThreadChannel(threadID)
	b.mu.Lock()
	b.start()
	if len(b.subs[channel]) == 0 {
		if err := b.pubsub.Subscribe(ctx, channel); err != nil {
			b.mu.Unlock()
			return nil, err
		}
	}
	b.nextID++
	id := b.nextID
	if b.subs[channel] == nil {
		b.subs[channel] = map[int]*busSub{}
	}
	sub := &busSub{threadID: threadID, handler: handler, queue: make(chan ports.AgentEvent, busSubQueue), done: make(chan struct{})}
	b.subs[channel][id] = sub
	b.mu.Unlock()
	go sub.run()

	var once sync.Once
	return func() error {
		var err error
		once.Do(func() {
			close(sub.done)
			b.mu.Lock()
			defer b.mu.Unlock()
			delete(b.subs[channel], id)
			if len(b.subs[channel]) == 0 {
				delete(b.subs, channel)
				err = b.pubsub.Unsubscribe(context.Background(), channel)
			}
		})
		return err
	}, nil
}
