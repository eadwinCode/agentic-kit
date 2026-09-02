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

// Bus is an EventBus over Redis Pub/Sub.
//
// While subscribed, it emits a bus-only HEARTBEAT notice (seq 0, never
// persisted) every heartbeat interval: the §2.5 watchdog pattern. Pub/sub is
// at-most-once, so a distributor treats heartbeats as a trigger to re-check
// for orphaned HITL waits.
type Bus struct {
	client    goredis.UniversalClient
	heartbeat time.Duration
}

// NewBus wraps a client. A zero heartbeat means one minute.
func NewBus(client goredis.UniversalClient, heartbeat time.Duration) *Bus {
	if heartbeat <= 0 {
		heartbeat = time.Minute
	}
	return &Bus{client: client, heartbeat: heartbeat}
}

func (b *Bus) Publish(ctx context.Context, threadID string, event ports.AgentEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return b.client.Publish(ctx, ThreadChannel(threadID), body).Err()
}

func (b *Bus) Subscribe(ctx context.Context, threadID string, handler func(ports.AgentEvent)) (func() error, error) {
	sub := b.client.Subscribe(ctx, ThreadChannel(threadID))
	if _, err := sub.Receive(ctx); err != nil {
		_ = sub.Close()
		return nil, err
	}
	done := make(chan struct{})
	var once sync.Once
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		ch := sub.Channel()
		for {
			select {
			case <-done:
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				var e ports.AgentEvent
				if err := json.Unmarshal([]byte(msg.Payload), &e); err != nil {
					continue // malformed frame: never kill the subscription
				}
				handler(e)
			}
		}
	}()
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(b.heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				handler(ports.AgentEvent{ThreadID: threadID, Seq: 0, Type: "HEARTBEAT", Payload: json.RawMessage("null"), CreatedAt: time.Now()})
			}
		}
	}()
	return func() error {
		var err error
		once.Do(func() {
			close(done)
			err = sub.Close()
			wg.Wait()
		})
		return err
	}, nil
}
