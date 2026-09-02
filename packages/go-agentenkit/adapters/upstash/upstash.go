// Package upstash holds the reference Kv and EventBus adapters over the
// Upstash Redis REST API.
//
// Upstash Pub/Sub over REST has no subscribe: live tailing needs a
// WebSocket-based subscriber, which differs per SDK. Wire one in through
// the Subscriber interface; replay-only usage works without it.
package upstash

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// ThreadChannel is the pub/sub channel for a thread's events.
func ThreadChannel(threadID string) string { return "thread:" + threadID + ":events" }

// Redis is the Upstash Redis REST client the adapters use.
type Redis struct {
	// URL is UPSTASH_REDIS_REST_URL.
	URL string
	// Token is UPSTASH_REDIS_REST_TOKEN.
	Token string
	// HTTP defaults to http.DefaultClient.
	HTTP *http.Client
}

// Do runs one command and returns its result.
func (r *Redis) Do(ctx context.Context, cmd ...any) (json.RawMessage, error) {
	body, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+r.Token)
	req.Header.Set("Content-Type", "application/json")
	client := r.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	var out struct {
		Result json.RawMessage `json:"result"`
		Error  string          `json:"error"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("upstash: %s: %s", res.Status, string(raw))
	}
	if out.Error != "" {
		return nil, errors.New("upstash: " + out.Error)
	}
	return out.Result, nil
}

// Kv is a Kv over Upstash Redis REST.
type Kv struct{ redis *Redis }

// NewKv wraps a client.
func NewKv(redis *Redis) *Kv { return &Kv{redis: redis} }

func (k *Kv) Get(ctx context.Context, key string) (string, bool, error) {
	res, err := k.redis.Do(ctx, "GET", key)
	if err != nil {
		return "", false, err
	}
	if bytes.Equal(res, []byte("null")) {
		return "", false, nil
	}
	var v string
	if err := json.Unmarshal(res, &v); err != nil {
		return "", false, err
	}
	return v, true, nil
}

func (k *Kv) Set(ctx context.Context, key, value string, opts ports.SetOptions) (bool, error) {
	cmd := []any{"SET", key, value}
	if opts.Expiry > 0 {
		cmd = append(cmd, "EX", int64(opts.Expiry.Seconds()))
	}
	if opts.OnlyIfNotExists {
		cmd = append(cmd, "NX")
	}
	res, err := k.redis.Do(ctx, cmd...)
	if err != nil {
		return false, err
	}
	// SET NX returns null when the key exists
	if opts.OnlyIfNotExists {
		return !bytes.Equal(res, []byte("null")), nil
	}
	return true, nil
}

func (k *Kv) Del(ctx context.Context, key string) error {
	_, err := k.redis.Do(ctx, "DEL", key)
	return err
}

func (k *Kv) Incr(ctx context.Context, key string) (int64, error) {
	res, err := k.redis.Do(ctx, "INCR", key)
	if err != nil {
		return 0, err
	}
	var n int64
	return n, json.Unmarshal(res, &n)
}

// Subscriber tails a thread channel. Return an unsubscribe function.
type Subscriber interface {
	Subscribe(ctx context.Context, threadID string, handler func(raw string)) (func() error, error)
}

// Bus is an EventBus over Upstash Redis REST publish plus an optional
// subscriber.
type Bus struct {
	redis      *Redis
	subscriber Subscriber
}

// NewBus wraps a client. subscriber may be nil for replay-only usage.
func NewBus(redis *Redis, subscriber Subscriber) *Bus {
	return &Bus{redis: redis, subscriber: subscriber}
}

func (b *Bus) Publish(ctx context.Context, threadID string, event ports.AgentEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = b.redis.Do(ctx, "PUBLISH", ThreadChannel(threadID), string(body))
	return err
}

func (b *Bus) Subscribe(ctx context.Context, threadID string, handler func(ports.AgentEvent)) (func() error, error) {
	if b.subscriber == nil {
		return nil, errors.New("upstash: Bus requires a Subscriber (WebSocket-based) for live tailing; replay-only usage works without one")
	}
	return b.subscriber.Subscribe(ctx, threadID, func(raw string) {
		var e ports.AgentEvent
		if err := json.Unmarshal([]byte(raw), &e); err != nil {
			return // malformed frame: never kill the subscription
		}
		handler(e)
	})
}
