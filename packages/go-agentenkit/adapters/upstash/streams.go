package upstash

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/internal/redisstreams"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// RunStreams is RunStreams over Upstash Redis Streams: the same keys and
// scripts as the Redis adapter (see package redisstreams). Upstash's REST
// API has no blocking read, so a live reader polls every Poll.
type RunStreams struct{ *redisstreams.Streams }

var _ ports.RunStreams = (*RunStreams)(nil)

// NewRunStreams wraps a client. A zero poll means 250ms.
func NewRunStreams(redis *Redis, poll time.Duration) *RunStreams {
	if poll <= 0 {
		poll = 250 * time.Millisecond
	}
	return &RunStreams{&redisstreams.Streams{
		Run: func(ctx context.Context, script string, keys []string, args []any) (any, error) {
			cmd := append([]any{"EVAL", script, len(keys)}, anyStrings(keys)...)
			raw, err := redis.Do(ctx, append(cmd, args...)...)
			if err != nil {
				return nil, err
			}
			var out any
			dec := json.NewDecoder(bytes.NewReader(raw))
			dec.UseNumber()
			if err := dec.Decode(&out); err != nil {
				return nil, err
			}
			return out, nil
		},
		Wait: func(ctx context.Context, _, _ string, max time.Duration) { redisstreams.Sleep(ctx, max) },
		Poll: poll,
	}}
}

func anyStrings(s []string) []any {
	out := make([]any, len(s))
	for i, v := range s {
		out[i] = v
	}
	return out
}
