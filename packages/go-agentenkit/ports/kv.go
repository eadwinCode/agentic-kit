package ports

import (
	"context"
	"time"
)

// SetOptions tunes Kv.Set.
type SetOptions struct {
	// Expiry is the key's TTL; zero means no expiry.
	Expiry time.Duration
	// OnlyIfNotExists is SET NX: the atomic primitive behind the per-thread
	// run lock (§3.4).
	OnlyIfNotExists bool
}

// Kv is the hot key-value port: thread state cache, HITL handoff keys, seq &
// attempt counters, per-thread run locks (§3.2). Backed by Redis in the
// reference adapters; any KV works.
type Kv interface {
	// Get returns the value and true, or "" and false when the key is missing.
	Get(ctx context.Context, key string) (string, bool, error)
	// Set returns true iff the value was written. With OnlyIfNotExists it
	// returns false when the key already existed.
	Set(ctx context.Context, key, value string, opts SetOptions) (bool, error)
	Del(ctx context.Context, key string) error
	Incr(ctx context.Context, key string) (int64, error)
}
