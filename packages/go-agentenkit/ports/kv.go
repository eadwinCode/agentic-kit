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
//
// The two compare-and-act calls are what make a lease safe to renew and
// release: a worker only ever extends or frees a lock that still carries
// its own value, never one another worker took after its lapsed.
type Kv interface {
	// Get returns the value and true, or "" and false when the key is missing.
	Get(ctx context.Context, key string) (string, bool, error)
	// Set returns true iff the value was written. With OnlyIfNotExists it
	// returns false when the key already existed.
	Set(ctx context.Context, key, value string, opts SetOptions) (bool, error)
	// SetIfValue writes value with the given expiry only while the key
	// currently holds expected. Returns false when it holds anything else,
	// or nothing. Zero expiry means no expiry.
	SetIfValue(ctx context.Context, key, expected, value string, expiry time.Duration) (bool, error)
	Del(ctx context.Context, key string) error
	// DelIfValue deletes the key only while it holds expected. Returns true
	// iff it did.
	DelIfValue(ctx context.Context, key, expected string) (bool, error)
	Incr(ctx context.Context, key string) (int64, error)
	// IncrWithExpiry is Incr that also gives a NEW counter the expiry, so a
	// counter nobody clears still ages out. An existing counter keeps the
	// expiry it has.
	IncrWithExpiry(ctx context.Context, key string, expiry time.Duration) (int64, error)
}
