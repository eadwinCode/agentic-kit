package redis

import (
	"context"
	"errors"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/internal/redisstreams"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// RunStreams is RunStreams over Redis Streams (see package redisstreams for
// the keys and scripts, the same ones the TS adapter runs). Readers in one
// process share one blocking XREAD; Poll is the floor under a missed
// wake-up.
//
// The shared XREAD names many stream keys at once, so on Redis Cluster,
// where they live in different slots, it fails and readers poll instead.
type RunStreams struct {
	*redisstreams.Streams
	tail *streamTail
}

var _ ports.RunStreams = (*RunStreams)(nil)

// StreamsOptions tunes NewRunStreams. Zero values take the defaults.
type StreamsOptions struct {
	// Poll is the longest a reader waits without news. Default 1s.
	Poll time.Duration
	// Block is how long the shared XREAD blocks per pass. Default 5s.
	Block time.Duration
}

// NewRunStreams wraps a client.
func NewRunStreams(client goredis.UniversalClient, opts StreamsOptions) *RunStreams {
	if opts.Poll <= 0 {
		opts.Poll = time.Second
	}
	if opts.Block <= 0 {
		opts.Block = 5 * time.Second
	}
	tail := &streamTail{client: client, block: opts.Block, keys: map[string]*tailKey{}}
	return &RunStreams{
		Streams: &redisstreams.Streams{
			Run: func(ctx context.Context, script string, keys []string, args []any) (any, error) {
				return client.Eval(ctx, script, keys, args...).Result()
			},
			Wait: tail.wait,
			Poll: opts.Poll,
		},
		tail: tail,
	}
}

// Shutdown closes the shared tail connection.
func (r *RunStreams) Shutdown() error { return r.tail.close() }

type tailKey struct {
	// seen is the newest entry id the tail has seen on the key.
	seen    string
	waiters map[chan struct{}]struct{}
	// used is when a reader last waited on the key. A key stays watched a
	// while after its last reader leaves, so a reader that waits again
	// after each event does not break the block every time.
	used time.Time
}

// tailIdle is how long a key with no reader stays watched.
const tailIdle = 10 * time.Second

// streamTail is one blocking XREAD per process, shared by every reader of
// every stream here: a connection per reader would run a busy deployment
// into Redis's maxclients. A reader registers the stream key and the last
// id it has; the tail wakes it when an entry past that id lands. When a key
// it does not watch yet arrives, the tail breaks its own block with CLIENT
// UNBLOCK and starts again with the new key.
type streamTail struct {
	client goredis.UniversalClient
	block  time.Duration

	mu      sync.Mutex
	keys    map[string]*tailKey
	running bool
	// conn is the tail's own connection, and connID its CLIENT ID, when
	// the client can hand one out (a single-node or failover client).
	conn   *goredis.Conn
	connID int64
}

func (t *streamTail) wait(ctx context.Context, key, after string, max time.Duration) {
	ch := make(chan struct{})
	t.mu.Lock()
	k := t.keys[key]
	// The tail already saw something past this reader: wake at once.
	if k != nil && redisstreams.CompareIDs(k.seen, after) > 0 {
		t.mu.Unlock()
		return
	}
	fresh := k == nil
	if fresh {
		k = &tailKey{seen: after, waiters: map[chan struct{}]struct{}{}}
		t.keys[key] = k
	}
	k.waiters[ch] = struct{}{}
	k.used = time.Now()
	switch {
	case !t.running:
		t.running = true
		go t.loop()
	case fresh && t.connID != 0:
		id := t.connID
		go func() { _ = t.client.Do(context.Background(), "CLIENT", "UNBLOCK", id).Err() }()
	}
	t.mu.Unlock()

	timer := time.NewTimer(max)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-ch:
	case <-timer.C:
	}
	t.mu.Lock()
	if k := t.keys[key]; k != nil {
		delete(k.waiters, ch)
		k.used = time.Now()
	}
	t.mu.Unlock()
}

// reader is the connection the XREAD runs on.
func (t *streamTail) reader(ctx context.Context) goredis.Cmdable {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.conn != nil {
		return t.conn
	}
	c, ok := t.client.(*goredis.Client)
	if !ok {
		return t.client
	}
	conn := c.Conn()
	id, err := conn.ClientID(ctx).Result()
	if err != nil {
		_ = conn.Close()
		return t.client
	}
	t.conn, t.connID = conn, id
	return conn
}

func (t *streamTail) loop() {
	ctx := context.Background()
	for {
		t.mu.Lock()
		for key, k := range t.keys {
			if len(k.waiters) == 0 && time.Since(k.used) > tailIdle {
				delete(t.keys, key)
			}
		}
		if len(t.keys) == 0 {
			t.running = false
			t.mu.Unlock()
			return
		}
		streams := make([]string, 0, 2*len(t.keys))
		ids := make([]string, 0, len(t.keys))
		for key, k := range t.keys {
			streams = append(streams, key)
			ids = append(ids, k.seen)
		}
		t.mu.Unlock()

		res, err := t.reader(ctx).XRead(ctx, &goredis.XReadArgs{Streams: append(streams, ids...), Block: t.block}).Result()
		if errors.Is(err, goredis.Nil) {
			continue // timed out, or unblocked for a new key
		}
		if err != nil {
			// A dropped connection: readers fall back on their own poll
			// until the next pass reconnects.
			t.mu.Lock()
			if t.conn != nil {
				_ = t.conn.Close()
				t.conn, t.connID = nil, 0
			}
			t.mu.Unlock()
			time.Sleep(200 * time.Millisecond)
			continue
		}
		t.mu.Lock()
		for _, s := range res {
			k := t.keys[s.Stream]
			if k == nil || len(s.Messages) == 0 {
				continue
			}
			k.seen = s.Messages[len(s.Messages)-1].ID
			for ch := range k.waiters {
				close(ch)
			}
			clear(k.waiters)
		}
		t.mu.Unlock()
	}
}

func (t *streamTail) close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.conn == nil {
		return nil
	}
	err := t.conn.Close()
	t.conn, t.connID = nil, 0
	return err
}
