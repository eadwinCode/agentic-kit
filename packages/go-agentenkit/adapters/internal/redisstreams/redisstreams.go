// Package redisstreams is RunStreams over Redis Streams, shared by the Redis
// and Upstash adapters. Each stream is two keys in one hash slot: a hash
// with who it belongs to, whether it is closed and its end event, and the
// Redis Stream itself, one entry per event. The offset is the entry id.
// Every write is one Lua script, so a close can never slip between an
// append's check and its XADD. The TS adapters run the same scripts on the
// same keys (src/adapters/stream-scripts.ts).
package redisstreams

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"strconv"
	"strings"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// MetaKey is a stream's hash.
func MetaKey(streamID string) string { return "agent:run:{" + streamID + "}:meta" }

// Key is the Redis Stream itself.
func Key(streamID string) string { return "agent:run:{" + streamID + "}" }

// OpenScript: KEYS meta, stream. ARGV threadId, runId, ttlMs. 1 opened, 0
// already open.
const OpenScript = `if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('HSET', KEYS[1], 'threadId', ARGV[1], 'runId', ARGV[2], 'closed', '0')
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 1`

// AppendScript: KEYS meta, stream. ARGV one event JSON each. The entry ids,
// or -1 gone, -2 closed. The stream key takes the meta key's expiry.
const AppendScript = `local closed = redis.call('HGET', KEYS[1], 'closed')
if not closed then return -1 end
if closed == '1' then return -2 end
local ids = {}
for i = 1, #ARGV do ids[i] = redis.call('XADD', KEYS[2], '*', 'e', ARGV[i]) end
local ttl = redis.call('PTTL', KEYS[1])
if ttl > 0 then redis.call('PEXPIRE', KEYS[2], ttl) end
return ids`

// CloseScript: KEYS meta, stream. ARGV end JSON, graceMs. 1 closed, 0
// already closed, -1 gone.
const CloseScript = `local closed = redis.call('HGET', KEYS[1], 'closed')
if not closed then return -1 end
if closed == '1' then return 0 end
redis.call('XADD', KEYS[2], '*', 'e', ARGV[1])
redis.call('HSET', KEYS[1], 'closed', '1', 'end', ARGV[1])
redis.call('PEXPIRE', KEYS[1], ARGV[2])
redis.call('PEXPIRE', KEYS[2], ARGV[2])
return 1`

// ReadScript: KEYS meta, stream. ARGV the XRANGE start ('-', or '(' + an
// entry id). -1 gone, else { threadId, runId, closed, the end JSON or an
// empty string, entries }.
const ReadScript = `local meta = redis.call('HMGET', KEYS[1], 'threadId', 'runId', 'closed', 'end')
if not meta[1] then return -1 end
return { meta[1], meta[2], meta[3], meta[4] or '', redis.call('XRANGE', KEYS[2], ARGV[1], '+') }`

// DeleteScript: KEYS meta, stream.
const DeleteScript = `redis.call('DEL', KEYS[1], KEYS[2]) return 1`

// RunScript runs a script: the one thing the Redis and Upstash clients
// differ in. The reply uses Go's plain types: string, int64 (or a JSON
// number), []any.
type RunScript func(ctx context.Context, script string, keys []string, args []any) (any, error)

// WaitForNews waits until the stream key may have an entry past `after`,
// or max passes, or ctx is done. Returning early without news is harmless:
// the reader reads again and finds nothing.
type WaitForNews func(ctx context.Context, key, after string, max time.Duration)

// CompareIDs compares two entry ids ("<ms>-<seq>").
func CompareIDs(a, b string) int {
	am, as := splitID(a)
	bm, bs := splitID(b)
	switch {
	case am != bm:
		if am < bm {
			return -1
		}
		return 1
	case as != bs:
		if as < bs {
			return -1
		}
		return 1
	}
	return 0
}

func splitID(id string) (uint64, uint64) {
	ms, seq, _ := strings.Cut(id, "-")
	m, _ := strconv.ParseUint(ms, 10, 64)
	s, _ := strconv.ParseUint(seq, 10, 64)
	return m, s
}

// Streams implements ports.RunStreams over the scripts.
type Streams struct {
	Run  RunScript
	Wait WaitForNews
	// Poll is the longest a reader waits before reading again, news or
	// not: the floor under a missed wake-up.
	Poll time.Duration
}

var _ ports.RunStreams = (*Streams)(nil)

func keys(streamID string) []string { return []string{MetaKey(streamID), Key(streamID)} }

func asInt(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case float64:
		return int64(n), true
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	}
	return 0, false
}

func asString(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case []byte:
		return string(s)
	case nil:
		return ""
	}
	return fmt.Sprint(v)
}

func (s *Streams) Open(ctx context.Context, streamID string, meta ports.StreamMeta, ttl time.Duration) error {
	_, err := s.Run(ctx, OpenScript, keys(streamID), []any{meta.ThreadID, meta.RunID, max(int64(1), ttl.Milliseconds())})
	return err
}

func (s *Streams) Append(ctx context.Context, streamID string, events []ports.StreamEvent) ([]string, error) {
	if len(events) == 0 {
		return nil, nil
	}
	args := make([]any, len(events))
	for i, e := range events {
		b, err := ports.EncodeStreamEvent(e)
		if err != nil {
			return nil, err
		}
		args[i] = string(b)
	}
	res, err := s.Run(ctx, AppendScript, keys(streamID), args)
	if err != nil {
		return nil, err
	}
	if n, ok := asInt(res); ok {
		if n == -2 {
			return nil, ports.ErrStreamClosed
		}
		return nil, ports.ErrStreamGone
	}
	ids, _ := res.([]any)
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = asString(id)
	}
	return out, nil
}

type page struct {
	meta   ports.StreamMeta
	closed bool
	end    ports.StreamEnd
	items  []ports.StreamItem
}

func (s *Streams) page(ctx context.Context, streamID, after string) (*page, error) {
	start := "-"
	if after != "" {
		start = "(" + after
	}
	res, err := s.Run(ctx, ReadScript, keys(streamID), []any{start})
	if err != nil {
		return nil, err
	}
	parts, ok := res.([]any)
	if !ok || len(parts) < 5 {
		return nil, nil // -1: gone
	}
	p := &page{
		meta:   ports.StreamMeta{ThreadID: asString(parts[0]), RunID: asString(parts[1])},
		closed: asString(parts[2]) == "1",
	}
	if p.closed {
		if raw := asString(parts[3]); raw != "" {
			e, err := ports.DecodeStreamEvent([]byte(raw))
			if err != nil {
				return nil, err
			}
			p.end, _ = e.(ports.StreamEnd)
		}
	}
	entries, _ := parts[4].([]any)
	for _, entry := range entries {
		pair, _ := entry.([]any)
		if len(pair) < 2 {
			continue
		}
		fields, _ := pair[1].([]any)
		for i := 0; i+1 < len(fields); i += 2 {
			if asString(fields[i]) != "e" {
				continue
			}
			e, err := ports.DecodeStreamEvent([]byte(asString(fields[i+1])))
			if err != nil {
				return nil, err
			}
			p.items = append(p.items, ports.StreamItem{Offset: asString(pair[0]), Event: e})
		}
	}
	return p, nil
}

func (s *Streams) Read(ctx context.Context, streamID, after string) iter.Seq2[ports.StreamItem, error] {
	return func(yield func(ports.StreamItem, error) bool) {
		cursor := after
		for {
			if ctx.Err() != nil {
				return
			}
			p, err := s.page(ctx, streamID, cursor)
			if err != nil {
				if ctx.Err() == nil {
					yield(ports.StreamItem{}, err)
				}
				return
			}
			if p == nil {
				yield(ports.StreamItem{}, ports.ErrStreamGone)
				return
			}
			for _, item := range p.items {
				if !yield(item, nil) {
					return
				}
				cursor = item.Offset
				if ports.IsStreamEnd(item.Event) {
					return
				}
			}
			// Read from past the end item: nothing more will ever come.
			if p.closed && len(p.items) == 0 {
				return
			}
			if len(p.items) == 0 {
				from := cursor
				if from == "" {
					from = "0-0"
				}
				s.Wait(ctx, Key(streamID), from, s.Poll)
			}
		}
	}
}

func (s *Streams) Snapshot(ctx context.Context, streamID, after string) (*ports.StreamSnapshot, error) {
	p, err := s.page(ctx, streamID, after)
	if err != nil || p == nil {
		return nil, err
	}
	return &ports.StreamSnapshot{Meta: p.meta, Items: p.items, End: p.end}, nil
}

func (s *Streams) Close(ctx context.Context, streamID string, end ports.StreamEnd, grace time.Duration) error {
	b, err := ports.EncodeStreamEvent(end)
	if err != nil {
		return err
	}
	res, err := s.Run(ctx, CloseScript, keys(streamID), []any{string(b), max(int64(1), grace.Milliseconds())})
	if err != nil {
		return err
	}
	if n, _ := asInt(res); n == -1 {
		return ports.ErrStreamGone
	}
	return nil
}

func (s *Streams) Delete(ctx context.Context, streamID string) error {
	_, err := s.Run(ctx, DeleteScript, keys(streamID), nil)
	return err
}

// Sleep waits d, or until ctx is done: the wait of a store with no blocking
// read.
func Sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
