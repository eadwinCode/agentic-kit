// Package memory holds the in-memory adapters: a full implementation used by
// the test suite, and a template for custom adapters.
package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

var idCounter uint64
var idMu sync.Mutex

func newID() string {
	idMu.Lock()
	defer idMu.Unlock()
	idCounter++
	return fmt.Sprintf("m%06d", idCounter)
}

type memEntry struct {
	value     string
	expiresAt time.Time
}

// Kv is an in-memory Kv for tests and local prototyping. Expiry is enforced
// lazily on read; Incr holds the lock, so concurrent callers can never
// collide on the same counter (§3.4).
type Kv struct {
	mu sync.Mutex
	m  map[string]memEntry
}

// NewKv makes an empty Kv.
func NewKv() *Kv { return &Kv{m: map[string]memEntry{}} }

func (k *Kv) live(key string) (memEntry, bool) {
	e, ok := k.m[key]
	if !ok {
		return e, false
	}
	if !e.expiresAt.IsZero() && e.expiresAt.Before(time.Now()) {
		delete(k.m, key)
		return e, false
	}
	return e, true
}

func (k *Kv) Get(_ context.Context, key string) (string, bool, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	e, ok := k.live(key)
	if !ok {
		return "", false, nil
	}
	return e.value, true, nil
}

func (k *Kv) Set(_ context.Context, key, value string, opts ports.SetOptions) (bool, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if opts.OnlyIfNotExists {
		if _, ok := k.live(key); ok {
			return false, nil
		}
	}
	e := memEntry{value: value}
	if opts.Expiry > 0 {
		e.expiresAt = time.Now().Add(opts.Expiry)
	}
	k.m[key] = e
	return true, nil
}

func (k *Kv) Del(_ context.Context, key string) error {
	k.mu.Lock()
	delete(k.m, key)
	k.mu.Unlock()
	return nil
}

func (k *Kv) Incr(_ context.Context, key string) (int64, error) {
	return k.IncrWithExpiry(context.Background(), key, 0)
}

func (k *Kv) IncrWithExpiry(_ context.Context, key string, ttl time.Duration) (int64, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	e, ok := k.live(key)
	n, _ := strconv.ParseInt(e.value, 10, 64)
	n++
	expires := e.expiresAt
	if !ok && ttl > 0 {
		expires = time.Now().Add(ttl)
	}
	k.m[key] = memEntry{value: strconv.FormatInt(n, 10), expiresAt: expires}
	return n, nil
}

func (k *Kv) SetIfValue(_ context.Context, key, expected, value string, ttl time.Duration) (bool, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	e, ok := k.live(key)
	if !ok || e.value != expected {
		return false, nil
	}
	next := memEntry{value: value}
	if ttl > 0 {
		next.expiresAt = time.Now().Add(ttl)
	}
	k.m[key] = next
	return true, nil
}

func (k *Kv) DelIfValue(_ context.Context, key, expected string) (bool, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	e, ok := k.live(key)
	if !ok || e.value != expected {
		return false, nil
	}
	delete(k.m, key)
	return true, nil
}

// Bus is a synchronous in-memory bus. Publishes are delivered to subscribers
// in order, on the publisher's goroutine.
type Bus struct {
	mu        sync.Mutex
	subs      map[string]map[int]func(ports.AgentEvent)
	nextSub   int
	published []ports.AgentEvent
}

// NewBus makes an empty Bus.
func NewBus() *Bus { return &Bus{subs: map[string]map[int]func(ports.AgentEvent){}} }

func (b *Bus) Publish(_ context.Context, threadID string, event ports.AgentEvent) error {
	b.mu.Lock()
	b.published = append(b.published, event)
	handlers := make([]func(ports.AgentEvent), 0, len(b.subs[threadID]))
	for _, h := range b.subs[threadID] {
		handlers = append(handlers, h)
	}
	b.mu.Unlock()
	for _, h := range handlers {
		h(event)
	}
	return nil
}

func (b *Bus) Subscribe(_ context.Context, threadID string, handler func(ports.AgentEvent)) (func() error, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.subs[threadID] == nil {
		b.subs[threadID] = map[int]func(ports.AgentEvent){}
	}
	id := b.nextSub
	b.nextSub++
	b.subs[threadID][id] = handler
	return func() error {
		b.mu.Lock()
		delete(b.subs[threadID], id)
		b.mu.Unlock()
		return nil
	}, nil
}

// Published is every event ever published, in order.
func (b *Bus) Published() []ports.AgentEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]ports.AgentEvent(nil), b.published...)
}

// Subscribers counts live subscriptions on a thread, so a test can prove a
// stream cleaned up after itself.
func (b *Bus) Subscribers(threadID string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.subs[threadID])
}

// Queue is an in-memory queue with a Drain helper: tests process jobs
// exactly like the worker would. It honours the port's keys (a duplicate
// key is refused, Cancel drops it) and an optional depth cap, so the
// engine's behaviour around both can be tested without a database.
type Queue struct {
	mu    sync.Mutex
	items []queued
	// MaxDepth refuses a fresh dispatch past this many waiting jobs; zero is
	// unbounded.
	MaxDepth int
}

type queued struct {
	job      ports.RunJob
	delay    time.Duration
	key      string
	priority int
}

// NewQueue makes an empty Queue.
func NewQueue() *Queue { return &Queue{} }

func (q *Queue) Enqueue(_ context.Context, job ports.RunJob, opts *ports.EnqueueOptions) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	item := queued{job: job}
	if opts != nil {
		item.delay, item.key, item.priority = opts.Delay, opts.Key, opts.Priority
	}
	if item.key != "" {
		for _, it := range q.items {
			if it.key == item.key {
				return ports.ErrDuplicateJob
			}
		}
	}
	// The cap counts everything ready to run, but refuses only new work: a
	// retry, a resume or an expiry belongs to a run already under way.
	if q.MaxDepth > 0 && job.Kind == ports.JobDispatch {
		ready := 0
		for _, it := range q.items {
			if it.delay <= 0 {
				ready++
			}
		}
		if ready >= q.MaxDepth {
			return ports.ErrQueueFull
		}
	}
	q.items = append(q.items, item)
	return nil
}

// Cancel drops every waiting job under key.
func (q *Queue) Cancel(_ context.Context, key string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	kept := q.items[:0]
	for _, it := range q.items {
		if key == "" || it.key != key {
			kept = append(kept, it)
		}
	}
	q.items = kept
	return nil
}

// Find returns the earliest waiting job for a run, or nil.
func (q *Queue) Find(_ context.Context, runID string) (*ports.QueuedJob, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, it := range q.items {
		if it.job.RunID == runID && runID != "" {
			return &ports.QueuedJob{ID: fmt.Sprint(i), RunID: runID, ThreadID: it.job.ThreadID, Kind: it.job.Kind, RunAt: time.Now().Add(it.delay), Position: i}, nil
		}
	}
	return nil, nil
}

// Stats counts what waits.
func (q *Queue) Stats(_ context.Context) (ports.QueueStats, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var s ports.QueueStats
	for _, it := range q.items {
		if it.delay > 0 {
			s.Delayed++
		} else {
			s.Ready++
		}
	}
	return s, nil
}

// Items are the jobs waiting, oldest first.
func (q *Queue) Items() []ports.RunJob {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]ports.RunJob, 0, len(q.items))
	for _, it := range q.items {
		out = append(out, it.job)
	}
	return out
}

// Delays are the delivery delays requested per enqueue, index-aligned with Items.
func (q *Queue) Delays() []time.Duration {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]time.Duration, 0, len(q.items))
	for _, it := range q.items {
		out = append(out, it.delay)
	}
	return out
}

// Keys are the dedupe keys per enqueue, index-aligned with Items.
func (q *Queue) Keys() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]string, 0, len(q.items))
	for _, it := range q.items {
		out = append(out, it.key)
	}
	return out
}

// Priorities are the priorities per enqueue, index-aligned with Items.
func (q *Queue) Priorities() []int {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]int, 0, len(q.items))
	for _, it := range q.items {
		out = append(out, it.priority)
	}
	return out
}

// Len is how many jobs wait.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// Shift takes the oldest job, or false.
func (q *Queue) Shift() (ports.RunJob, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return ports.RunJob{}, false
	}
	job := q.items[0].job
	q.items = q.items[1:]
	return job, true
}

// Drain hands every waiting job to the handler, in order, including jobs
// the handler enqueues. Returns how many were handled.
func (q *Queue) Drain(ctx context.Context, handler func(ctx context.Context, job ports.RunJob) error) (int, error) {
	n := 0
	for {
		job, ok := q.Shift()
		if !ok {
			return n, nil
		}
		if err := handler(ctx, job); err != nil {
			return n, err
		}
		n++
	}
}

// Storage is a full in-memory Storage: tests, demos, and a template for
// custom adapters.
type Storage struct {
	mu       sync.Mutex
	threads  map[string]*ports.ThreadDTO
	messages map[string][]ports.MessageDTO
	events   map[string][]ports.AgentEvent
	usage    []usageRow
	// LastContext is the StorageContext of the most recent call, so a test
	// can prove the run state reached the adapter (§2.10).
	LastContext ports.StorageContext
	// Contexts records every StorageContext seen, in order.
	Contexts []ports.StorageContext
}

type usageRow struct {
	threadID string
	u        ports.NewUsage
	id       string
	at       time.Time
}

// NewStorage makes an empty Storage.
func NewStorage() *Storage {
	return &Storage{
		threads:  map[string]*ports.ThreadDTO{},
		messages: map[string][]ports.MessageDTO{},
		events:   map[string][]ports.AgentEvent{},
	}
}

func (s *Storage) saw(sc ports.StorageContext) {
	s.LastContext = sc
	s.Contexts = append(s.Contexts, sc)
}

func (s *Storage) Threads() ports.ThreadStore   { return threads{s} }
func (s *Storage) Messages() ports.MessageStore { return messages{s} }
func (s *Storage) Events() ports.EventStore     { return events{s} }
func (s *Storage) Usage() ports.UsageStore      { return usage{s} }

// MessageRows is the raw message list for a thread, for tests.
func (s *Storage) MessageRows(threadID string) []ports.MessageDTO {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ports.MessageDTO(nil), s.messages[threadID]...)
}

// UsageRows is every recorded usage segment, for tests.
func (s *Storage) UsageRows() []struct {
	ThreadID string
	ports.NewUsage
} {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]struct {
		ThreadID string
		ports.NewUsage
	}, 0, len(s.usage))
	for _, r := range s.usage {
		out = append(out, struct {
			ThreadID string
			ports.NewUsage
		}{r.threadID, r.u})
	}
	return out
}

type threads struct{ s *Storage }

func (t threads) Get(_ context.Context, threadID string, sc ports.StorageContext) (*ports.ThreadDTO, error) {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	t.s.saw(sc)
	th, ok := t.s.threads[threadID]
	if !ok {
		return nil, nil
	}
	copy := *th
	return &copy, nil
}

func (t threads) Create(_ context.Context, init ports.ThreadInit, sc ports.StorageContext) (*ports.ThreadDTO, error) {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	t.s.saw(sc)
	model := init.Model
	if model == "" {
		model = "gpt-4o"
	}
	now := time.Now()
	th := &ports.ThreadDTO{ID: newID(), State: ports.StateIdle, Model: model, CreatedAt: now, UpdatedAt: now}
	t.s.threads[th.ID] = th
	copy := *th
	return &copy, nil
}

func (t threads) List(_ context.Context, sc ports.StorageContext) ([]ports.ThreadDTO, error) {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	t.s.saw(sc)
	out := make([]ports.ThreadDTO, 0, len(t.s.threads))
	for _, th := range t.s.threads {
		out = append(out, *th)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].UpdatedAt.After(out[j].UpdatedAt) })
	return out, nil
}

func (t threads) SetState(_ context.Context, threadID string, state ports.ExecutionState, sc ports.StorageContext) error {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	t.s.saw(sc)
	th, ok := t.s.threads[threadID]
	if !ok {
		return fmt.Errorf("unknown thread %s", threadID)
	}
	th.State = state
	th.UpdatedAt = time.Now()
	return nil
}

func (t threads) Delete(_ context.Context, threadID string, sc ports.StorageContext) error {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	t.s.saw(sc)
	if _, ok := t.s.threads[threadID]; !ok {
		return fmt.Errorf("unknown thread %s", threadID)
	}
	// The cascade reaches the sibling sections (§3.2). Run records are not
	// here; they are the platform's own (§2.9).
	delete(t.s.threads, threadID)
	delete(t.s.messages, threadID)
	delete(t.s.events, threadID)
	kept := t.s.usage[:0]
	for _, u := range t.s.usage {
		if u.threadID != threadID {
			kept = append(kept, u)
		}
	}
	t.s.usage = kept
	return nil
}

func (t threads) ClaimState(_ context.Context, threadID string, from, to ports.ExecutionState, sc ports.StorageContext) (bool, error) {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	t.s.saw(sc)
	th, ok := t.s.threads[threadID]
	if !ok || th.State != from {
		return false, nil
	}
	th.State = to
	th.UpdatedAt = time.Now()
	return true, nil
}

type messages struct{ s *Storage }

func (m messages) Append(_ context.Context, threadID string, msg ports.NewMessage, sc ports.StorageContext) (*ports.MessageDTO, error) {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	m.s.saw(sc)
	dto := ports.MessageDTO{
		ID: newID(), ThreadID: threadID, AgentID: msg.AgentID, Role: msg.Role,
		Content: append(json.RawMessage(nil), msg.Content...), CreatedAt: time.Now(),
	}
	m.s.messages[threadID] = append(m.s.messages[threadID], dto)
	return &dto, nil
}

func (m messages) List(_ context.Context, threadID string, scope *ports.MessageScope, sc ports.StorageContext) ([]ports.MessageDTO, error) {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	m.s.saw(sc)
	rows := m.s.messages[threadID]
	out := make([]ports.MessageDTO, 0, len(rows))
	for _, r := range rows {
		if scope == nil || r.AgentID == scope.AgentID {
			out = append(out, r)
		}
	}
	return out, nil
}

func (m messages) DeleteFrom(_ context.Context, threadID, messageID string, sc ports.StorageContext) (int, error) {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	m.s.saw(sc)
	rows := m.s.messages[threadID]
	for i, r := range rows {
		if r.ID == messageID {
			n := len(rows) - i
			m.s.messages[threadID] = rows[:i]
			return n, nil
		}
	}
	return 0, nil
}

type events struct{ s *Storage }

func (e events) Append(_ context.Context, threadID string, ev ports.AgentEvent, sc ports.StorageContext) error {
	e.s.mu.Lock()
	defer e.s.mu.Unlock()
	e.s.saw(sc)
	e.s.events[threadID] = append(e.s.events[threadID], ev)
	return nil
}

func (e events) ListSince(_ context.Context, threadID string, sinceSeq int64, sc ports.StorageContext) ([]ports.AgentEvent, error) {
	e.s.mu.Lock()
	defer e.s.mu.Unlock()
	e.s.saw(sc)
	var out []ports.AgentEvent
	for _, ev := range e.s.events[threadID] {
		if ev.Seq > sinceSeq {
			out = append(out, ev)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}

func (e events) Latest(_ context.Context, threadID, typ string, sc ports.StorageContext) (*ports.AgentEvent, error) {
	e.s.mu.Lock()
	defer e.s.mu.Unlock()
	e.s.saw(sc)
	list := e.s.events[threadID]
	for i := len(list) - 1; i >= 0; i-- {
		if list[i].Type == typ {
			ev := list[i]
			return &ev, nil
		}
	}
	return nil, nil
}

func (e events) ListByType(_ context.Context, threadID, typ string, sc ports.StorageContext) ([]ports.AgentEvent, error) {
	e.s.mu.Lock()
	defer e.s.mu.Unlock()
	e.s.saw(sc)
	var out []ports.AgentEvent
	for _, ev := range e.s.events[threadID] {
		if ev.Type == typ {
			out = append(out, ev)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}

type usage struct{ s *Storage }

func (u usage) Record(_ context.Context, threadID string, n ports.NewUsage, sc ports.StorageContext) error {
	u.s.mu.Lock()
	defer u.s.mu.Unlock()
	u.s.saw(sc)
	u.s.usage = append(u.s.usage, usageRow{
		threadID: threadID, u: n, id: newID(), at: time.Now(),
	})
	return nil
}

func (u usage) Total(_ context.Context, threadID string, f ports.UsageFilter, sc ports.StorageContext) (ports.UsageTotals, error) {
	u.s.mu.Lock()
	defer u.s.mu.Unlock()
	u.s.saw(sc)
	var agg ports.UsageAggregator
	for _, r := range u.s.usage {
		if r.threadID != threadID {
			continue
		}
		if f.RunID != "" && r.u.RunID != f.RunID {
			continue
		}
		agg.Add(r.u)
	}
	return agg.Totals(), nil
}
