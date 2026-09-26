package core

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// One sandbox per thread, kept between messages.
//
// The first sandbox call in a thread makes it and keeps its id in the kv
// under the thread. Every later call, in this run or the next (a new
// message, a resume after an approval, a retry, another worker), connects
// to the same one, so files stay. The run lock means one run at a time uses
// it; a run's subagents share it, as they share the thread.
//
// It ends when the thread is deleted, when it has sat idle past
// SandboxIdleTTL, or at SandboxMaxLifetime after it was made. Each use
// pushes the sandbox's own timeout back, so an idle one shuts itself down
// even when no app is running to end it. The next call then makes a fresh
// one and says the old files are lost. The TS runtime does the same
// (src/core/builtin/sandbox.ts).

// ThreadSandbox is a thread's sandbox, as a call gets it.
type ThreadSandbox struct {
	Sandbox ports.Sandbox
	// Created is true when this call made the sandbox.
	Created bool
	// Lost is true when the thread had a sandbox that is gone now: idle too
	// long, past its lifetime, or ended on its own. Its files are lost, and
	// a tool should tell the model so it does not expect them.
	Lost bool
}

// sandboxRecord is what the kv keeps for a thread's sandbox. Times are Unix
// milliseconds, as the TS runtime writes them.
type sandboxRecord struct {
	ID         string `json:"id"`
	Provider   string `json:"provider"`
	CreatedAt  int64  `json:"createdAt"`
	LastUsedAt int64  `json:"lastUsedAt"`
}

// SandboxKey is where the kv keeps a thread's sandbox.
func SandboxKey(threadID string) string { return "agent:tool:sandbox:" + threadID }

// pushEvery: a sandbox's own timeout is pushed back at most this often, and
// set this much past the idle limit so it never ends before the runtime
// counts it as idle.
const pushEvery = time.Minute

const maxHandles = 1000

type heldSandbox struct {
	sandbox  ports.Sandbox
	pushedAt time.Time
}

var sandboxes = struct {
	sync.Mutex
	// handles this process already holds, by sandbox id, so a call does not
	// reconnect every time; order keeps the oldest first.
	handles map[string]*heldSandbox
	order   []string
	// pending calls, by thread: parallel subagents get one sandbox, not one
	// each.
	pending map[string]*sandboxCall
}{handles: map[string]*heldSandbox{}, pending: map[string]*sandboxCall{}}

type sandboxCall struct {
	done chan struct{}
	ts   ThreadSandbox
	err  error
}

// SandboxFor returns the thread's sandbox, for a tool you write yourself.
// Pass the ctx your tool's Execute was given:
//
//	ts, err := agentenkit.SandboxFor(ctx)
//	text, err := ts.Sandbox.Filesystem().ReadFile(ctx, path)
func SandboxFor(ctx context.Context) (ThreadSandbox, error) {
	run, ok := ToolRunFromContext(ctx)
	if !ok {
		return ThreadSandbox{}, errors.New("SandboxFor: this tool is not running inside an agentenkit run")
	}
	return GetThreadSandbox(ctx, run)
}

// WithSandbox runs fn on the thread's sandbox. When the sandbox turns out
// to be gone part way (it ended on its own), a fresh one is made and fn
// runs once more, told Lost.
func WithSandbox[T any](ctx context.Context, fn func(ThreadSandbox) (T, error)) (T, error) {
	run, ok := ToolRunFromContext(ctx)
	if !ok {
		var zero T
		return zero, errors.New("WithSandbox: this tool is not running inside an agentenkit run")
	}
	return WithThreadSandbox(ctx, run, fn)
}

// WithThreadSandbox is WithSandbox for a ToolRun in hand.
func WithThreadSandbox[T any](ctx context.Context, run ToolRun, fn func(ThreadSandbox) (T, error)) (T, error) {
	ts, err := GetThreadSandbox(ctx, run)
	if err != nil {
		var zero T
		return zero, err
	}
	out, err := fn(ts)
	if err == nil || !errors.Is(err, ports.ErrSandboxGone) {
		return out, err
	}
	forgetHandle(ts.Sandbox.ID())
	// Only while the record still names the sandbox that is gone: a
	// parallel call may already have made the next one.
	key := SandboxKey(run.ThreadID)
	if raw, ok, err := run.Deps.Kv.Get(ctx, key); err == nil && ok {
		if rec, _ := readSandboxRecord(ctx, run.Deps.Kv, key); rec != nil && rec.ID == ts.Sandbox.ID() {
			_, _ = run.Deps.Kv.DelIfValue(ctx, key, raw)
		}
	}
	ts, err = GetThreadSandbox(ctx, run)
	if err != nil {
		var zero T
		return zero, err
	}
	ts.Lost = true
	return fn(ts)
}

// GetThreadSandbox returns the thread's sandbox, making or reconnecting it.
func GetThreadSandbox(ctx context.Context, run ToolRun) (ThreadSandbox, error) {
	provider := run.Deps.Tools.Sandbox
	if provider == nil {
		return ThreadSandbox{}, errors.New("No sandbox: pass RuntimeOptions.Tools.Sandbox")
	}
	sandboxes.Lock()
	if call, ok := sandboxes.pending[run.ThreadID]; ok {
		sandboxes.Unlock()
		select {
		case <-call.done:
			return call.ts, call.err
		case <-ctx.Done():
			return ThreadSandbox{}, ctx.Err()
		}
	}
	call := &sandboxCall{done: make(chan struct{})}
	sandboxes.pending[run.ThreadID] = call
	sandboxes.Unlock()

	call.ts, call.err = acquireSandbox(ctx, run, provider)
	sandboxes.Lock()
	delete(sandboxes.pending, run.ThreadID)
	sandboxes.Unlock()
	close(call.done)
	return call.ts, call.err
}

func acquireSandbox(ctx context.Context, run ToolRun, provider ports.SandboxProvider) (ThreadSandbox, error) {
	deps := run.Deps
	idle := deps.Config.SandboxIdleTTL
	lifetime := deps.Config.SandboxMaxLifetime
	now := time.Now()
	key := SandboxKey(run.ThreadID)
	rec, err := readSandboxRecord(ctx, deps.Kv, key)
	if err != nil {
		return ThreadSandbox{}, err
	}
	// A sandbox from another adapter cannot be reached from this one.
	if rec != nil && rec.Provider != provider.Name() {
		rec = nil
	}

	lost := false
	if rec != nil && (now.Sub(time.UnixMilli(rec.LastUsedAt)) > idle || now.Sub(time.UnixMilli(rec.CreatedAt)) > lifetime) {
		destroyQuietly(ctx, deps, provider, rec.ID)
		rec = nil
		lost = true
	}

	var sandbox ports.Sandbox
	created := false
	if rec != nil {
		s, err := connectHeld(ctx, provider, rec.ID)
		if err == nil {
			err = pushTimeout(ctx, s, now, min(idle+pushEvery, lifetime-now.Sub(time.UnixMilli(rec.CreatedAt))))
		}
		switch {
		case err == nil:
			sandbox = s
			rec.LastUsedAt = now.UnixMilli()
		case errors.Is(err, ports.ErrSandboxGone):
			forgetHandle(rec.ID)
			rec = nil
			lost = true
		default:
			return ThreadSandbox{}, err
		}
	}
	if rec == nil {
		s, err := provider.Create(ctx, ports.CreateSandboxOptions{
			Timeout:  min(idle+pushEvery, lifetime),
			Metadata: ports.SandboxMetadata{ThreadID: run.ThreadID, RunID: run.RunID, State: run.State},
		})
		if err != nil {
			return ThreadSandbox{}, err
		}
		remember(s, now)
		sandbox = s
		rec = &sandboxRecord{ID: s.ID(), Provider: provider.Name(), CreatedAt: now.UnixMilli(), LastUsedAt: now.UnixMilli()}
		created = true
	}
	raw, _ := json.Marshal(rec)
	if _, err := deps.Kv.Set(ctx, key, string(raw), ports.SetOptions{Expiry: lifetime}); err != nil {
		return ThreadSandbox{}, err
	}
	return ThreadSandbox{Sandbox: sandbox, Created: created, Lost: lost}, nil
}

// connectHeld reuses a handle this process holds, or connects.
func connectHeld(ctx context.Context, provider ports.SandboxProvider, id string) (ports.Sandbox, error) {
	sandboxes.Lock()
	held, ok := sandboxes.handles[id]
	sandboxes.Unlock()
	if ok {
		return held.sandbox, nil
	}
	s, err := provider.Connect(ctx, id)
	if err != nil {
		return nil, err
	}
	// A fresh handle pushes the timeout on its first use.
	remember(s, time.Time{})
	return s, nil
}

// pushTimeout pushes the sandbox's own end back, when it was not pushed in
// the last minute. A provider that cannot is left as it is.
func pushTimeout(ctx context.Context, s ports.Sandbox, now time.Time, d time.Duration) error {
	sandboxes.Lock()
	held := sandboxes.handles[s.ID()]
	due := held == nil || now.Sub(held.pushedAt) >= pushEvery
	sandboxes.Unlock()
	if !due {
		return nil
	}
	if err := s.SetTimeout(ctx, d); err != nil && !errors.Is(err, ports.ErrSandboxUnsupported) {
		return err
	}
	sandboxes.Lock()
	if held != nil {
		held.pushedAt = now
	}
	sandboxes.Unlock()
	return nil
}

func remember(s ports.Sandbox, pushedAt time.Time) {
	sandboxes.Lock()
	defer sandboxes.Unlock()
	if _, ok := sandboxes.handles[s.ID()]; !ok {
		if len(sandboxes.order) >= maxHandles {
			delete(sandboxes.handles, sandboxes.order[0])
			sandboxes.order = sandboxes.order[1:]
		}
		sandboxes.order = append(sandboxes.order, s.ID())
	}
	sandboxes.handles[s.ID()] = &heldSandbox{sandbox: s, pushedAt: pushedAt}
}

func forgetHandle(id string) {
	sandboxes.Lock()
	defer sandboxes.Unlock()
	delete(sandboxes.handles, id)
	for i, v := range sandboxes.order {
		if v == id {
			sandboxes.order = append(sandboxes.order[:i], sandboxes.order[i+1:]...)
			break
		}
	}
}

// ForgetSandboxHandles drops every handle this process holds, as a fresh
// worker would start. For tests.
func ForgetSandboxHandles() {
	sandboxes.Lock()
	defer sandboxes.Unlock()
	sandboxes.handles = map[string]*heldSandbox{}
	sandboxes.order = nil
}

func destroyQuietly(ctx context.Context, deps ports.RuntimePorts, provider ports.SandboxProvider, id string) {
	forgetHandle(id)
	s, err := provider.Connect(ctx, id)
	if err == nil {
		err = s.Destroy(ctx)
	}
	if err != nil && !errors.Is(err, ports.ErrSandboxGone) {
		Logger(deps).Error("sandbox not destroyed", "sandboxId", id, "err", err)
	}
}

// DestroyThreadSandbox ends the thread's sandbox, when it has one. Part of
// deleting a thread; it never fails, since the thread is gone either way.
func DestroyThreadSandbox(ctx context.Context, deps ports.RuntimePorts, threadID string) {
	key := SandboxKey(threadID)
	rec, _ := readSandboxRecord(ctx, deps.Kv, key)
	if provider := deps.Tools.Sandbox; provider != nil && rec != nil && rec.Provider == provider.Name() {
		destroyQuietly(ctx, deps, provider, rec.ID)
	}
	_ = deps.Kv.Del(ctx, key)
}

func readSandboxRecord(ctx context.Context, kv ports.Kv, key string) (*sandboxRecord, error) {
	raw, ok, err := kv.Get(ctx, key)
	if err != nil || !ok || raw == "" {
		return nil, err
	}
	var rec sandboxRecord
	if json.Unmarshal([]byte(raw), &rec) != nil || rec.ID == "" || rec.Provider == "" {
		return nil, nil
	}
	return &rec, nil
}
