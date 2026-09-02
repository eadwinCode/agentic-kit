package agentenkit_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/zendev-sh/goai/provider"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/memory"
	memoryadmin "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/admin/memory"
)

// call is one tool call a scripted step asks for.
type call struct {
	id, name, args string
}

// step is one scripted model round-trip.
type step struct {
	text  string
	calls []call
	usage *[2]int // prompt, completion; default 10, 5
	delay time.Duration
	err   error
}

// scriptedModel plays back one scripted step per round-trip. The platform
// loop makes one round-trip per iteration, so step N answers iteration N.
// It records every request so a test can inspect what reached the provider.
type scriptedModel struct {
	mu     sync.Mutex
	steps  []step
	call   int
	params []provider.GenerateParams
}

func scripted(steps ...step) *scriptedModel { return &scriptedModel{steps: steps} }

func (m *scriptedModel) ModelID() string { return "mock-scripted" }

func (m *scriptedModel) next(p provider.GenerateParams) step {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.params = append(m.params, p)
	s := m.steps[min(m.call, len(m.steps)-1)]
	m.call++
	return s
}

func (m *scriptedModel) Params() []provider.GenerateParams {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]provider.GenerateParams(nil), m.params...)
}

func (m *scriptedModel) Calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.call
}

func usageOf(s step) provider.Usage {
	p, c := 10, 5
	if s.usage != nil {
		p, c = s.usage[0], s.usage[1]
	}
	return provider.Usage{InputTokens: p, OutputTokens: c, TotalTokens: p + c}
}

func finishOf(s step) provider.FinishReason {
	if len(s.calls) > 0 {
		return provider.FinishToolCalls
	}
	return provider.FinishStop
}

func wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	select {
	case <-time.After(d):
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *scriptedModel) DoGenerate(ctx context.Context, p provider.GenerateParams) (*provider.GenerateResult, error) {
	s := m.next(p)
	if err := wait(ctx, s.delay); err != nil {
		return nil, err
	}
	if s.err != nil {
		return nil, s.err
	}
	res := &provider.GenerateResult{Text: s.text, FinishReason: finishOf(s), Usage: usageOf(s)}
	for _, c := range s.calls {
		res.ToolCalls = append(res.ToolCalls, provider.ToolCall{ID: c.id, Name: c.name, Input: json.RawMessage(c.args)})
	}
	return res, nil
}

func (m *scriptedModel) DoStream(ctx context.Context, p provider.GenerateParams) (*provider.StreamResult, error) {
	s := m.next(p)
	ch := make(chan provider.StreamChunk, 16)
	go func() {
		defer close(ch)
		if err := wait(ctx, s.delay); err != nil {
			ch <- provider.StreamChunk{Type: provider.ChunkError, Error: err}
			return
		}
		if s.err != nil {
			ch <- provider.StreamChunk{Type: provider.ChunkError, Error: s.err}
			return
		}
		if s.text != "" {
			ch <- provider.StreamChunk{Type: provider.ChunkText, Text: s.text}
		}
		for _, c := range s.calls {
			ch <- provider.StreamChunk{Type: provider.ChunkToolCall, ToolCallID: c.id, ToolName: c.name, ToolInput: c.args}
		}
		ch <- provider.StreamChunk{Type: provider.ChunkFinish, FinishReason: finishOf(s), Usage: usageOf(s)}
	}()
	return &provider.StreamResult{Stream: ch}, nil
}

// harness is one assembled runtime over the memory adapters.
type harness struct {
	rt      *agentenkit.AgentCore
	storage *memory.Storage
	bus     *memory.Bus
	queue   *memory.Queue
	kv      *memory.Kv
	admin   *memoryadmin.Store
	model   *scriptedModel
	ctx     context.Context
}

func makeRuntime(t *testing.T, model *scriptedModel, tune ...func(*agentenkit.AgentConfig)) *harness {
	t.Helper()
	cfg := agentenkit.DefaultConfig()
	cfg.StopPoll = 5 * time.Millisecond
	for _, f := range tune {
		f(&cfg)
	}
	h := &harness{
		storage: memory.NewStorage(), bus: memory.NewBus(), queue: memory.NewQueue(), kv: memory.NewKv(),
		admin: memoryadmin.New(), model: model, ctx: context.Background(),
	}
	rt, err := agentenkit.SetupAgentCore(h.ctx, agentenkit.RuntimeOptions{
		Storage: h.storage, Admin: h.admin, Bus: h.bus, Queue: h.queue, Kv: h.kv,
		ResolveModel: func(name string) (agentenkit.ResolvedModel, error) {
			if len(name) >= 8 && name[:8] == "unknown-" {
				return agentenkit.ResolvedModel{}, fmt.Errorf("unknown model %q", name)
			}
			return agentenkit.ResolvedModel{Instance: func() provider.LanguageModel { return model }, ContextWindow: 128_000}, nil
		},
		Config: &cfg,
	})
	if err != nil {
		t.Fatal(err)
	}
	h.rt = rt
	return h
}

// handleNext hands exactly one queued job to the worker.
func (h *harness) handleNext(t *testing.T) agentenkit.RunJob {
	t.Helper()
	job, ok := h.queue.Shift()
	if !ok {
		t.Fatal("no job queued")
	}
	if _, err := h.rt.Worker.HandleJob(h.ctx, job); err != nil {
		t.Fatalf("handleJob: %v", err)
	}
	return job
}

// drain hands every queued job to the worker, including jobs they enqueue.
func (h *harness) drain(t *testing.T) int {
	t.Helper()
	n, err := h.queue.Drain(h.ctx, h.rt.Worker.Handler())
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	return n
}

// run starts a run through a handle and fails the test if it was refused.
func (h *harness) run(t *testing.T, handle *agentenkit.AgentHandle, input agentenkit.RunInput) agentenkit.RunResult {
	t.Helper()
	res, err := handle.Run(h.ctx, input)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !res.Accepted {
		t.Fatalf("run refused: %s", res.Error)
	}
	return res
}

func (h *harness) events(threadID, typ string) []agentenkit.AgentEvent {
	var out []agentenkit.AgentEvent
	for _, e := range h.bus.Published() {
		if e.ThreadID == threadID && (typ == "" || e.Type == typ) {
			out = append(out, e)
		}
	}
	return out
}

func payload(e agentenkit.AgentEvent) map[string]any {
	var m map[string]any
	_ = json.Unmarshal(e.Payload, &m)
	return m
}

// states lists the STATE_CHANGE states published on a thread, in order.
func (h *harness) states(threadID string) []string {
	var out []string
	for _, e := range h.events(threadID, "STATE_CHANGE") {
		out = append(out, fmt.Sprint(payload(e)["state"]))
	}
	return out
}

func (h *harness) lastTerminal(threadID string) map[string]any {
	evs := h.events(threadID, "STATE_CHANGE")
	return payload(evs[len(evs)-1])
}

func (h *harness) roles(threadID string) []string {
	var out []string
	for _, m := range h.storage.MessageRows(threadID) {
		out = append(out, string(m.Role))
	}
	return out
}

func (h *harness) thread(t *testing.T, threadID string) agentenkit.ThreadDTO {
	t.Helper()
	th, err := h.storage.Threads().Get(h.ctx, threadID, agentenkit.StorageContext{})
	if err != nil || th == nil {
		t.Fatalf("thread %s: %v", threadID, err)
	}
	return *th
}

func (h *harness) kvGet(key string) string {
	v, _, _ := h.kv.Get(h.ctx, key)
	return v
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func mustEqual(t *testing.T, got, want any, what string) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: got %v, want %v", what, got, want)
	}
}

func mustStrings(t *testing.T, got, want []string, what string) {
	t.Helper()
	if !equalStrings(got, want) {
		t.Fatalf("%s: got %v, want %v", what, got, want)
	}
}

// tool builds a plain tool that records what it was called with.
func tool(name string, exec func(ctx context.Context, args map[string]any) (string, error)) agentenkit.Tool {
	return agentenkit.AgentTool(name, name, func(ctx context.Context, in map[string]any, _ agentenkit.ToolContext) (string, error) {
		return exec(ctx, in)
	})
}

var errBoom = errors.New("boom")
