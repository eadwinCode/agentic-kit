package agentenkit_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zendev-sh/goai/provider"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/memory"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/core"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// One sandbox per thread (spec T3). The same cases run in the TS package
// (test/sandbox-lifecycle.test.ts), under the same names.

const (
	sandboxIdle     = 30 * time.Minute
	sandboxLifetime = 24 * time.Hour
)

// touchModel calls touch `calls` times at once, then answers once the tool
// results are in.
type touchModel struct{ calls int }

func (m *touchModel) ModelID() string { return "mock" }

func (m *touchModel) DoGenerate(context.Context, provider.GenerateParams) (*provider.GenerateResult, error) {
	return nil, fmt.Errorf("not used")
}

func (m *touchModel) DoStream(_ context.Context, p provider.GenerateParams) (*provider.StreamResult, error) {
	answered := len(p.Messages) > 0 && p.Messages[len(p.Messages)-1].Role == provider.RoleTool
	ch := make(chan provider.StreamChunk, 16)
	go func() {
		defer close(ch)
		usage := provider.Usage{InputTokens: 1, OutputTokens: 1}
		if answered {
			ch <- provider.StreamChunk{Type: provider.ChunkText, Text: "done"}
			ch <- provider.StreamChunk{Type: provider.ChunkFinish, FinishReason: provider.FinishStop, Usage: usage}
			return
		}
		for i := 0; i < m.calls; i++ {
			ch <- provider.StreamChunk{Type: provider.ChunkToolCall, ToolCallID: fmt.Sprintf("c%d", i), ToolName: "touch", ToolInput: fmt.Sprintf(`{"name":"f%d"}`, i)}
		}
		ch <- provider.StreamChunk{Type: provider.ChunkFinish, FinishReason: provider.FinishToolCalls, Usage: usage}
	}()
	return &provider.StreamResult{Stream: ch}, nil
}

type seenSandbox struct {
	SandboxID string
	Created   bool
	Lost      bool
	Files     []string
}

type sandboxHarness struct {
	*harness
	sandboxes *memory.Sandbox
	agent     *agentenkit.AgentHandle
	mu        sync.Mutex
	seen      []seenSandbox
}

func sandboxRuntime(t *testing.T, calls int) *sandboxHarness {
	t.Helper()
	core.ForgetSandboxHandles()
	t.Cleanup(core.ForgetSandboxHandles)
	sh := &sandboxHarness{sandboxes: memory.NewSandbox(nil, "")}
	model := &touchModel{calls: calls}
	sh.harness = makeRuntimeOpts(t, nil, func(o *agentenkit.RuntimeOptions) {
		o.Tools.Sandbox = sh.sandboxes
		o.ResolveModel = func(string) (agentenkit.ResolvedModel, error) {
			return agentenkit.ResolvedModel{Instance: func() provider.LanguageModel { return model }, ContextWindow: 128_000}, nil
		}
	})
	touch := agentenkit.AgentTool("touch", "Write a file in the sandbox", func(ctx context.Context, in map[string]any, _ agentenkit.ToolContext) (string, error) {
		return agentenkit.WithSandbox(ctx, func(ts agentenkit.ThreadSandbox) (string, error) {
			fs := ts.Sandbox.Filesystem()
			if err := fs.WriteFile(ctx, fmt.Sprint(in["name"])+".txt", []byte("x")); err != nil {
				return "", err
			}
			entries, err := fs.ReadDir(ctx, ".")
			if err != nil {
				return "", err
			}
			names := []string{}
			for _, e := range entries {
				names = append(names, e.Name)
			}
			sh.mu.Lock()
			sh.seen = append(sh.seen, seenSandbox{SandboxID: ts.Sandbox.ID(), Created: ts.Created, Lost: ts.Lost, Files: names})
			sh.mu.Unlock()
			return "ok", nil
		})
	})
	sh.agent = sh.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "coder", Tools: []agentenkit.Tool{touch}})
	return sh
}

func (h *sandboxHarness) runIn(t *testing.T, threadID string) string {
	t.Helper()
	res := h.run(t, h.agent, agentenkit.RunInput{Prompt: "go", ThreadID: threadID})
	h.handleNext(t)
	return res.ThreadID
}

func (h *sandboxHarness) record(t *testing.T, threadID string) map[string]any {
	t.Helper()
	var rec map[string]any
	if err := json.Unmarshal([]byte(h.kvGet(core.SandboxKey(threadID))), &rec); err != nil {
		t.Fatalf("no sandbox record: %v", err)
	}
	return rec
}

func (h *sandboxHarness) setRecord(t *testing.T, threadID string, change map[string]any) {
	t.Helper()
	rec := h.record(t, threadID)
	for k, v := range change {
		rec[k] = v
	}
	raw, _ := json.Marshal(rec)
	if _, err := h.kv.Set(h.ctx, core.SandboxKey(threadID), string(raw), ports.SetOptions{}); err != nil {
		t.Fatal(err)
	}
}

func (h *sandboxHarness) seenAt(i int) seenSandbox {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.seen[i]
}

func mustSeen(t *testing.T, got, want seenSandbox) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func msAgo(d time.Duration) int64 { return time.Now().Add(-d).UnixMilli() }

func TestSandboxLifecycle_TheFirstSandboxCallInAThreadMakesOne(t *testing.T) {
	h := sandboxRuntime(t, 1)
	threadID := h.runIn(t, "")
	created := h.sandboxes.Created()
	mustEqual(t, len(created), 1, "sandboxes made")
	mustEqual(t, created[0].Metadata.ThreadID, threadID, "thread")
	mustEqual(t, created[0].Timeout, sandboxIdle+time.Minute, "timeout")
	mustSeen(t, h.seenAt(0), seenSandbox{SandboxID: "mem-1", Created: true, Files: []string{"f0.txt"}})
	mustEqual(t, h.record(t, threadID)["id"], "mem-1", "record")
}

func TestSandboxLifecycle_ALaterRunInTheThreadFindsTheSameSandboxAndItsFiles(t *testing.T) {
	h := sandboxRuntime(t, 1)
	threadID := h.runIn(t, "")
	core.ForgetSandboxHandles() // as another worker would come to it
	h.runIn(t, threadID)
	mustEqual(t, len(h.sandboxes.Created()), 1, "sandboxes made")
	mustSeen(t, h.seenAt(1), seenSandbox{SandboxID: "mem-1", Files: []string{"f0.txt"}})
}

func TestSandboxLifecycle_ParallelCallsInOneThreadShareOneSandbox(t *testing.T) {
	h := sandboxRuntime(t, 3)
	h.runIn(t, "")
	mustEqual(t, len(h.sandboxes.Created()), 1, "sandboxes made")
	for i := range 3 {
		mustEqual(t, h.seenAt(i).SandboxID, "mem-1", "sandbox")
	}
}

func TestSandboxLifecycle_ASandboxIdlePastSandboxIdleTtlIsReplacedAndItsFilesAreLost(t *testing.T) {
	h := sandboxRuntime(t, 1)
	threadID := h.runIn(t, "")
	h.setRecord(t, threadID, map[string]any{"lastUsedAt": msAgo(sandboxIdle + time.Second)})
	h.runIn(t, threadID)
	mustStrings(t, h.sandboxes.Destroyed(), []string{"mem-1"}, "destroyed")
	mustSeen(t, h.seenAt(1), seenSandbox{SandboxID: "mem-2", Created: true, Lost: true, Files: []string{"f0.txt"}})
}

func TestSandboxLifecycle_ASandboxPastSandboxMaxLifetimeIsReplaced(t *testing.T) {
	h := sandboxRuntime(t, 1)
	threadID := h.runIn(t, "")
	h.setRecord(t, threadID, map[string]any{"createdAt": msAgo(sandboxLifetime + time.Second)})
	h.runIn(t, threadID)
	mustStrings(t, h.sandboxes.Destroyed(), []string{"mem-1"}, "destroyed")
	mustEqual(t, h.seenAt(1).Lost, true, "lost")
}

func TestSandboxLifecycle_ASandboxThatEndedOnItsOwnIsReplacedAndItsFilesAreLost(t *testing.T) {
	h := sandboxRuntime(t, 1)
	threadID := h.runIn(t, "")
	h.sandboxes.End("mem-1")
	h.runIn(t, threadID) // this process still holds a handle on it
	mustSeen(t, h.seenAt(1), seenSandbox{SandboxID: "mem-2", Created: true, Lost: true, Files: []string{"f0.txt"}})
	core.ForgetSandboxHandles()
	h.sandboxes.End("mem-2")
	h.runIn(t, threadID) // and here it does not
	mustSeen(t, h.seenAt(2), seenSandbox{SandboxID: "mem-3", Created: true, Lost: true, Files: []string{"f0.txt"}})
}

func TestSandboxLifecycle_EachUsePushesTheSandboxsOwnEndBackNeverPastItsLifetime(t *testing.T) {
	h := sandboxRuntime(t, 1)
	threadID := h.runIn(t, "")
	mustEqual(t, len(h.sandboxes.Timeouts()), 0, "pushes after making it") // it was just made with its time
	core.ForgetSandboxHandles()
	h.runIn(t, threadID)
	timeouts := h.sandboxes.Timeouts()
	if !reflect.DeepEqual(timeouts, []memory.SandboxTimeout{{SandboxID: "mem-1", Timeout: sandboxIdle + time.Minute}}) {
		t.Fatalf("timeouts: %+v", timeouts)
	}
	core.ForgetSandboxHandles()
	h.setRecord(t, threadID, map[string]any{"createdAt": msAgo(sandboxLifetime - 5*time.Minute)})
	h.runIn(t, threadID)
	timeouts = h.sandboxes.Timeouts()
	last := timeouts[len(timeouts)-1].Timeout
	if last > 5*time.Minute || last <= 4*time.Minute {
		t.Fatalf("last push %v, want just under 5 minutes", last)
	}
}

func TestSandboxLifecycle_DeletingTheThreadDestroysItsSandbox(t *testing.T) {
	h := sandboxRuntime(t, 1)
	threadID := h.runIn(t, "")
	res, err := h.rt.DeleteThread(h.ctx, threadID, nil)
	if err != nil || !res.Accepted {
		t.Fatalf("delete: %+v %v", res, err)
	}
	mustStrings(t, h.sandboxes.Destroyed(), []string{"mem-1"}, "destroyed")
	mustEqual(t, h.kvGet(core.SandboxKey(threadID)), "", "record")
}

func TestSandboxLifecycle_TwoCallsThatFindTheSandboxGoneMakeOneNewSandboxBetweenThem(t *testing.T) {
	core.ForgetSandboxHandles()
	t.Cleanup(core.ForgetSandboxHandles)
	ctx := context.Background()
	sandboxes := memory.NewSandbox(nil, "")
	run := core.ToolRun{Deps: ports.RuntimePorts{Kv: memory.NewKv(), Config: agentenkit.DefaultConfig(), Tools: ports.BuiltinToolPorts{Sandbox: sandboxes}}, ThreadID: "t-race"}
	if _, err := core.GetThreadSandbox(ctx, run); err != nil {
		t.Fatal(err)
	}
	sandboxes.End("mem-1")
	write := func(ts core.ThreadSandbox) (string, error) {
		return ts.Sandbox.ID(), ts.Sandbox.Filesystem().WriteFile(ctx, "a.txt", []byte("x"))
	}
	firstDone := make(chan struct{})
	var first, second string
	var firstErr, secondErr error
	go func() {
		defer close(firstDone)
		first, firstErr = core.WithThreadSandbox(ctx, run, write)
	}()
	// The second finds mem-1 gone only after the first has made mem-2.
	second, secondErr = core.WithThreadSandbox(ctx, run, func(ts core.ThreadSandbox) (string, error) {
		<-firstDone
		return write(ts)
	})
	if firstErr != nil || secondErr != nil {
		t.Fatalf("errors: %v, %v", firstErr, secondErr)
	}
	mustStrings(t, []string{first, second}, []string{"mem-2", "mem-2"}, "sandboxes used")
	mustStrings(t, sandboxes.Live(), []string{"mem-2"}, "live")
}

func TestSandboxLifecycle_WithNoSandboxSetUpASandboxCallSaysSo(t *testing.T) {
	cfg := agentenkit.DefaultConfig()
	run := core.ToolRun{Deps: ports.RuntimePorts{Kv: memory.NewKv(), Config: cfg}, ThreadID: "t"}
	_, err := core.GetThreadSandbox(context.Background(), run)
	if err == nil || !strings.Contains(err.Error(), "No sandbox: pass RuntimeOptions.Tools.Sandbox") {
		t.Fatalf("got %v", err)
	}
}
