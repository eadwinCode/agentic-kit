package agentenkit_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/memory"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/core"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// A sandbox per run, a provider that restores the work folder, the app's
// create options and the hook after each sandbox call. The same cases run in
// the TS package (test/sandbox-run-scope.test.ts), under the same names.

// restoringSandbox is the memory provider, saying its new sandboxes get the
// work folder back.
type restoringSandbox struct{ *memory.Sandbox }

func (restoringSandbox) RestoresWorkdir() bool { return true }

type scopeHarness struct {
	*harness
	sandboxes *memory.Sandbox
	agent     *agentenkit.AgentHandle
	mu        sync.Mutex
	seen      []core.ThreadSandbox
	calls     []ports.SandboxCall
}

// scopeRuntime runs the scripted model with two tools: touch writes a file
// in the sandbox; wait parks the run for a job and returns once answered.
func scopeRuntime(t *testing.T, model *scriptedModel, opt func(*agentenkit.RuntimeOptions), tune func(*agentenkit.AgentConfig)) *scopeHarness {
	t.Helper()
	core.ForgetSandboxHandles()
	t.Cleanup(core.ForgetSandboxHandles)
	sh := &scopeHarness{sandboxes: memory.NewSandbox(nil, "")}
	sh.harness = makeRuntimeOpts(t, model, func(o *agentenkit.RuntimeOptions) {
		o.Tools.Sandbox = sh.sandboxes
		if opt != nil {
			opt(o)
		}
	}, func(c *agentenkit.AgentConfig) {
		c.HITLTTL = time.Hour
		if tune != nil {
			tune(c)
		}
	})
	touch := tool("touch", func(ctx context.Context, in map[string]any) (string, error) {
		return agentenkit.WithSandbox(ctx, func(ts agentenkit.ThreadSandbox) (string, error) {
			if err := ts.Sandbox.Filesystem().WriteFile(ctx, fmt.Sprint(in["name"])+".txt", []byte("x")); err != nil {
				return "", err
			}
			sh.mu.Lock()
			sh.seen = append(sh.seen, ts)
			sh.mu.Unlock()
			return "ok", nil
		})
	})
	wait := tool("wait", func(ctx context.Context, _ map[string]any) (string, error) {
		if agentenkit.ApprovalFromContext(ctx) != nil {
			return "answered", nil
		}
		return "", agentenkit.ParkForInput(agentenkit.ParkRequest{Reason: "job"})
	})
	plain := tool("plain", func(context.Context, map[string]any) (string, error) { return "no sandbox", nil })
	sh.agent = sh.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "coder", Model: "gpt-4o", Tools: []agentenkit.Tool{touch, wait, plain},
	})
	return sh
}

func runScope(c *agentenkit.AgentConfig) { c.SandboxScope = ports.SandboxScopeRun }

func (h *scopeHarness) seenAt(i int) core.ThreadSandbox {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.seen[i]
}

func (h *scopeHarness) respond(t *testing.T, threadID, callID string) {
	t.Helper()
	h.queue.Shift() // the park's expiry job; the answer comes first
	res, err := h.rt.HITL.Respond(h.ctx, agentenkit.RespondInput{ThreadID: threadID, ToolCallID: callID, Approved: true})
	if err != nil || !res.Delivered {
		t.Fatalf("respond: %v %+v", err, res)
	}
	h.drain(t)
}

func touchStep(id, name string) step {
	return step{calls: []call{{id, "touch", fmt.Sprintf(`{"name":%q}`, name)}}}
}

func TestSandboxRunScope_ARunsSandboxEndsWithTheRunAndTheNextRunStartsFresh(t *testing.T) {
	h := scopeRuntime(t, scripted(touchStep("c1", "a"), step{text: "done"}, touchStep("c2", "b"), step{text: "done"}), nil, runScope)
	ran := h.run(t, h.agent, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	mustStrings(t, h.sandboxes.Destroyed(), []string{"mem-1"}, "destroyed when the run ended")
	mustEqual(t, h.kvGet(core.SandboxKey(ran.ThreadID)), "", "record dropped")

	h.run(t, h.agent, agentenkit.RunInput{Prompt: "again", ThreadID: ran.ThreadID})
	h.handleNext(t)
	next := h.seenAt(1)
	mustEqual(t, next.Sandbox.ID(), "mem-2", "a fresh sandbox")
	mustEqual(t, next.Lost, false, "a new run's fresh sandbox is not a loss")
	mustStrings(t, h.sandboxes.Destroyed(), []string{"mem-1", "mem-2"}, "destroyed")
}

func TestSandboxRunScope_AParkGivesTheSandboxBackAndTheResumedRunIsToldItsFilesAreGone(t *testing.T) {
	h := scopeRuntime(t, scripted(
		touchStep("c1", "a"),
		step{calls: []call{{"c2", "wait", `{}`}}},
		touchStep("c3", "b"),
		step{text: "done"},
	), nil, runScope)
	ran := h.run(t, h.agent, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateWaitingForInput, "parked")
	mustStrings(t, h.sandboxes.Destroyed(), []string{"mem-1"}, "given back at the park")

	h.respond(t, ran.ThreadID, "c2")
	resumed := h.seenAt(1)
	mustEqual(t, resumed.Sandbox.ID(), "mem-2", "a fresh sandbox after the park")
	mustEqual(t, resumed.Lost, true, "the resumed run is told its files are gone")
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateCompleted, "completed")
	mustStrings(t, h.sandboxes.Destroyed(), []string{"mem-1", "mem-2"}, "destroyed")
}

func TestSandboxRunScope_SandboxKeepOnParkKeepsItThroughThePark(t *testing.T) {
	h := scopeRuntime(t, scripted(
		touchStep("c1", "a"),
		step{calls: []call{{"c2", "wait", `{}`}}},
		touchStep("c3", "b"),
		step{text: "done"},
	), nil, func(c *agentenkit.AgentConfig) {
		runScope(c)
		c.SandboxKeepOnPark = true
	})
	ran := h.run(t, h.agent, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	mustEqual(t, len(h.sandboxes.Destroyed()), 0, "kept while parked")
	h.respond(t, ran.ThreadID, "c2")
	mustEqual(t, h.seenAt(1).Sandbox.ID(), "mem-1", "the same sandbox after the park")
	mustEqual(t, h.seenAt(1).Lost, false, "nothing lost")
	mustStrings(t, h.sandboxes.Destroyed(), []string{"mem-1"}, "destroyed when the run ended")
}

func TestSandboxRunScope_ASandboxAnEarlierRunLeftIsEndedByTheNextRun(t *testing.T) {
	h := scopeRuntime(t, scripted(touchStep("c1", "a"), step{text: "done"}, touchStep("c2", "b"), step{text: "done"}), nil, runScope)
	ran := h.run(t, h.agent, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	// As a crash would leave it: a live sandbox, recorded for another run.
	s, err := h.sandboxes.Create(h.ctx, ports.CreateSandboxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]any{"id": s.ID(), "provider": h.sandboxes.Name(), "createdAt": time.Now().UnixMilli(), "lastUsedAt": time.Now().UnixMilli(), "runId": "old-run"})
	_, _ = h.kv.Set(h.ctx, core.SandboxKey(ran.ThreadID), string(raw), ports.SetOptions{})

	h.run(t, h.agent, agentenkit.RunInput{Prompt: "again", ThreadID: ran.ThreadID})
	h.handleNext(t)
	mustEqual(t, h.seenAt(1).Lost, false, "not a loss")
	mustStrings(t, h.sandboxes.Destroyed(), []string{"mem-1", s.ID(), "mem-3"}, "the left one ended, then this run's")
}

func TestSandboxRunScope_TheThreadScopeIsTheDefaultAndKeepsTheSandbox(t *testing.T) {
	h := scopeRuntime(t, scripted(touchStep("c1", "a"), step{text: "done"}), nil, nil)
	ran := h.run(t, h.agent, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	mustEqual(t, len(h.sandboxes.Destroyed()), 0, "kept between messages")
	if h.kvGet(core.SandboxKey(ran.ThreadID)) == "" {
		t.Fatal("record dropped")
	}
}

func TestSandboxRunScope_AProviderThatRestoresTheWorkFolderMarksTheNewSandboxRestored(t *testing.T) {
	var h *scopeHarness
	h = scopeRuntime(t, scripted(touchStep("c1", "a"), step{text: "done"}, touchStep("c2", "b"), step{text: "done"}), func(o *agentenkit.RuntimeOptions) {
		o.Tools.Sandbox = restoringSandbox{o.Tools.Sandbox.(*memory.Sandbox)}
	}, nil)
	ran := h.run(t, h.agent, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	h.sandboxes.End("mem-1")
	core.ForgetSandboxHandles()
	h.run(t, h.agent, agentenkit.RunInput{Prompt: "again", ThreadID: ran.ThreadID})
	h.handleNext(t)
	mustEqual(t, h.seenAt(1).Lost, true, "the old one is gone")
	mustEqual(t, h.seenAt(1).Restored, true, "and the provider restores the work folder")
}

func TestSandboxRunScope_TheAppsCreateOptionsReachEverySandbox(t *testing.T) {
	h := scopeRuntime(t, scripted(touchStep("c1", "a"), step{text: "done"}), nil, func(c *agentenkit.AgentConfig) {
		c.SandboxDefaults = ports.CreateSandboxOptions{
			Network:  &ports.SandboxNetwork{Mode: "all"},
			Envs:     map[string]string{"CI": "1"},
			Template: "node",
			Timeout:  time.Second, // the runtime's own wins
		}
	})
	ran := h.run(t, h.agent, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	created := h.sandboxes.Created()
	mustEqual(t, len(created), 1, "made")
	mustEqual(t, created[0].Network.Mode, "all", "network")
	mustEqual(t, created[0].Envs["CI"], "1", "envs")
	mustEqual(t, created[0].Template, "node", "template")
	mustEqual(t, created[0].Timeout, sandboxIdle+time.Minute, "timeout stays the runtime's")
	mustEqual(t, created[0].Metadata.ThreadID, ran.ThreadID, "metadata stays the runtime's")
}

func TestSandboxRunScope_AfterSandboxCallRunsOnceAfterEachCallThatUsedTheSandbox(t *testing.T) {
	var h *scopeHarness
	h = scopeRuntime(t, scripted(
		step{calls: []call{{"c1", "touch", `{"name":"a"}`}, {"c2", "plain", `{}`}}},
		step{text: "done"},
	), func(o *agentenkit.RuntimeOptions) {
		o.Tools.AfterSandboxCall = func(_ context.Context, c ports.SandboxCall) error {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.calls = append(h.calls, c)
			return nil
		}
	}, nil)
	ran := h.run(t, h.agent, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	mustEqual(t, len(h.calls), 1, "only the call that used the sandbox")
	c := h.calls[0]
	mustEqual(t, c.ToolName, "touch", "tool")
	mustEqual(t, c.ToolCallID, "c1", "call")
	mustEqual(t, c.ThreadID, ran.ThreadID, "thread")
	mustEqual(t, c.Sandbox.ID(), "mem-1", "sandbox")
	if c.Err != nil {
		t.Fatalf("err %v", c.Err)
	}
}

func TestSandboxRunScope_AnErrorFromAfterSandboxCallReachesTheModelAsTheCallsError(t *testing.T) {
	h := scopeRuntime(t, scripted(touchStep("c1", "a"), step{text: "done"}), func(o *agentenkit.RuntimeOptions) {
		o.Tools.AfterSandboxCall = func(context.Context, ports.SandboxCall) error {
			return errors.New("your change was not saved: disk full")
		}
	}, nil)
	ran := h.run(t, h.agent, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	var result string
	for _, m := range h.storage.MessageRows(ran.ThreadID) {
		for _, p := range agentenkit.ParseContent(m.Content) {
			if p.Type == "tool-result" && p.ToolCallID == "c1" {
				result = string(p.Result)
			}
		}
	}
	if !strings.Contains(result, "your change was not saved: disk full") {
		t.Fatalf("the model was not told: %s", result)
	}
}
