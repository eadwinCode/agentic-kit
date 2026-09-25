package agentenkit_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zendev-sh/goai/provider"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/core"
)

// Workstream F: the subagent cap is per run and per depth, a wait for a slot
// ends on a stop, and a child that was stopped or cut short is never taken
// for finished. The same cases run in the TS package
// (test/subagent-slots.test.ts).

// routedModel answers by who is asking rather than by call order: nested
// runs call it in parallel, so their order is not fixed.
type routedModel struct {
	route func(brief string, answered bool) step
}

func (m *routedModel) ModelID() string { return "mock-routed" }

func (m *routedModel) pick(p provider.GenerateParams) step {
	brief, answered := "", false
	for _, msg := range p.Messages {
		for _, part := range msg.Content {
			if msg.Role == provider.RoleUser && brief == "" && part.Type == provider.PartText {
				brief = part.Text
			}
			if part.Type == provider.PartToolResult {
				answered = true
			}
		}
	}
	return m.route(brief, answered)
}

func (m *routedModel) DoGenerate(ctx context.Context, p provider.GenerateParams) (*provider.GenerateResult, error) {
	return scripted(m.pick(p)).DoGenerate(ctx, p)
}

func (m *routedModel) DoStream(ctx context.Context, p provider.GenerateParams) (*provider.StreamResult, error) {
	return scripted(m.pick(p)).DoStream(ctx, p)
}

func routedRuntime(t *testing.T, route func(brief string, answered bool) step, tune ...func(*agentenkit.AgentConfig)) (*harness, *agentenkit.AgentHandle) {
	model := &routedModel{route: route}
	h := makeRuntimeOpts(t, scripted(step{text: "unused"}), func(o *agentenkit.RuntimeOptions) {
		o.ResolveModel = func(string) (agentenkit.ResolvedModel, error) {
			return agentenkit.ResolvedModel{Instance: func() provider.LanguageModel { return model }, ContextWindow: 128_000}, nil
		}
	}, tune...)
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Model: "gpt-4o", Subagents: &agentenkit.SubagentsConfig{},
	})
	return h, chat
}

func spawn(id, instructions string) call {
	return call{id, "spawnSubagent", `{"name":"helper","instructions":"` + instructions + `"}`}
}

func TestSubagents_NestedSpawnsAtTheDefaultLimitDoNotDeadlock(t *testing.T) {
	h, chat := routedRuntime(t, func(brief string, answered bool) step {
		switch {
		case brief == "go" && !answered: // the parent fills every slot at depth 1
			return step{calls: []call{spawn("s1", "child"), spawn("s2", "child"), spawn("s3", "child")}}
		case brief == "go":
			return step{text: "parent done"}
		case brief == "child" && !answered:
			// Each child spawns one level deeper, after a moment, so all
			// three hold their slots before any grandchild asks for one.
			return step{delay: 50 * time.Millisecond, calls: []call{spawn("g", "grandchild")}}
		case brief == "child":
			return step{text: "child done"}
		default:
			return step{text: "grandchild done"}
		}
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	done := make(chan struct{})
	go func() { defer close(done); h.handleNext(t) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the run deadlocked on the subagent cap")
	}
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateCompleted, "completed")
	mustEqual(t, len(h.events(ran.ThreadID, "SUBAGENT_COMPLETED")), 6, "three children, three grandchildren")
}

func TestSubagents_SlotsArePerDepth(t *testing.T) {
	slots := core.NewRunSlots(1)
	release, err := slots.Acquire(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := slots.Acquire(ctx, 2); err != nil {
		t.Fatalf("a deeper level has slots of its own: %v", err)
	}
}

func TestSubagents_AWaitForASlotEndsWhenTheRunIsStopped(t *testing.T) {
	slots := core.NewRunSlots(1)
	release, _ := slots.Acquire(context.Background(), 1)
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	if _, err := slots.Acquire(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("the wait gives up on the stop: %v", err)
	}
}

func TestSubagents_AChildCutOffByAStopIsRecordedCancelled(t *testing.T) {
	var childStarted atomic.Bool
	h, chat := routedRuntime(t, func(brief string, answered bool) step {
		switch {
		case brief == "go" && !answered:
			return step{calls: []call{spawn("s1", "child")}}
		case brief == "go":
			return step{text: "parent done"}
		default:
			childStarted.Store(true)
			return step{text: "never", delay: 2 * time.Second}
		}
	}, func(c *agentenkit.AgentConfig) { c.StopPoll = 5 * time.Millisecond })
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	job, _ := h.queue.Shift()
	done := make(chan struct{})
	go func() { defer close(done); _, _ = h.rt.Worker.HandleJob(h.ctx, job) }()
	waitFor(t, childStarted.Load)
	if _, err := chat.Stop(h.ctx, ran.ThreadID, nil); err != nil {
		t.Fatal(err)
	}
	<-done
	mustEqual(t, len(h.events(ran.ThreadID, "SUBAGENT_COMPLETED")), 0, "never completed")
	childID := payload(h.events(ran.ThreadID, "SUBAGENT_STARTED")[0])["agentId"].(string)
	rec, _ := h.admin.Runs().Get(h.ctx, childID)
	mustEqual(t, rec.State, agentenkit.StateCancelled, "the child is recorded cancelled")
}

func TestSubagents_AChildWhoseStreamEndsWithoutAFinishFails(t *testing.T) {
	h, chat := routedRuntime(t, func(brief string, answered bool) step {
		switch {
		case brief == "go" && !answered:
			return step{calls: []call{spawn("s1", "child")}}
		case brief == "go":
			return step{text: "parent done"}
		default:
			return step{text: "half an answer", noFinish: true}
		}
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateCompleted, "the parent goes on")
	failed := h.events(ran.ThreadID, "SUBAGENT_FAILED")
	mustEqual(t, len(failed), 1, "the child failed")
	if !strings.Contains(payload(failed[0])["error"].(string), "without a finish") {
		t.Fatalf("with why: %v", payload(failed[0]))
	}
	mustEqual(t, len(h.events(ran.ThreadID, "SUBAGENT_COMPLETED")), 0, "its partial text is not a result")
}
