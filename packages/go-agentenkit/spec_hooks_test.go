package agentenkit_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zendev-sh/goai/provider"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
)

// noCaching keeps the system prompt in GenerateParams.System, where a test
// can read it; with caching on it is hoisted into the messages.
func noCaching(c *agentenkit.AgentConfig) { c.PromptCaching = false }

func TestSystemFn_BuildsThePromptPerStepFromTheRunState(t *testing.T) {
	h := makeRuntime(t, scripted(
		step{calls: []call{{"c1", "ping", `{}`}}},
		step{text: "done"},
	), noCaching)
	var built atomic.Int32
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", System: "static persona",
		SystemFn: func(_ context.Context, threadID string, state agentenkit.AgentRunState) (string, error) {
			n := built.Add(1)
			return fmt.Sprintf("org=%v thread=%s step=%d", state["orgId"], threadID, n), nil
		},
		Tools: []agentenkit.Tool{tool("ping", func(context.Context, map[string]any) (string, error) { return "pong", nil })},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi", State: agentenkit.AgentRunState{"orgId": "acme"}})
	h.handleNext(t)

	params := h.model.Params()
	mustEqual(t, len(params), 2, "model calls")
	mustEqual(t, params[0].System, "org=acme thread="+ran.ThreadID+" step=1", "first step prompt")
	mustEqual(t, params[1].System, "org=acme thread="+ran.ThreadID+" step=2", "second step prompt")
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "state")
}

func TestSystemFn_AnErrorFailsTheRun(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "never"}), func(c *agentenkit.AgentConfig) { c.RunMaxAttempts = 1 })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat",
		SystemFn: func(context.Context, string, agentenkit.AgentRunState) (string, error) {
			return "", errors.New("no project")
		},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	h.handleNext(t)
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateFailed, "state")
	mustEqual(t, h.model.Calls(), 0, "the model was never called")
}

func TestOnSettle_RunsBeforeTheTerminalStateIsWritten(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok", usage: &[2]int{7, 3}}))
	var seen agentenkit.RunFinishInfo
	var stateAtSettle agentenkit.ExecutionState
	var terminalsAtSettle int
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat",
		OnSettle: func(ctx context.Context, info agentenkit.RunFinishInfo) error {
			seen = info
			th, _ := h.storage.Threads().Get(ctx, info.ThreadID, agentenkit.StorageContext{})
			stateAtSettle = th.State
			for _, e := range h.events(info.ThreadID, "STATE_CHANGE") {
				if s := payload(e)["state"]; s == "COMPLETED" || s == "FAILED" || s == "CANCELLED" {
					terminalsAtSettle++
				}
			}
			return nil
		},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	h.handleNext(t)

	mustEqual(t, seen.RunID, ran.RunID, "run id")
	mustEqual(t, seen.State, agentenkit.StateCompleted, "state handed to settle")
	mustEqual(t, seen.TokensUsed, 10, "tokens")
	mustEqual(t, seen.Cancelled, false, "not cancelled")
	mustEqual(t, stateAtSettle, agentenkit.StateRunning, "durable state while settling")
	mustEqual(t, terminalsAtSettle, 0, "no terminal published yet")
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "terminal after settle")
}

func TestOnSettle_AnErrorFailsTheRunAndKeepsWhy(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	var finished agentenkit.RunFinishInfo
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name:     "chat",
		OnSettle: func(context.Context, agentenkit.RunFinishInfo) error { return errors.New("commit refused") },
		OnFinish: func(info agentenkit.RunFinishInfo) { finished = info },
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	h.handleNext(t)

	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateFailed, "durable state")
	term := h.lastTerminal(ran.ThreadID)
	mustEqual(t, term["state"], "FAILED", "published state")
	mustEqual(t, term["error"], "commit refused", "published reason")
	rec, _ := h.admin.Runs().Get(h.ctx, ran.RunID)
	mustEqual(t, rec.State, agentenkit.StateFailed, "run record")
	mustEqual(t, rec.Error, "commit refused", "recorded reason")
	mustEqual(t, finished.State, agentenkit.StateFailed, "OnFinish sees the failure")
	mustEqual(t, finished.Error, "commit refused", "OnFinish sees why")
	mustEqual(t, h.queue.Len(), 0, "a settle failure is not retried")
}

func TestOnSettle_SeesAStopAsCancelledOnACancelledContext(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "slow", delay: 500 * time.Millisecond}))
	var seen agentenkit.RunFinishInfo
	var ctxErr error
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat",
		OnSettle: func(ctx context.Context, info agentenkit.RunFinishInfo) error {
			seen, ctxErr = info, ctx.Err()
			return errors.New("ignored on a stop")
		},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	job, _ := h.queue.Shift()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = h.rt.Worker.HandleJob(h.ctx, job)
	}()
	time.Sleep(30 * time.Millisecond)
	if _, err := chat.Stop(h.ctx, ran.ThreadID, nil); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	mustEqual(t, seen.Cancelled, true, "cancelled")
	mustEqual(t, seen.State, agentenkit.StateCancelled, "state")
	if ctxErr == nil {
		t.Fatal("a stop must reach the settle hook on a cancelled context")
	}
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateCancelled, "a settle error cannot turn a stop into a failure")
}

func TestOnSettle_RunsWhenAttemptsAreExhausted(t *testing.T) {
	h := makeRuntime(t, scripted(step{err: errBoom}), func(c *agentenkit.AgentConfig) { c.RunMaxAttempts = 1 })
	var seen agentenkit.RunFinishInfo
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name:     "chat",
		OnSettle: func(_ context.Context, info agentenkit.RunFinishInfo) error { seen = info; return nil },
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	h.handleNext(t)
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateFailed, "state")
	mustEqual(t, seen.RunID, ran.RunID, "settle saw the run")
	mustEqual(t, seen.State, agentenkit.StateFailed, "as failed")
	mustEqual(t, seen.Error, "boom", "with the reason")
}

func TestRun_AcceptsACallerRunIDAndRefusesItsReuse(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "one", RunID: "run-abc"})
	mustEqual(t, ran.RunID, "run-abc", "result run id")
	mustEqual(t, h.queue.Items()[0].RunID, "run-abc", "job run id")
	mustEqual(t, h.kvGet(agentenkit.RunIDKey(ran.ThreadID)), "run-abc", "run id key")
	rec, _ := h.admin.Runs().Get(h.ctx, "run-abc")
	if rec == nil {
		t.Fatal("run record under the caller's id")
	}
	h.handleNext(t)
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "state")

	before := len(h.roles(ran.ThreadID))
	again, err := chat.Run(h.ctx, agentenkit.RunInput{ThreadID: ran.ThreadID, Prompt: "two", RunID: "run-abc"})
	if err != nil {
		t.Fatal(err)
	}
	if again.Accepted {
		t.Fatal("a reused run id must be refused")
	}
	mustEqual(t, again.Error, "Run id already used", "error")
	mustEqual(t, len(h.roles(ran.ThreadID)), before, "nothing was written")
	mustEqual(t, h.queue.Len(), 0, "nothing was queued")
}

func TestRun_MaxStepsCapsBelowTheConfigAndNeverAbove(t *testing.T) {
	looping := step{calls: []call{{"c1", "ping", `{}`}}}
	h := makeRuntime(t, scripted(looping, looping, looping, looping))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name:  "chat",
		Tools: []agentenkit.Tool{tool("ping", func(context.Context, map[string]any) (string, error) { return "pong", nil })},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "loop", MaxSteps: 2})
	mustEqual(t, h.queue.Items()[0].MaxSteps, 2, "job cap")
	h.handleNext(t)
	mustEqual(t, h.model.Calls(), 2, "two round trips")
	term := h.lastTerminal(ran.ThreadID)
	mustEqual(t, term["state"], "COMPLETED", "state")
	mustEqual(t, term["stopReason"], "max_steps", "stopReason")

	// Above the config it is clamped, and the config's ceiling holds.
	over := h.run(t, chat, agentenkit.RunInput{Prompt: "loop", MaxSteps: 1_000})
	mustEqual(t, h.queue.Items()[0].MaxSteps, agentenkit.DefaultConfig().MaxSteps, "clamped to the config")
	_ = over
	refused, _ := chat.Run(h.ctx, agentenkit.RunInput{Prompt: "loop", MaxSteps: -1})
	mustEqual(t, refused.Accepted, false, "a negative cap is refused")
}

func TestRun_AttachmentsBecomeImagePartsTheModelSees(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "a cat"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{
		Prompt:      "what is this?",
		Attachments: []agentenkit.Attachment{{URL: "https://cdn.example/cat.png", MediaType: "image/png"}},
	})
	stored := h.storage.MessageRows(ran.ThreadID)[0]
	parts := agentenkit.ParseContent(stored.Content)
	mustEqual(t, len(parts), 2, "text + image parts")
	mustEqual(t, parts[0].Type, "text", "text first")
	mustEqual(t, parts[1].Type, "image", "then the image")
	mustEqual(t, parts[1].Image, "https://cdn.example/cat.png", "image url")
	mustEqual(t, parts[1].MimeType, "image/png", "mime type")
	appended := payload(h.events(ran.ThreadID, "MESSAGE_APPENDED")[0])
	if !strings.Contains(fmt.Sprint(appended["content"]), "cat.png") {
		t.Fatalf("MESSAGE_APPENDED carries the image: %v", appended["content"])
	}

	h.handleNext(t)
	msgs := h.model.Params()[0].Messages
	var image *provider.Part
	for _, m := range msgs {
		for i := range m.Content {
			if m.Content[i].Type == provider.PartImage {
				image = &m.Content[i]
			}
		}
	}
	if image == nil {
		t.Fatal("the model never saw an image part")
	}
	mustEqual(t, image.URL, "https://cdn.example/cat.png", "provider url")
	mustEqual(t, image.MediaType, "image/png", "provider media type")
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "state")
}

func TestSubagents_AProfileGivesTheChildItsOwnPersonaToolsAndModel(t *testing.T) {
	h := makeRuntime(t, scripted(
		step{calls: []call{{"s1", "spawnSubagent", `{"name":"researcher","instructions":"find it"}`}}},
		step{calls: []call{{"l1", "lookup", `{"q":"x"}`}}}, // the child uses its own tool
		step{text: "child: found"},
		step{text: "parent: done"},
	), noCaching)
	var looked []string
	var persona atomic.Int32
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Model: "gpt-4o",
		Subagents: &agentenkit.SubagentsConfig{
			Profiles: map[string]agentenkit.SubagentProfile{
				"researcher": {
					Description: "finds facts", Model: "gpt-4o-mini", MaxSteps: 3,
					SystemFn: func(_ context.Context, _ string, state agentenkit.AgentRunState) (string, error) {
						persona.Add(1)
						return fmt.Sprintf("You are the researcher for %v.", state["orgId"]), nil
					},
					Tools: []agentenkit.Tool{tool("lookup", func(_ context.Context, args map[string]any) (string, error) {
						looked = append(looked, args["q"].(string))
						return `{"found":true}`, nil
					})},
				},
				"writer": {Description: "writes copy", System: "You write."},
			},
		},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go", State: agentenkit.AgentRunState{"orgId": "acme"}})
	h.handleNext(t)

	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "state")
	mustStrings(t, looked, []string{"x"}, "the profile's tool ran")
	params := h.model.Params()
	mustEqual(t, params[1].System, "You are the researcher for acme.", "the child's persona, from its SystemFn")
	mustEqual(t, params[2].System, "You are the researcher for acme.", "again on its second step")
	mustEqual(t, persona.Load(), int32(2), "built once per child step")
	// The model is told who it can delegate to
	var spawnDesc string
	for _, td := range params[0].Tools {
		if td.Name == "spawnSubagent" {
			spawnDesc = td.Description
		}
	}
	for _, want := range []string{"researcher (finds facts)", "writer (writes copy)"} {
		if !strings.Contains(spawnDesc, want) {
			t.Fatalf("spawn description %q lacks %q", spawnDesc, want)
		}
	}
	childID := payload(h.events(ran.ThreadID, "SUBAGENT_STARTED")[0])["agentId"].(string)
	rec, _ := h.admin.Runs().Get(h.ctx, childID)
	mustEqual(t, rec.Model, "gpt-4o-mini", "the profile's model")
	mustEqual(t, rec.Agent, "researcher", "the profile's name")
}

func TestSubagents_AnUnknownProfileIsReportedToTheModel(t *testing.T) {
	h := makeRuntime(t, scripted(
		step{calls: []call{{"s1", "spawnSubagent", `{"name":"nobody","instructions":"?"}`}}},
		step{text: "parent: ok then"},
	))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat",
		Subagents: &agentenkit.SubagentsConfig{Profiles: map[string]agentenkit.SubagentProfile{
			"researcher": {System: "You research."},
		}},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "state")
	mustEqual(t, len(h.events(ran.ThreadID, "SUBAGENT_STARTED")), 0, "nothing was spawned")
	parent, _ := h.storage.Messages().List(h.ctx, ran.ThreadID, agentenkit.MainAgent, agentenkit.StorageContext{})
	result := string(agentenkit.ParseContent(parent[2].Content)[0].Result)
	if !strings.Contains(result, `Unknown subagent \"nobody\"`) || !strings.Contains(result, "researcher") {
		t.Fatalf("tool result: %s", result)
	}
}

func TestSubagents_AProfileToolParksAndResumesWithTheProfilesTool(t *testing.T) {
	h := makeRuntime(t, scripted(
		step{calls: []call{{"s1", "spawnSubagent", `{"name":"mailer","instructions":"send it"}`}}},
		step{calls: []call{{"d1", "sendEmail", `{"to":"a@b.c"}`}}},
		step{text: "child: sent"},
		step{text: "parent: done"},
	))
	var sent []string
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat",
		Subagents: &agentenkit.SubagentsConfig{Profiles: map[string]agentenkit.SubagentProfile{
			"mailer": {System: "You mail.", Tools: []agentenkit.Tool{
				agentenkit.MarkRequiresConfirmation(tool("sendEmail", func(_ context.Context, args map[string]any) (string, error) {
					sent = append(sent, args["to"].(string))
					return `{"sent":true}`, nil
				})),
			}},
		}},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateWaitingForInput, "parked")
	mustEqual(t, len(sent), 0, "nothing sent yet")
	h.queue.Shift() // the park's expiry job
	if _, err := h.rt.HITL.Respond(h.ctx, agentenkit.RespondInput{ThreadID: ran.ThreadID, ToolCallID: "d1", Approved: true}); err != nil {
		t.Fatal(err)
	}
	h.handleNext(t)
	mustStrings(t, sent, []string{"a@b.c"}, "the profile's tool ran on approval")
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "state")
}

// ctxModel records the run id and state its calls were made under.
type ctxModel struct {
	*scriptedModel
	mu   sync.Mutex
	seen []string
}

func (m *ctxModel) note(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seen = append(m.seen, agentenkit.RunIDFromContext(ctx)+"|"+fmt.Sprint(agentenkit.RunStateFromContext(ctx)["orgId"]))
}

func (m *ctxModel) DoGenerate(ctx context.Context, p provider.GenerateParams) (*provider.GenerateResult, error) {
	m.note(ctx)
	return m.scriptedModel.DoGenerate(ctx, p)
}

func (m *ctxModel) DoStream(ctx context.Context, p provider.GenerateParams) (*provider.StreamResult, error) {
	m.note(ctx)
	return m.scriptedModel.DoStream(ctx, p)
}

func TestModelCalls_CarryTheRunIDAndStateOnTheirContext(t *testing.T) {
	inner := scripted(
		step{calls: []call{{"s1", "spawnSubagent", spawnArgs}}},
		step{text: "child"},
		step{text: "parent"},
	)
	h := makeRuntime(t, inner)
	model := &ctxModel{scriptedModel: inner}
	rt, err := agentenkit.SetupAgentCore(h.ctx, agentenkit.RuntimeOptions{
		Storage: h.storage, Admin: h.admin, Bus: h.bus, Kv: h.kv, Queue: h.queue,
		ResolveModel: func(string) (agentenkit.ResolvedModel, error) {
			return agentenkit.ResolvedModel{Instance: func() provider.LanguageModel { return model }, ContextWindow: 128_000}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	chat := rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Subagents: &agentenkit.SubagentsConfig{}})
	ran, err := chat.Run(h.ctx, agentenkit.RunInput{Prompt: "go", RunID: "run-x", State: agentenkit.AgentRunState{"orgId": "acme"}})
	if err != nil || !ran.Accepted {
		t.Fatalf("run: %v %+v", err, ran)
	}
	job, _ := h.queue.Shift()
	if _, err := rt.Worker.HandleJob(h.ctx, job); err != nil {
		t.Fatal(err)
	}
	childID := payload(h.events(ran.ThreadID, "SUBAGENT_STARTED")[0])["agentId"].(string)
	mustStrings(t, model.seen, []string{"run-x|acme", childID + "|acme", "run-x|acme"}, "run id and state per model call")
}

func TestPrepareStep_AddsEphemeralContextThatIsNeverPersisted(t *testing.T) {
	h := makeRuntime(t, scripted(
		step{calls: []call{{"c1", "ping", `{}`}}},
		step{text: "done"},
	))
	var calls atomic.Int32
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat",
		PrepareStep: func(_ context.Context, _ string, state agentenkit.AgentRunState, messages []provider.Message) ([]provider.Message, error) {
			calls.Add(1)
			extra := provider.Message{Role: provider.RoleUser, Content: []provider.Part{
				{Type: provider.PartImage, URL: "data:image/png;base64,AAAA", MediaType: "image/png"},
				{Type: provider.PartText, Text: "look at this once"},
			}}
			return append(append([]provider.Message{}, messages...), extra), nil
		},
		Tools: []agentenkit.Tool{tool("ping", func(context.Context, map[string]any) (string, error) { return "pong", nil })},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	h.handleNext(t)
	mustEqual(t, calls.Load(), int32(2), "once per step")
	for i, p := range h.model.Params() {
		last := p.Messages[len(p.Messages)-1]
		if last.Content[0].Type != provider.PartImage {
			t.Fatalf("step %d: the prepared image is not the last message: %+v", i, last)
		}
	}
	// Nothing ephemeral reached the durable history
	for _, m := range h.storage.MessageRows(ran.ThreadID) {
		if strings.Contains(string(m.Content), "look at this once") {
			t.Fatal("ephemeral context was persisted")
		}
	}
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "state")
}
