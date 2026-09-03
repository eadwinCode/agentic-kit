package agentenkit_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
)

// A tool that starts work it cannot wait for parks ITSELF (§2.5): the run
// lock and the worker are released, the request is durable with the tool's
// own reason and deadline, and whoever finishes the work resumes the same
// call through Respond with a payload the tool then returns from.
func TestToolPark_StartsWorkParksAndResumesWithThePayload(t *testing.T) {
	model := scripted(
		step{calls: []call{{"c1", "render", `{"scene":"intro"}`}}},
		step{text: "rendered"},
	)
	h := makeRuntime(t, model, func(c *agentenkit.AgentConfig) { c.HITLTTL = time.Hour })
	var started []string
	var resumedWith []string
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Model: "gpt-4o",
		Tools: []agentenkit.Tool{tool("render", func(ctx context.Context, args map[string]any) (string, error) {
			if a := agentenkit.ApprovalFromContext(ctx); a != nil {
				// Second call: the job finished; its outcome rode the payload.
				resumedWith = append(resumedWith, string(a.Payload))
				return `{"url":"https://cdn/x.mp4"}`, nil
			}
			started = append(started, args["scene"].(string))
			return "", agentenkit.ParkForInput(agentenkit.ParkRequest{
				Reason: "job", Payload: map[string]any{"jobId": "job-1"}, TTL: 30 * time.Minute,
			})
		})},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "render the intro"})
	h.handleNext(t)

	// The work started, then the run parked without holding anything.
	mustStrings(t, started, []string{"intro"}, "the tool ran once and started the job")
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateWaitingForInput, "parked")
	mustEqual(t, h.kvGet(agentenkit.RunLockKey(ran.ThreadID)), "", "no lock held while parked")
	req := h.events(ran.ThreadID, "INPUT_REQUIRED")
	mustEqual(t, len(req), 1, "one request")
	p := payload(req[0])
	mustEqual(t, p["reason"], "job", "the tool's own reason, so a UI can tell it from an approval")
	mustEqual(t, p["toolName"], "render", "tool")
	mustEqual(t, p["arguments"].(map[string]any)["jobId"], "job-1", "the payload is the request's arguments")
	expires, _ := time.Parse(time.RFC3339Nano, p["expiresAt"].(string))
	if until := time.Until(expires); until < 25*time.Minute || until > 31*time.Minute {
		t.Fatalf("per-park TTL not honoured: expires in %s", until)
	}
	// The expiry job is timed to the park's own TTL, not the config's.
	delays := h.queue.Delays()
	if len(delays) != 1 || delays[0] < 30*time.Minute || delays[0] > 31*time.Minute {
		t.Fatalf("expiry delay = %v", delays)
	}
	h.queue.Shift() // leave the expiry aside; the job finishes first

	// The job completes: its owner responds with the outcome.
	res, err := h.rt.HITL.Respond(h.ctx, agentenkit.RespondInput{
		ThreadID: ran.ThreadID, ToolCallID: "c1", Approved: true,
		Payload: map[string]any{"status": "succeeded", "url": "https://cdn/x.mp4"},
	})
	if err != nil || !res.Delivered {
		t.Fatalf("respond: %v %+v", err, res)
	}
	h.drain(t)

	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateCompleted, "completed after the resume")
	mustEqual(t, len(resumedWith), 1, "the same call ran again with the payload")
	var got map[string]any
	_ = json.Unmarshal([]byte(resumedWith[0]), &got)
	mustEqual(t, got["status"], "succeeded", "payload reached the tool")
	// The conversation carries the real result, never the park sentinel.
	mustStrings(t, h.roles(ran.ThreadID), []string{"user", "assistant", "tool", "assistant"}, "roles")
}

// A self-park that nobody answers expires on its own deadline, like an
// approval, and the model is told so.
func TestToolPark_ExpiresOnItsOwnDeadline(t *testing.T) {
	model := scripted(
		step{calls: []call{{"c1", "render", `{}`}}},
		step{text: "gave up"},
	)
	h := makeRuntime(t, model, func(c *agentenkit.AgentConfig) {
		c.HITLTTL = time.Hour // the config's TTL is long; the park's own is short
		c.ReclaimGrace = 0
	})
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Model: "gpt-4o",
		Tools: []agentenkit.Tool{tool("render", func(ctx context.Context, _ map[string]any) (string, error) {
			if agentenkit.ApprovalFromContext(ctx) != nil {
				t.Fatal("an expired park must not run the tool again")
			}
			return "", agentenkit.ParkForInput(agentenkit.ParkRequest{Reason: "job", TTL: 20 * time.Millisecond})
		})},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "render"})
	h.handleNext(t)
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateWaitingForInput, "parked")

	time.Sleep(30 * time.Millisecond)
	h.drain(t) // the park's own expiry job
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateCompleted, "completed on expiry")
	mustEqual(t, len(h.events(ran.ThreadID, "INPUT_EXPIRED")), 1, "expired on the park's deadline, not the config's")
}
