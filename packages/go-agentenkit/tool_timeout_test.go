package agentenkit_test

import (
	"context"
	"strings"
	"testing"
	"time"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
)

// sleepTool waits for d, or until its context ends.
func sleepTool(name string, d time.Duration) agentenkit.Tool {
	return tool(name, func(ctx context.Context, _ map[string]any) (string, error) {
		select {
		case <-time.After(d):
			return "slept", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})
}

// A tool with its own Timeout gets that long, even past StepTimeout.
func TestToolTimeout_ASlowToolWithALongerLimitFinishes(t *testing.T) {
	h := makeRuntime(t, scripted(step{calls: []call{{"c1", "render", `{}`}}}, step{text: "done"}),
		func(c *agentenkit.AgentConfig) { c.StepTimeout = 50 * time.Millisecond; c.RunRetryBackoff = 0 })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name:  "chat",
		Tools: []agentenkit.Tool{agentenkit.WithToolTimeout(sleepTool("render", 200*time.Millisecond), 2*time.Second)},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "the slow tool finished in its own time")
	mustEqual(t, h.queue.Len(), 0, "no retry")
	mustEqual(t, h.model.Calls(), 2, "two model calls")
}

// A tool with no Timeout still has StepTimeout, even beside one that has.
func TestToolTimeout_AToolWithoutOneKeepsTheStepLimit(t *testing.T) {
	h := makeRuntime(t, scripted(step{calls: []call{{"c1", "lookup", `{}`}}}, step{text: "done"}),
		func(c *agentenkit.AgentConfig) { c.StepTimeout = 50 * time.Millisecond; c.RunRetryBackoff = 0 })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat",
		Tools: []agentenkit.Tool{
			agentenkit.WithToolTimeout(sleepTool("render", time.Millisecond), 2*time.Second),
			sleepTool("lookup", 2*time.Second),
		},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	started := time.Now()
	h.handleNext(t)
	if time.Since(started) > time.Second {
		t.Fatal("the step must end at StepTimeout")
	}
	mustEqual(t, h.queue.Len(), 1, "the timed-out step put a retry on the queue")
	mustEqual(t, h.queue.Items()[0].Kind, agentenkit.JobRetry, "kind")
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateQueued, "waiting to retry")
}

// A call past its own Timeout ends the step like a step past StepTimeout:
// the same error, and the same retry.
func TestToolTimeout_AToolPastItsOwnLimitFailsLikeAStepTimeout(t *testing.T) {
	h := makeRuntime(t, scripted(
		step{calls: []call{{"c1", "render", `{}`}}},
		step{calls: []call{{"c2", "render", `{}`}}},
	), func(c *agentenkit.AgentConfig) {
		c.StepTimeout = 2 * time.Second
		c.RunRetryBackoff = 0
		c.RunMaxAttempts = 2
	})
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name:  "chat",
		Tools: []agentenkit.Tool{agentenkit.WithToolTimeout(sleepTool("render", 5*time.Second), 50*time.Millisecond)},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	started := time.Now()
	h.handleNext(t)
	if time.Since(started) > time.Second {
		t.Fatal("the step must end at the tool's own limit")
	}
	mustEqual(t, h.queue.Len(), 1, "the timed-out step put a retry on the queue")
	mustEqual(t, h.queue.Items()[0].Kind, agentenkit.JobRetry, "kind")
	h.handleNext(t)
	term := h.lastTerminal(ran.ThreadID)
	mustEqual(t, term["state"], "FAILED", "out of attempts")
	if msg, _ := term["error"].(string); !strings.Contains(msg, "step 1 ran longer than 50ms") {
		t.Fatalf("error: %q", msg)
	}
}

// A nested run happens inside one spawnSubagent call, so inside one parent
// step: SubagentsConfig.Timeout gives it longer than StepTimeout, and a
// child's own timed tool runs within that.
func TestToolTimeout_ASubagentWithItsOwnLimitOutlastsTheParentStep(t *testing.T) {
	h := makeRuntime(t, scripted(
		step{calls: []call{{"s1", "spawnSubagent", spawnArgs}}},
		step{calls: []call{{"c1", "render", `{}`}}}, // the child
		step{text: "child done"},
		step{text: "parent done"},
	), func(c *agentenkit.AgentConfig) { c.StepTimeout = 50 * time.Millisecond; c.RunRetryBackoff = 0 })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Model: "gpt-4o",
		Subagents: &agentenkit.SubagentsConfig{
			Timeout: 2 * time.Second,
			Tools:   []agentenkit.Tool{agentenkit.WithToolTimeout(sleepTool("render", 200*time.Millisecond), time.Second)},
		},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "the child had its time")
	mustEqual(t, h.queue.Len(), 0, "no retry")
}
