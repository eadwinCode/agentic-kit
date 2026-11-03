package core

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/zendev-sh/goai"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// A timed tool that returns close to its own limit leaves the step
// StepTimeout to finish, not the moment its own limit ends.
func TestStepLimit_AToolThatReturnsInTimeLeavesTheStepTimeToFinish(t *testing.T) {
	slow := ports.Tool{Tool: goai.Tool{Name: "slow", Execute: func(context.Context, json.RawMessage) (string, error) {
		time.Sleep(90 * time.Millisecond)
		return "ok", nil
	}}, Timeout: 100 * time.Millisecond}
	ctx, limit := startStepLimit(context.Background(), 50*time.Millisecond, []ports.Tool{slow})
	tools := withStepLimit([]ports.Tool{slow}, limit)
	if _, err := tools[0].Execute(ctx, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond) // past the tool's own end, within StepTimeout of its return
	if expired, _ := limit.stop(); expired {
		t.Fatal("the step expired after the tool kept to its limit")
	}
}

// A step with no timed tool keeps a plain deadline context.
func TestStepLimit_AStepWithNoTimedToolKeepsADeadline(t *testing.T) {
	ctx, limit := startStepLimit(context.Background(), time.Minute, nil)
	defer limit.stop()
	if _, ok := ctx.Deadline(); !ok {
		t.Fatal("no deadline")
	}
}
