package core

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/zendev-sh/goai"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

func timedTool(timeout time.Duration, exec func(ctx context.Context) (string, error)) ports.Tool {
	return ports.Tool{Tool: goai.Tool{Name: "slow", Execute: func(ctx context.Context, _ json.RawMessage) (string, error) {
		return exec(ctx)
	}}, Timeout: timeout}
}

// A timed call that returns close to its own limit leaves the step
// StepTimeout to finish, not the moment its own limit ends.
func TestStepLimit_AToolThatReturnsInTimeLeavesTheStepTimeToFinish(t *testing.T) {
	slow := timedTool(300*time.Millisecond, func(context.Context) (string, error) {
		time.Sleep(150 * time.Millisecond)
		return "ok", nil
	})
	ctx, limit := startStepLimit(context.Background(), 290*time.Millisecond, []ports.Tool{slow})
	if _, err := withStepLimit([]ports.Tool{slow}, limit)[0].Execute(ctx, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(160 * time.Millisecond) // past the tool's own end, within StepTimeout of its return
	if expired, _ := limit.stop(); expired {
		t.Fatal("the step expired after the tool kept to its limit")
	}
}

// A timed call that runs out of time fails the step by the time it hands
// back its result, even when it never asks its context why it ended.
func TestStepLimit_ACallPastItsLimitFailsTheStepBeforeItReturns(t *testing.T) {
	for range 20 {
		slow := timedTool(10*time.Millisecond, func(ctx context.Context) (string, error) {
			<-ctx.Done()
			return "gave up", nil
		})
		ctx, limit := startStepLimit(context.Background(), 0, []ports.Tool{slow})
		_, _ = withStepLimit([]ports.Tool{slow}, limit)[0].Execute(ctx, nil)
		expired, ran := limit.stop()
		if !expired || ran != 10*time.Millisecond {
			t.Fatalf("expired %v after %s", expired, ran)
		}
	}
}

// Once a long timed call returns, the step is back to StepTimeout, and the
// error names that limit, not the tool's.
func TestStepLimit_AfterATimedCallTheStepIsBackToStepTimeout(t *testing.T) {
	quick := timedTool(time.Minute, func(context.Context) (string, error) { return "ok", nil })
	ctx, limit := startStepLimit(context.Background(), 100*time.Millisecond, []ports.Tool{quick})
	_, _ = withStepLimit([]ports.Tool{quick}, limit)[0].Execute(ctx, nil)
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the step kept the tool's minute")
	}
	expired, ran := limit.stop()
	if !expired || ran != 100*time.Millisecond {
		t.Fatalf("expired %v after %s", expired, ran)
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
