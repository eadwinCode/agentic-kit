package agentenkit_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/zendev-sh/goai"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
)

// A run that fails with an error retrying cannot fix is failed at once. The
// same cases run in the TS package (test/permanent-errors.test.ts).

func apiError(status int, body string) error {
	return &goai.APIError{Message: "provider answered " + strconv.Itoa(status), StatusCode: status, ResponseBody: body}
}

func failingRun(t *testing.T, err error) (*harness, agentenkit.RunResult) {
	t.Helper()
	h := makeRuntime(t, scripted(step{err: err}), func(c *agentenkit.AgentConfig) { c.RunRetryBackoff = 0 })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	h.handleNext(t)
	return h, ran
}

func TestPermanent_AnErrorThatRetryingCannotFixFailsTheRunAtOnce(t *testing.T) {
	for _, status := range []int{400, 401, 403, 404, 413, 422} {
		h, ran := failingRun(t, apiError(status, "{}"))
		mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateFailed, fmt.Sprint(status, " failed"))
		mustEqual(t, h.queue.Len(), 0, fmt.Sprint(status, " no retry"))
		mustEqual(t, h.model.Calls(), 1, fmt.Sprint(status, " one call"))
		if e, _ := h.lastTerminal(ran.ThreadID)["error"].(string); !strings.Contains(e, fmt.Sprint("provider answered ", status)) {
			t.Fatalf("%d: error %q", status, e)
		}
	}
}

func TestPermanent_NoCreditsFailsTheRunAtOnceThoughItComesAsA429(t *testing.T) {
	h, ran := failingRun(t, apiError(429, `{"error":{"code":"insufficient_quota","message":"You exceeded your current quota"}}`))
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateFailed, "failed")
	mustEqual(t, h.queue.Len(), 0, "no retry")
}

func TestPermanent_ARateLimitIsStillRetried(t *testing.T) {
	h, ran := failingRun(t, apiError(429, `{"error":{"code":"rate_limit_exceeded"}}`))
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateQueued, "waiting to retry")
	mustEqual(t, h.queue.Len(), 1, "the retry")
}

func TestPermanent_AServerErrorIsStillRetried(t *testing.T) {
	h, ran := failingRun(t, apiError(503, "{}"))
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateQueued, "waiting to retry")
	mustEqual(t, h.queue.Len(), 1, "the retry")
}

func TestPermanent_IsPermanentErrorLooksInsideTheErrorsThatWrapIt(t *testing.T) {
	mustEqual(t, agentenkit.IsPermanentError(fmt.Errorf("step 1: %w", apiError(401, "{}"))), true, "wrapped")
	mustEqual(t, agentenkit.IsPermanentError(fmt.Errorf("network down")), false, "plain error")
	mustEqual(t, agentenkit.IsPermanentError(nil), false, "nil")
}
