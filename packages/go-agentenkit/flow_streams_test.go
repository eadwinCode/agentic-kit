package agentenkit_test

import (
	"os"
	"testing"
	"time"

	flow "github.com/eadwinCode/flow-sdk"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/clevixflow"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/memory"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// The run streams suite against a real Flow engine: set TEST_FLOW_BASE_URL,
// TEST_FLOW_APP_ID and TEST_FLOW_SIGNING_KEY (an app's signing key).
func TestRunStreamsFlow(t *testing.T) {
	base, app, key := os.Getenv("TEST_FLOW_BASE_URL"), os.Getenv("TEST_FLOW_APP_ID"), os.Getenv("TEST_FLOW_SIGNING_KEY")
	if base == "" || app == "" || key == "" {
		t.Skip("TEST_FLOW_BASE_URL, TEST_FLOW_APP_ID or TEST_FLOW_SIGNING_KEY not set: skipping the flow case")
	}
	client, err := flow.NewClient(flow.ClientOpts{
		AppID: app, BaseURL: flow.StrPtr(base), SigningKey: flow.StrPtr(key), NoSyncOnBoot: true,
	})
	must(t, err)
	kv := memory.NewKv()
	open := func(t *testing.T) ports.RunStreams {
		s, err := clevixflow.NewStreams(client, clevixflow.StreamsOptions{Kv: kv})
		must(t, err)
		return s
	}
	runStreamsSuite(t, "flow", open, streamsSuiteOptions{other: open, settle: 500 * time.Millisecond})
}
