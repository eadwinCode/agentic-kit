package agentenkit_test

import (
	"bufio"
	"os"
	"testing"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// The TS runtime writes one event of every type to parity/stream-events.jsonl
// (test/stream-events.test.ts). Reading each line and writing it back must
// give the same bytes, so both runtimes put the same JSON on the wire.
func TestStreamEvents(t *testing.T) {
	t.Run("every event type has a line in the parity file", func(t *testing.T) {
		f, err := os.Open("../parity/stream-events.jsonl")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		seen := map[string]bool{}
		scan := bufio.NewScanner(f)
		for scan.Scan() {
			line := scan.Text()
			e, err := ports.DecodeStreamEvent([]byte(line))
			if err != nil {
				t.Fatalf("decode %s: %v", line, err)
			}
			seen[e.StreamEventType()] = true
			got, err := ports.EncodeStreamEvent(e)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != line {
				t.Errorf("round trip differs:\n TS: %s\n Go: %s", line, got)
			}
		}
		for _, typ := range []string{
			ports.EventRunStarted, ports.EventTextMessageStart, ports.EventTextMessageContent,
			ports.EventTextMessageEnd, ports.EventReasoningStart, ports.EventReasoningContent,
			ports.EventReasoningEnd, ports.EventToolCallStart, ports.EventToolCallArgs,
			ports.EventToolCallEnd, ports.EventToolCallResult, ports.EventSource,
			ports.EventStepFinished, ports.EventMessageAppended, ports.EventInputRequired,
			ports.EventSubagentStarted, ports.EventSubagentEvent, ports.EventSubagentFinished,
			ports.EventCustom, ports.EventRunFinished, ports.EventRunError,
		} {
			if !seen[typ] {
				t.Errorf("no line for %s", typ)
			}
		}
	})

	t.Run("a stream id names its run and segment", func(t *testing.T) {
		if got := ports.StreamIDOf("run_1", 2); got != "run_1:2" {
			t.Fatal(got)
		}
		if run, seg, ok := ports.ParseStreamID("run:with:colons:3"); !ok || run != "run:with:colons" || seg != 3 {
			t.Fatal(run, seg, ok)
		}
		for _, bad := range []string{"run_1", "run_1:0", ":1"} {
			if _, _, ok := ports.ParseStreamID(bad); ok {
				t.Errorf("%q parsed", bad)
			}
		}
	})
}
