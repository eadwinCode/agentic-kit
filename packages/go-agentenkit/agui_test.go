package agentenkit_test

import (
	"bufio"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/core"
)

// The AG-UI wire format (opt-in). The TS runtime writes each frame and what
// it becomes to parity/agui.jsonl (test/agui.test.ts); Go must turn each
// frame into the same events.
func TestAgUI(t *testing.T) {
	t.Run("every frame turns into the same AG-UI events in both runtimes", func(t *testing.T) {
		f, err := os.Open("../parity/agui.jsonl")
		must(t, err)
		defer f.Close()
		st := core.NewAgUIState("t1")
		scan := bufio.NewScanner(f)
		scan.Buffer(make([]byte, 1<<20), 1<<20)
		n := 0
		for scan.Scan() {
			var line struct {
				Frame  core.FollowFrame `json:"frame"`
				Events json.RawMessage  `json:"events"`
			}
			must(t, json.Unmarshal(scan.Bytes(), &line))
			got, err := core.ToAgUI(line.Frame, st)
			must(t, err)
			gotJSON, _ := json.Marshal(got)
			var a, b any
			_ = json.Unmarshal(gotJSON, &a)
			_ = json.Unmarshal(line.Events, &b)
			if !reflect.DeepEqual(a, b) {
				t.Errorf("frame %d:\n TS: %s\n Go: %s", n, line.Events, gotJSON)
			}
			n++
		}
		mustEqual(t, n > 10, true, "the file has the frames")
	})

	t.Run("the resume id rides on the first event of a frame", func(t *testing.T) {
		f, err := os.Open("../parity/agui.jsonl")
		must(t, err)
		defer f.Close()
		scan := bufio.NewScanner(f)
		scan.Buffer(make([]byte, 1<<20), 1<<20)
		var frames []core.FollowFrame
		for scan.Scan() {
			var line struct {
				Frame core.FollowFrame `json:"frame"`
			}
			must(t, json.Unmarshal(scan.Bytes(), &line))
			frames = append(frames, line.Frame)
		}
		at := core.ThreadCursor{Seq: 2}
		sse, err := core.AgUIFrameSSE(frames[11], &at, core.NewAgUIState("t1")) // a call whose arguments came whole
		must(t, err)
		messages := strings.Split(strings.TrimSuffix(sse, "\n\n"), "\n\n")
		mustEqual(t, len(messages), 2, "its arguments, then its end")
		if !strings.HasPrefix(messages[0], "id: 2 r1:1 12\ndata: {") || !strings.Contains(messages[0], `"TOOL_CALL_ARGS"`) {
			t.Fatalf("first: %q", messages[0])
		}
		if strings.Contains(messages[1], "id:") || !strings.Contains(messages[1], `"TOOL_CALL_END"`) {
			t.Fatalf("second: %q", messages[1])
		}
	})
}
