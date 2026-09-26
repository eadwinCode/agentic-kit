//go:build !windows

package agentenkit_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zendev-sh/goai/provider"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/localsandbox"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/core"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/pricing"
)

// bash, code_execution and text_editor (spec T4). The same cases run in the
// TS package (test/sandbox-tools.test.ts), under the same names.

type toolCall struct {
	id, name string
	args     map[string]any
}

// stepsModel makes the calls of each step in turn (one step per model
// call, across runs), and answers "done" when the script runs out.
type stepsModel struct {
	mu    sync.Mutex
	steps [][]toolCall
	call  int
}

func (m *stepsModel) ModelID() string { return "mock" }

func (m *stepsModel) DoGenerate(context.Context, provider.GenerateParams) (*provider.GenerateResult, error) {
	return nil, fmt.Errorf("not used")
}

func (m *stepsModel) DoStream(context.Context, provider.GenerateParams) (*provider.StreamResult, error) {
	m.mu.Lock()
	var calls []toolCall
	if m.call < len(m.steps) {
		calls = m.steps[m.call]
	}
	m.call++
	m.mu.Unlock()
	ch := make(chan provider.StreamChunk, 16)
	go func() {
		defer close(ch)
		usage := provider.Usage{InputTokens: 1, OutputTokens: 1}
		if len(calls) == 0 {
			ch <- provider.StreamChunk{Type: provider.ChunkText, Text: "done"}
			ch <- provider.StreamChunk{Type: provider.ChunkFinish, FinishReason: provider.FinishStop, Usage: usage}
			return
		}
		for _, c := range calls {
			raw, _ := json.Marshal(c.args)
			ch <- provider.StreamChunk{Type: provider.ChunkToolCall, ToolCallID: c.id, ToolName: c.name, ToolInput: string(raw)}
		}
		ch <- provider.StreamChunk{Type: provider.ChunkFinish, FinishReason: provider.FinishToolCalls, Usage: usage}
	}()
	return &provider.StreamResult{Stream: ch}, nil
}

type toolsHarness struct {
	*harness
	agent *agentenkit.AgentHandle
}

func toolsRuntime(t *testing.T, steps [][]toolCall, names []string, opts agentenkit.BuiltinToolOptions, tune func(*agentenkit.RuntimeOptions), cfg ...func(*agentenkit.AgentConfig)) *toolsHarness {
	t.Helper()
	core.ForgetSandboxHandles()
	t.Cleanup(core.ForgetSandboxHandles)
	model := &stepsModel{steps: steps}
	h := makeRuntimeOpts(t, nil, func(o *agentenkit.RuntimeOptions) {
		o.Tools.Sandbox = localsandbox.New(localsandbox.Options{RootDir: t.TempDir()})
		o.ResolveModel = func(string) (agentenkit.ResolvedModel, error) {
			return agentenkit.ResolvedModel{Instance: func() provider.LanguageModel { return model }, ContextWindow: 128_000}, nil
		}
		if tune != nil {
			tune(o)
		}
	}, cfg...)
	tools, err := h.rt.BuiltinTools(names, opts)
	if err != nil {
		t.Fatal(err)
	}
	return &toolsHarness{harness: h, agent: h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "coder", Tools: tools})}
}

func (h *toolsHarness) runIn(t *testing.T, threadID string) string {
	t.Helper()
	res := h.run(t, h.agent, agentenkit.RunInput{Prompt: "go", ThreadID: threadID})
	h.handleNext(t)
	return res.ThreadID
}

// result is the tool call's result as JSON would read it, or nil.
func (h *toolsHarness) result(t *testing.T, threadID, id string) any {
	t.Helper()
	for _, m := range h.storage.MessageRows(threadID) {
		for _, p := range agentenkit.ParseContent(m.Content) {
			if p.Type == "tool-result" && p.ToolCallID == id {
				var out any
				if err := json.Unmarshal(p.Result, &out); err != nil {
					t.Fatalf("result of %s: %v (%s)", id, err, p.Result)
				}
				return out
			}
		}
	}
	return nil
}

func (h *toolsHarness) resultMap(t *testing.T, threadID, id string) map[string]any {
	t.Helper()
	m, _ := h.result(t, threadID, id).(map[string]any)
	return m
}

func bashCall(id, command string, extra ...map[string]any) toolCall {
	args := map[string]any{"command": command}
	for _, e := range extra {
		for k, v := range e {
			args[k] = v
		}
	}
	return toolCall{id: id, name: "bash", args: args}
}

func editCall(id string, args map[string]any) toolCall {
	return toolCall{id: id, name: "text_editor", args: args}
}

var noApproval = agentenkit.BuiltinToolOptions{
	Bash:       agentenkit.BashOptions{Approval: agentenkit.AskNever},
	TextEditor: agentenkit.TextEditorOptions{Approval: agentenkit.AskNever},
}

func TestSandboxTools_TheSandboxToolsNeedASandbox(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	for _, name := range []string{"bash", "code_execution", "text_editor"} {
		_, err := h.rt.BuiltinTools([]string{name}, agentenkit.BuiltinToolOptions{})
		if err == nil || err.Error() != "builtinTools: "+name+" needs RuntimeOptions.Tools.Sandbox" {
			t.Fatalf("%s: got %v", name, err)
		}
	}
}

func TestSandboxTools_BashAsksForApprovalByDefaultAndRunsOnceApproved(t *testing.T) {
	h := toolsRuntime(t, [][]toolCall{{bashCall("b1", "echo hi")}}, []string{"bash"}, agentenkit.BuiltinToolOptions{}, nil)
	threadID := h.runIn(t, "")
	mustEqual(t, h.thread(t, threadID).State, ports.StateWaitingForInput, "state")
	if _, err := h.rt.HITL.Respond(h.ctx, agentenkit.RespondInput{ThreadID: threadID, ToolCallID: "b1", Approved: true}); err != nil {
		t.Fatal(err)
	}
	h.handleNext(t)
	mustJSON(t, h.result(t, threadID, "b1"), map[string]any{"stdout": "hi\n", "stderr": "", "exitCode": 0}, "b1")
	mustEqual(t, h.thread(t, threadID).State, ports.StateCompleted, "state after")
}

func TestSandboxTools_ADeniedBashCommandDoesNotRun(t *testing.T) {
	h := toolsRuntime(t, [][]toolCall{{bashCall("b1", "echo x > made.txt")}, {editCall("e1", map[string]any{"command": "view", "path": "made.txt"})}},
		[]string{"bash", "text_editor"}, agentenkit.BuiltinToolOptions{}, nil)
	threadID := h.runIn(t, "")
	if _, err := h.rt.HITL.Respond(h.ctx, agentenkit.RespondInput{ThreadID: threadID, ToolCallID: "b1", Approved: false}); err != nil {
		t.Fatal(err)
	}
	h.handleNext(t)
	mustJSON(t, h.result(t, threadID, "b1"), map[string]any{"denied": true}, "b1")
	mustJSON(t, h.result(t, threadID, "e1"), map[string]any{"error": "text_editor: made.txt does not exist"}, "e1")
}

func TestSandboxTools_AnApprovalRuleCanDecidePerCall(t *testing.T) {
	rm := regexp.MustCompile(`\brm\b`)
	h := toolsRuntime(t, [][]toolCall{{bashCall("b1", "echo safe")}, {bashCall("b2", "rm -rf data")}}, []string{"bash"},
		agentenkit.BuiltinToolOptions{Bash: agentenkit.BashOptions{Approval: func(_ context.Context, in map[string]any) (bool, error) {
			c, _ := in["command"].(string)
			return rm.MatchString(c), nil
		}}}, nil)
	threadID := h.runIn(t, "")
	mustJSON(t, h.result(t, threadID, "b1"), map[string]any{"stdout": "safe\n", "stderr": "", "exitCode": 0}, "b1")
	if h.result(t, threadID, "b2") != nil {
		t.Fatal("b2 ran without approval")
	}
	mustEqual(t, h.thread(t, threadID).State, ports.StateWaitingForInput, "state")
}

func TestSandboxTools_BashKeepsTheWorkingFolderBetweenCommands(t *testing.T) {
	h := toolsRuntime(t, [][]toolCall{
		{bashCall("b1", "mkdir -p sub && cd sub")},
		{bashCall("b2", `basename "$PWD"`)},
		{bashCall("b3", `basename "$PWD"`, map[string]any{"restart": true})},
	}, []string{"bash"}, noApproval, nil)
	threadID := h.runIn(t, "")
	mustEqual(t, h.resultMap(t, threadID, "b2")["stdout"], "sub\n", "b2")
	mustEqual(t, h.resultMap(t, threadID, "b3")["stdout"], "work\n", "b3")
}

func TestSandboxTools_BashOutputIsCutInTheMiddlePastTheResultCap(t *testing.T) {
	h := toolsRuntime(t, [][]toolCall{{bashCall("b1", `printf 'start'; head -c 500 /dev/zero | tr '\0' x; printf 'end'`)}}, []string{"bash"}, noApproval, nil,
		func(c *agentenkit.AgentConfig) { c.BuiltinToolResultCapChars = 100 })
	threadID := h.runIn(t, "")
	r := h.resultMap(t, threadID, "b1")
	mustEqual(t, r["truncated"], true, "truncated")
	out := r["stdout"].(string)
	mustEqual(t, strings.HasPrefix(out, "start") && strings.HasSuffix(out, "end"), true, "start and end kept")
	mustEqual(t, strings.Contains(out, "\n… 408 characters cut …\n"), true, "cut line: "+out)
}

func TestSandboxTools_BashReportsACommandStoppedAtItsTimeLimit(t *testing.T) {
	h := toolsRuntime(t, [][]toolCall{{bashCall("b1", "echo started; sleep 10")}}, []string{"bash"},
		agentenkit.BuiltinToolOptions{Bash: agentenkit.BashOptions{Approval: agentenkit.AskNever, Timeout: time.Second}}, nil)
	threadID := h.runIn(t, "")
	mustJSON(t, h.result(t, threadID, "b1"), map[string]any{
		"stdout": "started\n", "stderr": "", "exitCode": 124, "timedOut": true,
		"error": "bash: the command was stopped after 1 s",
	}, "b1")
}

func TestSandboxTools_BashOutputGoesOutLiveAsToolOutputEvents(t *testing.T) {
	h := toolsRuntime(t, [][]toolCall{{bashCall("b1", "echo hello; echo oops >&2")}}, []string{"bash"}, noApproval, nil)
	threadID := h.runIn(t, "")
	// Live only: on the run's stream as CUSTOM, never in the thread record.
	var got []string
	for _, c := range customOf(t, h.harness, threadID, "tool.output") {
		var v map[string]any
		_ = json.Unmarshal(c.Value, &v)
		got = append(got, fmt.Sprintf("%v|%v|%v|%v", v["toolCallId"], v["tool"], v["stream"], v["text"]))
	}
	sort.Strings(got)
	mustStrings(t, got, []string{"b1|bash|stderr|oops\n", "b1|bash|stdout|hello\n"}, "tool.output")
	mustEqual(t, len(h.events(threadID, "tool.output")), 0, "not in the record")
}

func TestSandboxTools_CodeExecutionRunsPythonWithoutApprovalAndListsTheFilesItMade(t *testing.T) {
	code := "open('chart.png', 'wb').write(b'png')\nopen('data/out.csv', 'w').write('a,b')\nprint(1 + 1)"
	h := toolsRuntime(t, [][]toolCall{{bashCall("b0", "mkdir -p data")}, {{id: "c1", name: "code_execution", args: map[string]any{"code": code}}}},
		[]string{"bash", "code_execution"}, noApproval, nil)
	threadID := h.runIn(t, "")
	mustJSON(t, h.result(t, threadID, "c1"), map[string]any{
		"stdout": "2\n", "stderr": "", "exitCode": 0,
		"files": []any{map[string]any{"path": "chart.png", "mediaType": "image/png"}, map[string]any{"path": "data/out.csv", "mediaType": "text/csv"}},
	}, "c1")
}

func TestSandboxTools_CodeExecutionReportsAFailingProgram(t *testing.T) {
	h := toolsRuntime(t, [][]toolCall{{{id: "c1", name: "code_execution", args: map[string]any{"code": "raise ValueError('boom')"}}}},
		[]string{"code_execution"}, agentenkit.BuiltinToolOptions{}, nil)
	threadID := h.runIn(t, "")
	r := h.resultMap(t, threadID, "c1")
	mustEqual(t, r["exitCode"], float64(1), "exit code")
	mustEqual(t, r["error"], "code_execution: the program exited with code 1", "error")
	mustEqual(t, strings.Contains(r["stderr"].(string), "ValueError: boom"), true, "stderr")
	mustJSON(t, r["files"], []any{}, "files")
}

func TestSandboxTools_TextEditorViewsWithoutApprovalAndAsksBeforeAChange(t *testing.T) {
	h := toolsRuntime(t, [][]toolCall{
		{editCall("e1", map[string]any{"command": "view", "path": "."})},
		{editCall("e2", map[string]any{"command": "create", "path": "a.txt", "file_text": "x"})},
	}, []string{"text_editor"}, agentenkit.BuiltinToolOptions{}, nil)
	threadID := h.runIn(t, "")
	mustJSON(t, h.result(t, threadID, "e1"), map[string]any{"path": ".", "content": ""}, "e1")
	mustEqual(t, h.thread(t, threadID).State, ports.StateWaitingForInput, "state")
	if _, err := h.rt.HITL.Respond(h.ctx, agentenkit.RespondInput{ThreadID: threadID, ToolCallID: "e2", Approved: true}); err != nil {
		t.Fatal(err)
	}
	h.handleNext(t)
	mustJSON(t, h.result(t, threadID, "e2"), map[string]any{"ok": true, "message": "Created a.txt."}, "e2")
}

func TestSandboxTools_TextEditorCreatesReplacesInsertsAndUndoes(t *testing.T) {
	p := "notes/a.txt"
	h := toolsRuntime(t, [][]toolCall{
		{editCall("e1", map[string]any{"command": "create", "path": p, "file_text": "one\ntwo\nthree"})},
		{editCall("e2", map[string]any{"command": "str_replace", "path": p, "old_str": "two", "new_str": "TWO"})},
		{editCall("e3", map[string]any{"command": "insert", "path": p, "insert_line": 1, "new_str": "one and a half"})},
		{editCall("e4", map[string]any{"command": "view", "path": p})},
		{editCall("e5", map[string]any{"command": "undo_edit", "path": p})},
		{editCall("e6", map[string]any{"command": "view", "path": p, "view_range": []int{2, -1}})},
		{editCall("e7", map[string]any{"command": "view", "path": "."})},
	}, []string{"text_editor"}, noApproval, nil)
	threadID := h.runIn(t, "")
	mustJSON(t, h.result(t, threadID, "e1"), map[string]any{"ok": true, "message": "Created notes/a.txt."}, "e1")
	mustJSON(t, h.result(t, threadID, "e2"), map[string]any{"ok": true, "message": "Replaced 1 place in notes/a.txt."}, "e2")
	mustJSON(t, h.result(t, threadID, "e3"), map[string]any{"ok": true, "message": "Inserted after line 1 of notes/a.txt."}, "e3")
	mustJSON(t, h.result(t, threadID, "e4"), map[string]any{
		"path": p, "totalLines": 4,
		"content": "     1\tone\n     2\tone and a half\n     3\tTWO\n     4\tthree",
	}, "e4")
	mustJSON(t, h.result(t, threadID, "e5"), map[string]any{"ok": true, "message": "Undid the last change to notes/a.txt."}, "e5")
	mustJSON(t, h.result(t, threadID, "e6"), map[string]any{"path": p, "totalLines": 3, "content": "     2\tTWO\n     3\tthree"}, "e6")
	// Hidden entries (the tools' own .agentenkit folder) are left out.
	mustJSON(t, h.result(t, threadID, "e7"), map[string]any{"path": ".", "content": "notes/\nnotes/a.txt"}, "e7")
}

func TestSandboxTools_StrReplaceNeedsExactlyOneMatch(t *testing.T) {
	h := toolsRuntime(t, [][]toolCall{
		{editCall("e1", map[string]any{"command": "create", "path": "a.txt", "file_text": "x x"})},
		{editCall("e2", map[string]any{"command": "str_replace", "path": "a.txt", "old_str": "y", "new_str": "z"})},
		{editCall("e3", map[string]any{"command": "str_replace", "path": "a.txt", "old_str": "x", "new_str": "z"})},
	}, []string{"text_editor"}, noApproval, nil)
	threadID := h.runIn(t, "")
	mustJSON(t, h.result(t, threadID, "e2"), map[string]any{"error": "text_editor: old_str was not found in a.txt; it must match exactly, whitespace included"}, "e2")
	mustJSON(t, h.result(t, threadID, "e3"), map[string]any{"error": "text_editor: old_str matches 2 places in a.txt; include more of the text around it so it matches one"}, "e3")
}

func TestSandboxTools_EachSandboxToolCallBooksAUsageRowWithItsSeconds(t *testing.T) {
	h := toolsRuntime(t, [][]toolCall{{bashCall("b1", "sleep 0.2")}}, []string{"bash"}, noApproval, func(o *agentenkit.RuntimeOptions) {
		o.Pricer = pricing.Tools{"local": {PerSecond: 1}}
	})
	h.runIn(t, "")
	var rows []ports.NewUsage
	for _, r := range h.storage.UsageRows() {
		if r.Kind == ports.KindTool {
			rows = append(rows, r.NewUsage)
		}
	}
	mustEqual(t, len(rows), 1, "rows")
	mustEqual(t, rows[0].Model, "tool:bash", "model")
	mustEqual(t, rows[0].ModelID, "local", "adapter")
	secs, _ := rows[0].ProviderMetadata["seconds"].(float64)
	if secs < 0.2 {
		t.Fatalf("seconds %v", secs)
	}
	mustEqual(t, rows[0].Cost.Micros, int64(math.Round(secs*1_000_000)), "cost")
}

func TestSandboxTools_ACallOnAFreshSandboxAfterTheOldOneEndedSaysTheFilesAreGone(t *testing.T) {
	h := toolsRuntime(t, [][]toolCall{
		{editCall("e1", map[string]any{"command": "create", "path": "a.txt", "file_text": "x"})},
		{},
		{editCall("e2", map[string]any{"command": "view", "path": "a.txt"})},
	}, []string{"text_editor"}, noApproval, nil)
	threadID := h.runIn(t, "")
	var rec map[string]any
	_ = json.Unmarshal([]byte(h.kvGet(core.SandboxKey(threadID))), &rec)
	rec["lastUsedAt"] = 0
	raw, _ := json.Marshal(rec)
	_, _ = h.kv.Set(h.ctx, core.SandboxKey(threadID), string(raw), ports.SetOptions{})
	h.runIn(t, threadID)
	mustJSON(t, h.result(t, threadID, "e2"), map[string]any{
		"error": "text_editor: a.txt does not exist",
		"note":  "The sandbox this conversation used before has ended, so files from earlier are gone. This is a fresh one.",
	}, "e2")
}

func TestSandboxTools_TheSandboxToolsStopAtMaxUses(t *testing.T) {
	h := toolsRuntime(t, [][]toolCall{{bashCall("b1", "true")}, {bashCall("b2", "true")}}, []string{"bash"},
		agentenkit.BuiltinToolOptions{Bash: agentenkit.BashOptions{Approval: agentenkit.AskNever, MaxUses: 1}}, nil)
	threadID := h.runIn(t, "")
	mustEqual(t, h.resultMap(t, threadID, "b1")["exitCode"], float64(0), "b1")
	mustJSON(t, h.result(t, threadID, "b2"), map[string]any{"error": "bash: this run has used its 1 commands"}, "b2")
}

func TestSandboxTools_CutMiddleKeepsTheStartAndTheEndAndNeverSplitsACharacter(t *testing.T) {
	text, cut := core.CutMiddle("abcdefghij", 20)
	mustEqual(t, text+fmt.Sprint(cut), "abcdefghijfalse", "short")
	text, cut = core.CutMiddle("abcdefghij", 4)
	mustEqual(t, text+fmt.Sprint(cut), "ab\n… 6 characters cut …\nijtrue", "cut")
	// 😀 is two UTF-16 units; neither half is kept alone.
	text, _ = core.CutMiddle("a😀bcdef😀g", 4)
	mustEqual(t, text, "a\n… 9 characters cut …\ng", "pairs")
}

func TestSandboxTools_ToolPricesCanBePerSecondPerUseOrBoth(t *testing.T) {
	p := pricing.Tools{"e2b": {PerSecond: 0.0001}, "docker": {PerUse: 0.001, PerSecond: 0.0001}}
	price := func(adapter string, seconds float64) int64 {
		c, _ := p.Price(context.Background(), core.ToolUseRow("bash", adapter, 1, seconds))
		return c.Micros
	}
	mustEqual(t, price("e2b", 2.5), int64(250), "per second")
	mustEqual(t, price("docker", 2.5), int64(1_250), "per use and per second")
	mustEqual(t, price("e2b", 0), int64(0), "no time")
}
