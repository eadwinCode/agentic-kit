package core

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf16"

	"github.com/zendev-sh/goai"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// bash, code_execution and text_editor (spec T4): the built-in tools that
// work in the thread's sandbox. The TS runtime has the same three
// (src/core/builtin/sandbox-tools.ts), with the same inputs, results and
// wording.

// ApprovalRule decides whether a call waits for a person to approve it
// first, from the call's input. An approved call runs when the approval
// comes back; a denied one tells the model it was denied. Nil keeps the
// tool's default.
type ApprovalRule func(ctx context.Context, input map[string]any) (bool, error)

// AskAlways makes every call wait for approval.
var AskAlways ApprovalRule = func(context.Context, map[string]any) (bool, error) { return true, nil }

// AskNever runs every call without approval.
var AskNever ApprovalRule = func(context.Context, map[string]any) (bool, error) { return false, nil }

// BashOptions are the app's settings for bash.
type BashOptions struct {
	// Timeout is how long one command may run. Default 120 s.
	Timeout time.Duration
	// MaxUses is the commands allowed in one run. 0: no limit.
	MaxUses int
	// Approval defaults to AskAlways: every command waits for approval.
	Approval ApprovalRule
}

// CodeExecutionOptions are the app's settings for code_execution.
type CodeExecutionOptions struct {
	// Timeout is how long one program may run. Default 60 s.
	Timeout time.Duration
	MaxUses int
	// Approval defaults to AskNever: it runs sealed in the sandbox.
	Approval ApprovalRule
}

// TextEditorOptions are the app's settings for text_editor.
type TextEditorOptions struct {
	MaxUses int
	// Approval defaults to asking for changes but not for view.
	Approval ApprovalRule
}

// stateDir is where the tools keep their own files, in the sandbox's work
// folder: the bash folder, the programs code_execution ran, the editor's
// undo history. Hidden, so a plain ls does not show it.
const stateDir = ".agentenkit"

// undoDepth is the changes the editor can take back, per file.
const undoDepth = 10

const lostNote = "The sandbox this conversation used before has ended, so files from earlier are gone. This is a fresh one."

// needsApproval asks the rule, unless this call is the approved one coming
// back.
func needsApproval(ctx context.Context, rule, def ApprovalRule, input map[string]any) (bool, error) {
	if ApprovalFromContext(ctx) != nil {
		return false, nil
	}
	if rule == nil {
		rule = def
	}
	return rule(ctx, input)
}

// CutMiddle keeps the start and the end of text over max units (counted as
// JavaScript counts a string's length, UTF-16), with a line saying how much
// was cut from the middle: the start says what ran, the end holds the
// error. A character is never split. The TS runtime cuts the same.
func CutMiddle(text string, max int) (string, bool) {
	u := utf16.Encode([]rune(text))
	if len(u) <= max {
		return text, false
	}
	head := max / 2
	tail := len(u) - (max - head)
	if head > 0 && utf16.IsSurrogate(rune(u[head-1])) && u[head-1] < 0xdc00 {
		head--
	}
	if tail < len(u) && u[tail] >= 0xdc00 && u[tail] <= 0xdfff {
		tail++
	}
	return string(utf16.Decode(u[:head])) + fmt.Sprintf("\n… %d characters cut …\n", tail-head) + string(utf16.Decode(u[tail:])), true
}

// liveOutput sends output as it comes, on the thread as live-only
// tool.output events, a few at a time so a chatty command does not flood
// the bus.
type liveOutput struct {
	ctx        context.Context
	run        ToolRun
	tool, call string
	mu         sync.Mutex
	parts      []livePart
	timer      *time.Timer
	send       sync.Mutex
}

type livePart struct{ stream, text string }

func newLiveOutput(ctx context.Context, run ToolRun, tool string) *liveOutput {
	return &liveOutput{ctx: context.WithoutCancel(ctx), run: run, tool: tool, call: goai.ToolCallIDFromContext(ctx)}
}

func (l *liveOutput) push(stream string) func(string) {
	return func(text string) {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.parts = append(l.parts, livePart{stream, text})
		if l.timer == nil {
			l.timer = time.AfterFunc(100*time.Millisecond, l.flush)
		}
	}
}

func (l *liveOutput) flush() {
	l.send.Lock()
	defer l.send.Unlock()
	l.mu.Lock()
	parts := l.parts
	l.parts, l.timer = nil, nil
	l.mu.Unlock()
	for _, stream := range []string{"stdout", "stderr"} {
		var b strings.Builder
		for _, p := range parts {
			if p.stream == stream {
				b.WriteString(p.text)
			}
		}
		if b.Len() == 0 {
			continue
		}
		payload := map[string]any{"toolCallId": nil, "tool": l.tool, "stream": stream, "text": b.String()}
		if l.call != "" {
			payload["toolCallId"] = l.call
		}
		if l.run.AgentID != "" {
			payload["agentId"] = l.run.AgentID
		}
		_, _ = PublishEvent(l.ctx, l.run.Deps, l.run.ThreadID, "tool.output", payload, PublishOptions{})
	}
}

func (l *liveOutput) done() {
	l.mu.Lock()
	if l.timer != nil {
		l.timer.Stop()
	}
	l.mu.Unlock()
	l.flush()
}

type commandResult struct {
	Stdout    string `json:"stdout"`
	Stderr    string `json:"stderr"`
	ExitCode  int    `json:"exitCode"`
	Truncated bool   `json:"truncated,omitempty"`
	TimedOut  bool   `json:"timedOut,omitempty"`
	Error     string `json:"error,omitempty"`
	Note      string `json:"note,omitempty"`
}

func capCommand(run ToolRun, r ports.CommandResult) commandResult {
	max := run.Deps.Config.BuiltinToolResultCapChars
	out, cutOut := CutMiddle(r.Stdout, max)
	errOut, cutErr := CutMiddle(r.Stderr, max)
	return commandResult{Stdout: out, Stderr: errOut, ExitCode: r.ExitCode, Truncated: r.Truncated || cutOut || cutErr}
}

func secondsOf(d time.Duration) string { return strconv.FormatFloat(d.Seconds(), 'f', -1, 64) }

func randomSuffix() string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	for i := range b {
		b[i] = alphabet[int(b[i])%len(alphabet)]
	}
	return string(b)
}

// --- bash ------------------------------------------------------------------

// bashScript keeps the folder a bash call ends in, in the sandbox, and the
// next call starts there: the "session" the model expects, without a shell
// that has to outlive the process.
func bashScript(command string, restart bool) string {
	lines := []string{`__ak="$PWD/` + stateDir + `"; mkdir -p "$__ak"`}
	if restart {
		lines = append(lines, `rm -f "$__ak/bash-cwd"`)
	}
	lines = append(lines,
		`if [ -f "$__ak/bash-cwd" ]; then cd -- "$(cat "$__ak/bash-cwd")" 2>/dev/null; fi`,
		`trap 'pwd > "$__ak/bash-cwd"' EXIT`,
		command,
		// Saved again here, for a command that set its own EXIT trap in
		// place of ours; the trap still covers one that calls exit.
		`__ec=$?`,
		`pwd > "$__ak/bash-cwd"`,
		`exit $__ec`)
	return strings.Join(lines, "\n")
}

// RunBash is the bash tool's work.
func RunBash(ctx context.Context, opts BashOptions, args map[string]any, run ToolRun) (string, error) {
	command, _ := args["command"].(string)
	restart, _ := args["restart"].(bool)
	if strings.TrimSpace(command) == "" && !restart {
		return failed("bash: command is required")
	}
	ask, err := needsApproval(ctx, opts.Approval, AskAlways, args)
	if err != nil {
		return "", err
	}
	if ask {
		return "", ParkForInput(ParkRequest{Payload: args})
	}
	if over, err := overLimit(ctx, run, "bash", opts.MaxUses); err != nil {
		return "", err
	} else if over {
		return failed(fmt.Sprintf("bash: this run has used its %d commands", opts.MaxUses))
	}
	limit := opts.Timeout
	if limit <= 0 {
		limit = 120 * time.Second
	}
	if command == "" {
		command = ":"
	}
	live := newLiveOutput(ctx, run, "bash")
	defer live.done()
	return WithThreadSandbox(ctx, run, func(ts ThreadSandbox) (string, error) {
		r, err := ts.Sandbox.RunCommand(ctx, bashScript(command, restart), ports.RunCommandOptions{
			Timeout: limit, OnStdout: live.push("stdout"), OnStderr: live.push("stderr"),
		})
		if err != nil {
			return "", err
		}
		RecordToolUsage(ctx, run, ToolUseRow("bash", ts.Sandbox.Provider(), 1, float64(r.DurationMs)/1000))
		res := capCommand(run, r)
		if r.TimedOut {
			res.TimedOut = true
			res.Error = "bash: the command was stopped after " + secondsOf(limit) + " s"
		}
		if ts.Lost {
			res.Note = lostNote
		}
		return resultJSON(res)
	})
}

// --- code_execution --------------------------------------------------------

type codeRunner struct{ ext, run string }

// Python writes no __pycache__, which would show up among the files the
// program made.
var codeRunners = map[string]codeRunner{"python": {"py", "PYTHONDONTWRITEBYTECODE=1 python3"}, "javascript": {"js", "node"}}

// MediaTypeOf is the media type of a file a program made, from its name.
func MediaTypeOf(path string) string {
	ext := strings.ToLower(path[strings.LastIndex(path, ".")+1:])
	if t, ok := map[string]string{
		"png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg", "gif": "image/gif", "webp": "image/webp",
		"svg": "image/svg+xml", "pdf": "application/pdf", "csv": "text/csv", "json": "application/json",
		"txt": "text/plain", "md": "text/markdown", "html": "text/html",
	}[ext]; ok {
		return t
	}
	return "application/octet-stream"
}

type madeFile struct {
	Path      string `json:"path"`
	MediaType string `json:"mediaType"`
}

// RunCodeExecution is the code_execution tool's work.
func RunCodeExecution(ctx context.Context, opts CodeExecutionOptions, args map[string]any, run ToolRun) (string, error) {
	code, _ := args["code"].(string)
	if strings.TrimSpace(code) == "" {
		return failed("code_execution: code is required")
	}
	language := "python"
	if l, ok := args["language"]; ok && l != nil {
		language = fmt.Sprint(l)
	}
	runner, ok := codeRunners[language]
	if !ok {
		quoted, _ := json.Marshal(language)
		return failed("code_execution: language must be python or javascript, not " + string(quoted))
	}
	ask, err := needsApproval(ctx, opts.Approval, AskNever, args)
	if err != nil {
		return "", err
	}
	if ask {
		return "", ParkForInput(ParkRequest{Payload: args})
	}
	if over, err := overLimit(ctx, run, "code_execution", opts.MaxUses); err != nil {
		return "", err
	} else if over {
		return failed(fmt.Sprintf("code_execution: this run has used its %d runs", opts.MaxUses))
	}
	limit := opts.Timeout
	if limit <= 0 {
		limit = 60 * time.Second
	}
	live := newLiveOutput(ctx, run, "code_execution")
	defer live.done()
	return WithThreadSandbox(ctx, run, func(ts ThreadSandbox) (string, error) {
		fs := ts.Sandbox.Filesystem()
		name := fmt.Sprintf("%d-%s", time.Now().UnixMilli(), randomSuffix())
		base := stateDir + "/code/" + name
		file := base + "." + runner.ext
		if err := fs.WriteFile(ctx, file, []byte(code)); err != nil {
			return "", err
		}
		// A marker made just before the run: the files newer than it are the
		// ones the program made or changed.
		// The program is read from stdin, so its imports (Python) and
		// relative requires (Node) resolve from the work folder, not the
		// hidden one.
		script := strings.Join([]string{
			`touch "` + base + `.start"`,
			runner.run + ` - < "` + file + `"`,
			`__ec=$?`,
			`find . -path "./` + stateDir + `" -prune -o -type f -newer "` + base + `.start" -print > "` + base + `.files" 2>/dev/null`,
			`exit $__ec`,
		}, "\n")
		r, err := ts.Sandbox.RunCommand(ctx, script, ports.RunCommandOptions{
			Timeout: limit, OnStdout: live.push("stdout"), OnStderr: live.push("stderr"),
		})
		if err != nil {
			return "", err
		}
		RecordToolUsage(ctx, run, ToolUseRow("code_execution", ts.Sandbox.Provider(), 1, float64(r.DurationMs)/1000))
		listed, _ := fs.ReadFile(ctx, base+".files")
		paths := []string{}
		for _, l := range strings.Split(listed, "\n") {
			if p := strings.TrimPrefix(l, "./"); p != "" {
				paths = append(paths, p)
			}
		}
		sort.Strings(paths)
		files := make([]madeFile, 0, len(paths))
		for _, p := range paths {
			files = append(files, madeFile{Path: p, MediaType: MediaTypeOf(p)})
		}
		c := capCommand(run, r)
		res := struct {
			Stdout    string     `json:"stdout"`
			Stderr    string     `json:"stderr"`
			ExitCode  int        `json:"exitCode"`
			Error     string     `json:"error,omitempty"`
			Files     []madeFile `json:"files"`
			Truncated bool       `json:"truncated,omitempty"`
			Note      string     `json:"note,omitempty"`
		}{Stdout: c.Stdout, Stderr: c.Stderr, ExitCode: r.ExitCode, Files: files, Truncated: c.Truncated}
		switch {
		case r.TimedOut:
			res.Error = "code_execution: the program was stopped after " + secondsOf(limit) + " s"
		case r.ExitCode != 0:
			res.Error = fmt.Sprintf("code_execution: the program exited with code %d", r.ExitCode)
		}
		if ts.Lost {
			res.Note = lostNote
		}
		return resultJSON(res)
	})
}

// --- text_editor -----------------------------------------------------------

var editCommands = map[string]bool{"create": true, "str_replace": true, "insert": true, "undo_edit": true}

// askForChanges is text_editor's default: changes wait, view does not.
var askForChanges ApprovalRule = func(_ context.Context, input map[string]any) (bool, error) {
	c, _ := input["command"].(string)
	return editCommands[c], nil
}

// historyPath is the editor's undo history for one file: earlier versions,
// newest last; null where the file did not exist yet.
func historyPath(path string) string {
	sum := sha256.Sum256([]byte(path))
	return stateDir + "/history/" + hex.EncodeToString(sum[:])[:32] + ".json"
}

func readHistory(ctx context.Context, s ports.Sandbox, path string) []*string {
	var h []*string
	raw, err := s.Filesystem().ReadFile(ctx, historyPath(path))
	if err != nil || json.Unmarshal([]byte(raw), &h) != nil {
		return []*string{}
	}
	return h
}

func writeHistory(ctx context.Context, s ports.Sandbox, path string, h []*string) error {
	raw, _ := json.Marshal(h)
	return s.Filesystem().WriteFile(ctx, historyPath(path), raw)
}

func rememberVersion(ctx context.Context, s ports.Sandbox, path string, before *string) error {
	h := append(readHistory(ctx, s, path), before)
	if len(h) > undoDepth {
		h = h[len(h)-undoDepth:]
	}
	return writeHistory(ctx, s, path, h)
}

func readOrNil(ctx context.Context, s ports.Sandbox, path string) (*string, error) {
	text, err := s.Filesystem().ReadFile(ctx, path)
	if errors.Is(err, ports.ErrSandboxFileNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &text, nil
}

// listFolder is a folder's contents two levels deep, hidden entries left
// out; ok is false when path is not a folder.
func listFolder(ctx context.Context, s ports.Sandbox, path string) (string, bool, error) {
	if _, err := s.Filesystem().ReadDir(ctx, path); err != nil {
		if errors.Is(err, ports.ErrSandboxFileNotFound) {
			return "", false, nil
		}
		return "", false, err
	}
	var out []string
	var walk func(dir, prefix string, depth int)
	walk = func(dir, prefix string, depth int) {
		entries, err := s.Filesystem().ReadDir(ctx, dir)
		if err != nil {
			return
		}
		for _, e := range entries {
			if strings.HasPrefix(e.Name, ".") {
				continue
			}
			line := prefix + e.Name
			if e.Type == "directory" {
				line += "/"
			}
			out = append(out, line)
			if e.Type == "directory" && depth < 2 {
				walk(dir+"/"+e.Name, prefix+e.Name+"/", depth+1)
			}
		}
	}
	walk(path, "", 1)
	return strings.Join(out, "\n"), true, nil
}

func numbered(lines []string, from int) string {
	out := make([]string, len(lines))
	for i, l := range lines {
		out[i] = fmt.Sprintf("%6d\t%s", from+i, l)
	}
	return strings.Join(out, "\n")
}

// wholeNumber reads a JSON number that must be a whole one.
func wholeNumber(v any) (int, bool) {
	f, ok := v.(float64)
	if !ok || f != math.Trunc(f) {
		return 0, false
	}
	return int(f), true
}

type editResult map[string]any

func editFailed(msg string) editResult { return editResult{"error": msg} }

func runEdit(ctx context.Context, s ports.Sandbox, run ToolRun, command, path string, args map[string]any) (editResult, error) {
	fs := s.Filesystem()
	max := run.Deps.Config.BuiltinToolResultCapChars
	switch command {
	case "view":
		// A folder first: reading one as a file fails differently on each
		// adapter, listing a file fails the same on all.
		listing, isDir, err := listFolder(ctx, s, path)
		if err != nil {
			return nil, err
		}
		if isDir {
			shown, cut := CutMiddle(listing, max)
			res := editResult{"path": path, "content": shown}
			if cut {
				res["truncated"] = true
			}
			return res, nil
		}
		text, err := readOrNil(ctx, s, path)
		if err != nil {
			return nil, err
		}
		if text == nil {
			return editFailed("text_editor: " + path + " does not exist"), nil
		}
		lines := strings.Split(*text, "\n")
		from, to := 1, -1
		if vr, ok := args["view_range"].([]any); ok && len(vr) == 2 {
			f, okF := wholeNumber(vr[0])
			t, okT := wholeNumber(vr[1])
			if !okF || !okT {
				return editFailed(fmt.Sprintf("text_editor: view_range must be two line numbers within 1 and %d", len(lines))), nil
			}
			from, to = f, t
		}
		if to == -1 || to > len(lines) {
			to = len(lines)
		}
		if from < 1 || from > max1(len(lines)) || to < from {
			return editFailed(fmt.Sprintf("text_editor: view_range must be two line numbers within 1 and %d", len(lines))), nil
		}
		shown, cut := CutMiddle(numbered(lines[from-1:to], from), max)
		res := editResult{"path": path, "content": shown, "totalLines": len(lines)}
		if cut {
			res["truncated"] = true
		}
		return res, nil
	case "create":
		fileText, ok := args["file_text"].(string)
		if !ok {
			return editFailed("text_editor: create needs file_text"), nil
		}
		before, err := readOrNil(ctx, s, path)
		if err != nil {
			return nil, err
		}
		if err := rememberVersion(ctx, s, path, before); err != nil {
			return nil, err
		}
		if err := fs.WriteFile(ctx, path, []byte(fileText)); err != nil {
			return nil, err
		}
		verb := "Replaced"
		if before == nil {
			verb = "Created"
		}
		return editResult{"ok": true, "message": verb + " " + path + "."}, nil
	case "str_replace":
		oldStr, _ := args["old_str"].(string)
		if oldStr == "" {
			return editFailed("text_editor: str_replace needs old_str"), nil
		}
		text, err := readOrNil(ctx, s, path)
		if err != nil {
			return nil, err
		}
		if text == nil {
			return editFailed("text_editor: " + path + " does not exist"), nil
		}
		switch n := strings.Count(*text, oldStr); {
		case n == 0:
			return editFailed("text_editor: old_str was not found in " + path + "; it must match exactly, whitespace included"), nil
		case n > 1:
			return editFailed(fmt.Sprintf("text_editor: old_str matches %d places in %s; include more of the text around it so it matches one", n, path)), nil
		}
		if err := rememberVersion(ctx, s, path, text); err != nil {
			return nil, err
		}
		newStr, _ := args["new_str"].(string)
		if err := fs.WriteFile(ctx, path, []byte(strings.Replace(*text, oldStr, newStr, 1))); err != nil {
			return nil, err
		}
		return editResult{"ok": true, "message": "Replaced 1 place in " + path + "."}, nil
	case "insert":
		newStr, ok := args["new_str"].(string)
		if !ok {
			return editFailed("text_editor: insert needs new_str"), nil
		}
		text, err := readOrNil(ctx, s, path)
		if err != nil {
			return nil, err
		}
		if text == nil {
			return editFailed("text_editor: " + path + " does not exist"), nil
		}
		lines := strings.Split(*text, "\n")
		at, ok := wholeNumber(args["insert_line"])
		if !ok || at < 0 || at > len(lines) {
			return editFailed(fmt.Sprintf("text_editor: insert_line must be within 0 and %d", len(lines))), nil
		}
		if err := rememberVersion(ctx, s, path, text); err != nil {
			return nil, err
		}
		merged := append(append(append([]string{}, lines[:at]...), strings.Split(newStr, "\n")...), lines[at:]...)
		if err := fs.WriteFile(ctx, path, []byte(strings.Join(merged, "\n"))); err != nil {
			return nil, err
		}
		return editResult{"ok": true, "message": fmt.Sprintf("Inserted after line %d of %s.", at, path)}, nil
	case "undo_edit":
		h := readHistory(ctx, s, path)
		if len(h) == 0 {
			return editFailed("text_editor: there is no change to " + path + " to undo"), nil
		}
		before := h[len(h)-1]
		h = h[:len(h)-1]
		if before == nil {
			if err := fs.Remove(ctx, path); err != nil {
				return nil, err
			}
		} else if err := fs.WriteFile(ctx, path, []byte(*before)); err != nil {
			return nil, err
		}
		if err := writeHistory(ctx, s, path, h); err != nil {
			return nil, err
		}
		if before == nil {
			return editResult{"ok": true, "message": "Removed " + path + ", which the change had created."}, nil
		}
		return editResult{"ok": true, "message": "Undid the last change to " + path + "."}, nil
	}
	return editFailed("text_editor: command must be view, create, str_replace, insert or undo_edit"), nil
}

func max1(n int) int {
	if n < 1 {
		return 1
	}
	return n
}

// RunTextEditor is the text_editor tool's work.
func RunTextEditor(ctx context.Context, opts TextEditorOptions, args map[string]any, run ToolRun) (string, error) {
	command, _ := args["command"].(string)
	if command != "view" && !editCommands[command] {
		return failed("text_editor: command must be view, create, str_replace, insert or undo_edit")
	}
	path, _ := args["path"].(string)
	if strings.TrimSpace(path) == "" {
		return failed("text_editor: path is required")
	}
	ask, err := needsApproval(ctx, opts.Approval, askForChanges, args)
	if err != nil {
		return "", err
	}
	if ask {
		return "", ParkForInput(ParkRequest{Payload: args})
	}
	if over, err := overLimit(ctx, run, "text_editor", opts.MaxUses); err != nil {
		return "", err
	} else if over {
		return failed(fmt.Sprintf("text_editor: this run has used its %d edits and views", opts.MaxUses))
	}
	return WithThreadSandbox(ctx, run, func(ts ThreadSandbox) (string, error) {
		res, err := runEdit(ctx, ts.Sandbox, run, command, path, args)
		if err != nil {
			return "", err
		}
		RecordToolUsage(ctx, run, ToolUseRow("text_editor", ts.Sandbox.Provider(), 1, 0))
		if ts.Lost {
			res["note"] = lostNote
		}
		return resultJSON(res)
	})
}
