// Package sandboxsh holds what the sandbox adapters share. The TS adapters
// use the same scripts (src/adapters/sandbox-shell.ts), so a command
// behaves the same in both runtimes.
package sandboxsh

import (
	"crypto/rand"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// RunScript runs $1 in bash (sh where there is no bash), under timeout for
// $2 seconds when that is given and the sandbox has timeout. $3 is -l for
// a login shell. Called as sh -c RunScript sandbox <command> <secs> <-l>.
const RunScript = `if command -v bash >/dev/null 2>&1; then s=bash; else s=sh; fi; ` +
	`if [ -n "$2" ] && command -v timeout >/dev/null 2>&1; then exec timeout -k 2 "$2" "$s" $3 -c "$1"; fi; ` +
	`exec "$s" $3 -c "$1"`

// RunArgv is the argv that runs command through RunScript. Zero seconds
// means no timeout inside the sandbox.
func RunArgv(command string, timeoutSeconds int, login bool) []string {
	secs, l := "", ""
	if timeoutSeconds > 0 {
		secs = strconv.Itoa(timeoutSeconds)
	}
	if login {
		l = "-l"
	}
	return []string{"sh", "-c", RunScript, "sandbox", command, secs, l}
}

// BackgroundCommand starts command apart from the caller and returns at
// once.
func BackgroundCommand(command string) string {
	return "nohup sh -c " + ShellQuote(command) + " >/dev/null 2>&1 &"
}

// TimeoutSeconds is whole seconds for timeout(1), never less than one.
func TimeoutSeconds(d time.Duration) int {
	s := int((d + time.Second - 1) / time.Second)
	if s < 1 {
		return 1
	}
	return s
}

// LooksTimedOut: timeout stops a command with 124 (TERM) or 137 (KILL
// after -k).
func LooksTimedOut(exitCode int, elapsed, limit time.Duration) bool {
	return (exitCode == 124 || exitCode == 137) && elapsed >= limit-250*time.Millisecond
}

// ShellQuote quotes s for sh.
func ShellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// ResolveIn takes a relative path from the work folder.
func ResolveIn(workdir, p string) string {
	if strings.HasPrefix(p, "/") {
		return path.Clean(p)
	}
	return path.Join(workdir, p)
}

// NotFoundExit is the exit code the file scripts give for "no such file or
// folder".
const NotFoundExit = 44

// The sh -c <script> sandbox <path> scripts for adapters that reach files
// only through commands. Each exits 44 when the path is not there.
const (
	ReadScript  = `[ -f "$1" ] || exit 44; cat -- "$1"`
	WriteScript = `mkdir -p -- "$(dirname -- "$1")" && cat > "$1"`
	ListScript  = `[ -d "$1" ] || exit 44; cd -- "$1" || exit 44; ` +
		`for f in * .[!.]* ..?*; do [ -e "$f" ] || [ -L "$f" ] || continue; ` +
		`if [ -d "$f" ]; then printf 'd\t0\t%s\n' "$f"; ` +
		`else printf 'f\t%s\t%s\n' "$(wc -c < "$f" | tr -d ' ')" "$f"; fi; done`
	MkdirScript  = `mkdir -p -- "$1"`
	ExistsScript = `[ -e "$1" ] || [ -L "$1" ]`
	RemoveScript = `rm -rf -- "$1"`
)

// ParseListing reads what ListScript printed.
func ParseListing(out string) []ports.FileEntry {
	entries := []ports.FileEntry{}
	for _, line := range strings.Split(out, "\n") {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "\t", 3)
		if len(parts) < 3 {
			continue
		}
		if parts[0] == "d" {
			entries = append(entries, ports.FileEntry{Name: parts[2], Type: "directory"})
			continue
		}
		size, _ := strconv.ParseInt(parts[1], 10, 64)
		entries = append(entries, ports.FileEntry{Name: parts[2], Type: "file", Size: &size})
	}
	return SortEntries(entries)
}

// SortEntries sorts by name, as every adapter returns them.
func SortEntries(entries []ports.FileEntry) []ports.FileEntry {
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return entries
}

// Output collects one output stream: it keeps up to MaxCommandOutputBytes,
// hands each piece on as text as it comes, never splitting a character. It
// is an io.Writer, safe for one writer at a time.
type Output struct {
	mu        sync.Mutex
	onText    func(string)
	kept      []byte
	pending   []byte
	Truncated bool
}

// NewOutput hands text to onText (nil for none).
func NewOutput(onText func(string)) *Output { return &Output{onText: onText} }

// Write implements io.Writer; it never fails.
func (o *Output) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	n := len(p)
	if room := ports.MaxCommandOutputBytes - len(o.kept); len(p) > room {
		p = p[:max(0, room)]
		o.Truncated = true
	}
	if len(p) == 0 {
		return n, nil
	}
	o.kept = append(o.kept, p...)
	if o.onText != nil {
		buf := append(o.pending, p...)
		cut := completeUTF8(buf)
		if cut > 0 {
			o.onText(string(buf[:cut]))
		}
		o.pending = append([]byte(nil), buf[cut:]...)
	}
	return n, nil
}

// String is everything kept, with a character cut at the cap dropped.
func (o *Output) String() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.pending) > 0 && o.onText != nil && !o.Truncated {
		o.onText(strings.ToValidUTF8(string(o.pending), "�"))
		o.pending = nil
	}
	b := o.kept
	if o.Truncated {
		b = b[:completeUTF8(b)]
	}
	return strings.ToValidUTF8(string(b), "�")
}

// completeUTF8 is the length of b's longest prefix that does not end part
// way through a character.
func completeUTF8(b []byte) int {
	for i := len(b) - 1; i >= 0 && i >= len(b)-utf8.UTFMax; i-- {
		if !utf8.RuneStart(b[i]) {
			continue
		}
		if utf8.FullRune(b[i:]) {
			return len(b)
		}
		return i
	}
	return len(b)
}

// CapOutput cuts output from an adapter that hands it over whole, the way
// Output cuts a stream.
func CapOutput(text string) (string, bool) {
	if len(text) <= ports.MaxCommandOutputBytes {
		return text, false
	}
	b := []byte(text[:ports.MaxCommandOutputBytes])
	return string(b[:completeUTF8(b)]), true
}

// RandomID is random lowercase letters and digits, for sandbox names.
func RandomID(n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	_, _ = rand.Read(b)
	for i := range b {
		b[i] = alphabet[int(b[i])%len(alphabet)]
	}
	return string(b)
}
