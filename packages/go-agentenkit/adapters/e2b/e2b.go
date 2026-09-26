// Package e2b runs sandboxes on E2B: each is a small VM that starts in
// under a second. It talks to E2B's HTTP API directly, so it needs no SDK;
// the TS runtime has the same adapter (E2BSandbox). A sandbox shuts itself
// down at its timeout, which the runtime pushes back while the thread uses
// it.
package e2b

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/internal/sandboxsh"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Options shapes the provider.
type Options struct {
	// Template is what each sandbox starts from. Default "base".
	Template string
	// Network is "none" (the default), "all", or "allow" with Allow: only
	// these hosts.
	Network ports.SandboxNetwork
	// Domain defaults to e2b.app.
	Domain string
	// APIURL defaults to https://api.<domain>.
	APIURL string
	// SandboxURL sends every call to one sandbox here instead of
	// https://49983-<id>.<domain>, naming the sandbox in a header. For
	// tests and proxies.
	SandboxURL string
	// HTTPClient defaults to http.DefaultClient.
	HTTPClient *http.Client
}

const (
	envdPort = 49983
	workdir  = "/home/user"
)

// Provider makes E2B sandboxes.
type Provider struct {
	apiKey string
	opts   Options
	client *http.Client
}

// New returns the provider. The API key is required.
func New(apiKey string, opts Options) (*Provider, error) {
	if apiKey == "" {
		return nil, errors.New("E2BSandbox: apiKey is required")
	}
	if opts.Domain == "" {
		opts.Domain = "e2b.app"
	}
	if opts.APIURL == "" {
		opts.APIURL = "https://api." + opts.Domain
	}
	client := opts.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	return &Provider{apiKey: apiKey, opts: opts, client: client}, nil
}

// Name implements ports.SandboxProvider.
func (p *Provider) Name() string { return "e2b" }

// api makes a call to E2B's API.
func (p *Provider) api(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, p.opts.APIURL+path, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-API-Key", p.apiKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return p.client.Do(req)
}

type created struct {
	SandboxID       string `json:"sandboxID"`
	Domain          string `json:"domain"`
	EnvdAccessToken string `json:"envdAccessToken"`
}

// Create implements ports.SandboxProvider.
func (p *Provider) Create(ctx context.Context, opts ports.CreateSandboxOptions) (ports.Sandbox, error) {
	network := p.opts.Network
	if opts.Network != nil {
		network = *opts.Network
	}
	metadata := map[string]string{"agentenkit.threadId": opts.Metadata.ThreadID}
	if opts.Metadata.RunID != "" {
		metadata["agentenkit.runId"] = opts.Metadata.RunID
	}
	template := opts.Template
	if template == "" {
		template = p.opts.Template
	}
	if template == "" {
		template = "base"
	}
	body := map[string]any{"templateID": template, "timeout": seconds(opts.Timeout), "metadata": metadata}
	if opts.Envs != nil {
		body["envVars"] = opts.Envs
	}
	switch network.Mode {
	case "allow":
		body["allow_internet_access"] = true
		body["network"] = map[string]any{"allowOut": network.Allow, "denyOut": []string{"0.0.0.0/0"}}
	default:
		body["allow_internet_access"] = network.Mode == "all"
	}
	for k, v := range opts.Extra {
		body[k] = v
	}
	res, err := p.api(ctx, http.MethodPost, "/v2/sandboxes", body)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		return nil, fmt.Errorf("e2b: could not start a sandbox: %d %s", res.StatusCode, snippet(res.Body))
	}
	var c created
	if err := json.NewDecoder(res.Body).Decode(&c); err != nil {
		return nil, err
	}
	return p.box(c), nil
}

// Connect implements ports.SandboxProvider.
func (p *Provider) Connect(ctx context.Context, sandboxID string) (ports.Sandbox, error) {
	res, err := p.api(ctx, http.MethodPost, "/v2/sandboxes/"+url.PathEscape(sandboxID)+"/connect", map[string]any{})
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound || res.StatusCode == http.StatusGone {
		return nil, ports.SandboxGone(sandboxID, "")
	}
	if res.StatusCode >= 300 {
		return nil, fmt.Errorf("e2b: could not reach sandbox %s: %d %s", sandboxID, res.StatusCode, snippet(res.Body))
	}
	var c created
	if err := json.NewDecoder(res.Body).Decode(&c); err != nil {
		return nil, err
	}
	return p.box(c), nil
}

func (p *Provider) box(c created) *box {
	domain := c.Domain
	if domain == "" {
		domain = p.opts.Domain
	}
	envdURL := p.opts.SandboxURL
	if envdURL == "" {
		envdURL = fmt.Sprintf("https://%d-%s.%s", envdPort, c.SandboxID, domain)
	}
	b := &box{p: p, id: c.SandboxID, domain: domain, envdURL: envdURL, token: c.EnvdAccessToken}
	b.files = &files{b: b}
	return b
}

type box struct {
	p                          *Provider
	id, domain, envdURL, token string
	files                      *files
}

func (b *box) ID() string                          { return b.id }
func (b *box) Provider() string                    { return "e2b" }
func (b *box) Filesystem() ports.SandboxFileSystem { return b.files }
func (b *box) path(p string) string                { return sandboxsh.ResolveIn(workdir, p) }

// envd makes a call to the sandbox's own daemon.
func (b *box) envd(ctx context.Context, method, path string, body io.Reader, headers map[string]string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, b.envdURL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("E2b-Sandbox-Id", b.id)
	req.Header.Set("E2b-Sandbox-Port", strconv.Itoa(envdPort))
	if b.token != "" {
		req.Header.Set("X-Access-Token", b.token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	res, err := b.p.client.Do(req)
	if err != nil {
		return nil, err
	}
	// E2B's proxy answers 502 for a sandbox that is not running any more.
	if res.StatusCode == http.StatusBadGateway {
		res.Body.Close()
		return nil, ports.SandboxGone(b.id, "")
	}
	return res, nil
}

type rpcError struct{ code, message string }

// rpc makes a unary Connect call to envd, in JSON.
func (b *box) rpc(ctx context.Context, method string, body, out any) (*rpcError, error) {
	raw, _ := json.Marshal(body)
	res, err := b.envd(ctx, http.MethodPost, "/"+method, bytes.NewReader(raw),
		map[string]string{"Content-Type": "application/json", "Connect-Protocol-Version": "1"})
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	data, _ := io.ReadAll(res.Body)
	if res.StatusCode < 300 {
		if out != nil {
			_ = json.Unmarshal(data, out)
		}
		return nil, nil
	}
	var e struct {
		Code    any    `json:"code"`
		Message string `json:"message"`
	}
	_ = json.Unmarshal(data, &e)
	code := fmt.Sprint(e.Code)
	if e.Code == nil {
		code = strconv.Itoa(res.StatusCode)
	}
	return &rpcError{code: code, message: e.Message}, nil
}

type processEvent struct {
	Event struct {
		Start *struct {
			Pid int `json:"pid"`
		} `json:"start"`
		Data *struct {
			Stdout string `json:"stdout"`
			Stderr string `json:"stderr"`
		} `json:"data"`
		End *struct {
			ExitCode int `json:"exitCode"`
		} `json:"end"`
	} `json:"event"`
	Error *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (b *box) RunCommand(ctx context.Context, command string, opts ports.RunCommandOptions) (ports.CommandResult, error) {
	limit := opts.Timeout
	if limit <= 0 {
		limit = ports.DefaultCommandTimeout
	}
	cmd, secs := command, sandboxsh.TimeoutSeconds(limit)
	onOut, onErr := opts.OnStdout, opts.OnStderr
	if opts.Background {
		cmd, secs, onOut, onErr = sandboxsh.BackgroundCommand(command), 0, nil, nil
	}
	// timeout inside the sandbox stops the command and what it started; the
	// timer here is the backstop, and kills it through envd.
	argv := sandboxsh.RunArgv(cmd, secs, true)
	cwd := opts.Cwd
	if cwd == "" {
		cwd = "."
	}
	envs := opts.Env
	if envs == nil {
		envs = map[string]string{}
	}
	start := map[string]any{
		"process": map[string]any{"cmd": argv[0], "args": argv[1:], "cwd": b.path(cwd), "envs": envs},
		"stdin":   false,
	}
	out, errOut := sandboxsh.NewOutput(onOut), sandboxsh.NewOutput(onErr)
	started := time.Now()
	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var pid atomic.Int64
	var timedOut atomic.Bool
	kill := func() {
		if id := pid.Load(); id > 0 {
			_, _ = b.rpc(context.Background(), "process.Process/SendSignal",
				map[string]any{"process": map[string]any{"pid": id}, "signal": "SIGNAL_SIGKILL"}, nil)
		}
	}
	timer := time.AfterFunc(limit+time.Second, func() {
		timedOut.Store(true)
		kill()
		cancel()
	})
	defer timer.Stop()

	exitCode := -1
	err := func() error {
		res, err := b.envd(reqCtx, http.MethodPost, "/process.Process/Start", bytes.NewReader(Envelope(start, 0)), map[string]string{
			"Content-Type":             "application/connect+json",
			"Connect-Protocol-Version": "1",
			"Keepalive-Ping-Interval":  "50",
		})
		if err != nil {
			return err
		}
		defer res.Body.Close()
		if res.StatusCode == http.StatusNotFound {
			return ports.SandboxGone(b.id, "")
		}
		if res.StatusCode >= 300 {
			return fmt.Errorf("e2b: could not run the command: %d %s", res.StatusCode, snippet(res.Body))
		}
		frames := bufio.NewReader(res.Body)
		for {
			end, payload, err := ReadFrame(frames)
			if err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
			var ev processEvent
			if len(payload) > 0 {
				if err := json.Unmarshal(payload, &ev); err != nil {
					return err
				}
			}
			if end {
				if e := ev.Error; e != nil {
					if e.Code == "not_found" || e.Code == "unavailable" {
						return ports.SandboxGone(b.id, e.Message)
					}
					return fmt.Errorf("e2b: %s: %s", e.Code, e.Message)
				}
				return nil
			}
			switch {
			case ev.Event.Start != nil:
				pid.Store(int64(ev.Event.Start.Pid))
			case ev.Event.Data != nil:
				if s := ev.Event.Data.Stdout; s != "" {
					_, _ = out.Write(decode64(s))
				}
				if s := ev.Event.Data.Stderr; s != "" {
					_, _ = errOut.Write(decode64(s))
				}
			case ev.Event.End != nil:
				exitCode = ev.Event.End.ExitCode
			}
		}
	}()
	timer.Stop()
	if ctx.Err() != nil {
		kill()
		return ports.CommandResult{}, ctx.Err()
	}
	if err != nil && !timedOut.Load() {
		return ports.CommandResult{}, err
	}
	elapsed := time.Since(started)
	if opts.Background {
		return ports.CommandResult{ExitCode: exitCode, DurationMs: elapsed.Milliseconds()}, nil
	}
	stopped := timedOut.Load() || sandboxsh.LooksTimedOut(exitCode, elapsed, limit)
	if stopped {
		exitCode = ports.TimedOutExitCode
	}
	return ports.CommandResult{
		Stdout: out.String(), Stderr: errOut.String(), ExitCode: exitCode, DurationMs: elapsed.Milliseconds(),
		TimedOut: stopped, Truncated: out.Truncated || errOut.Truncated,
	}, nil
}

func (b *box) GetURL(_ context.Context, port int, protocol string) (string, error) {
	if protocol == "" {
		protocol = "https"
	}
	return fmt.Sprintf("%s://%d-%s.%s", protocol, port, b.id, b.domain), nil
}

func (b *box) GetInfo(ctx context.Context) (ports.SandboxInfo, error) {
	res, err := b.p.api(ctx, http.MethodGet, "/sandboxes/"+url.PathEscape(b.id), nil)
	if err != nil {
		return ports.SandboxInfo{}, err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound {
		return ports.SandboxInfo{}, ports.SandboxGone(b.id, "")
	}
	if res.StatusCode >= 300 {
		return ports.SandboxInfo{}, fmt.Errorf("e2b: %d %s", res.StatusCode, snippet(res.Body))
	}
	var d struct {
		StartedAt time.Time         `json:"startedAt"`
		EndAt     *time.Time        `json:"endAt"`
		State     string            `json:"state"`
		Metadata  map[string]string `json:"metadata"`
	}
	if err := json.NewDecoder(res.Body).Decode(&d); err != nil {
		return ports.SandboxInfo{}, err
	}
	md := map[string]string{}
	for k, v := range d.Metadata {
		if strings.HasPrefix(k, "agentenkit.") {
			md[strings.TrimPrefix(k, "agentenkit.")] = v
		}
	}
	status := "stopped"
	if d.State == "" || d.State == "running" {
		status = "running"
	}
	info := ports.SandboxInfo{ID: b.id, Provider: "e2b", Status: status, CreatedAt: d.StartedAt, Workdir: workdir, Metadata: md}
	if d.EndAt != nil {
		info.ExpiresAt = *d.EndAt
	}
	return info, nil
}

func (b *box) SetTimeout(ctx context.Context, d time.Duration) error {
	res, err := b.p.api(ctx, http.MethodPost, "/sandboxes/"+url.PathEscape(b.id)+"/timeout", map[string]any{"timeout": seconds(d)})
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound {
		return ports.SandboxGone(b.id, "")
	}
	if res.StatusCode >= 300 {
		return fmt.Errorf("e2b: could not set the sandbox's time: %d %s", res.StatusCode, snippet(res.Body))
	}
	return nil
}

func (b *box) Destroy(ctx context.Context) error {
	res, err := b.p.api(ctx, http.MethodDelete, "/sandboxes/"+url.PathEscape(b.id), nil)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 && res.StatusCode != http.StatusNotFound {
		return fmt.Errorf("e2b: could not end %s: %d", b.id, res.StatusCode)
	}
	return nil
}

type files struct{ b *box }

func (f *files) ReadFile(ctx context.Context, p string) (string, error) {
	b, err := f.ReadFileBytes(ctx, p)
	return string(b), err
}

func (f *files) ReadFileBytes(ctx context.Context, p string) ([]byte, error) {
	res, err := f.b.envd(ctx, http.MethodGet, "/files?"+url.Values{"path": {f.b.path(p)}}.Encode(), nil, nil)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound {
		return nil, ports.SandboxFileNotFound(p)
	}
	if res.StatusCode >= 300 {
		return nil, fmt.Errorf("e2b: could not read %s: %d %s", p, res.StatusCode, snippet(res.Body))
	}
	return io.ReadAll(res.Body)
}

func (f *files) WriteFile(ctx context.Context, p string, content []byte) error {
	full := f.b.path(p)
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	part, err := form.CreateFormFile("file", full)
	if err != nil {
		return err
	}
	_, _ = part.Write(content)
	_ = form.Close()
	res, err := f.b.envd(ctx, http.MethodPost, "/files?"+url.Values{"path": {full}}.Encode(), &body,
		map[string]string{"Content-Type": form.FormDataContentType()})
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		return fmt.Errorf("e2b: could not write %s: %d %s", p, res.StatusCode, snippet(res.Body))
	}
	return nil
}

func (f *files) ReadDir(ctx context.Context, p string) ([]ports.FileEntry, error) {
	var out struct {
		Entries []struct {
			Name string          `json:"name"`
			Type string          `json:"type"`
			Size json.RawMessage `json:"size"`
		} `json:"entries"`
	}
	e, err := f.b.rpc(ctx, "filesystem.Filesystem/ListDir", map[string]any{"path": f.b.path(p), "depth": 1}, &out)
	if err != nil {
		return nil, err
	}
	if e != nil {
		if e.code == "not_found" || e.code == "invalid_argument" {
			return nil, ports.SandboxFileNotFound(p)
		}
		return nil, fmt.Errorf("e2b: could not list %s: %s %s", p, e.code, e.message)
	}
	entries := []ports.FileEntry{}
	for _, en := range out.Entries {
		if en.Type == "FILE_TYPE_DIRECTORY" {
			entries = append(entries, ports.FileEntry{Name: en.Name, Type: "directory"})
			continue
		}
		// int64 comes as a string in protobuf JSON.
		size, _ := strconv.ParseInt(strings.Trim(string(en.Size), `"`), 10, 64)
		entries = append(entries, ports.FileEntry{Name: en.Name, Type: "file", Size: &size})
	}
	return sandboxsh.SortEntries(entries), nil
}

func (f *files) Mkdir(ctx context.Context, p string) error {
	e, err := f.b.rpc(ctx, "filesystem.Filesystem/MakeDir", map[string]any{"path": f.b.path(p)}, nil)
	if err != nil {
		return err
	}
	if e != nil && e.code != "already_exists" {
		return fmt.Errorf("e2b: could not make %s: %s %s", p, e.code, e.message)
	}
	return nil
}

func (f *files) Exists(ctx context.Context, p string) (bool, error) {
	e, err := f.b.rpc(ctx, "filesystem.Filesystem/Stat", map[string]any{"path": f.b.path(p)}, nil)
	if err != nil {
		return false, err
	}
	if e == nil {
		return true, nil
	}
	if e.code == "not_found" {
		return false, nil
	}
	return false, fmt.Errorf("e2b: could not check %s: %s %s", p, e.code, e.message)
}

func (f *files) Remove(ctx context.Context, p string) error {
	e, err := f.b.rpc(ctx, "filesystem.Filesystem/Remove", map[string]any{"path": f.b.path(p)}, nil)
	if err != nil {
		return err
	}
	if e != nil && e.code != "not_found" {
		return fmt.Errorf("e2b: could not remove %s: %s %s", p, e.code, e.message)
	}
	return nil
}

// seconds: E2B takes whole seconds.
func seconds(d time.Duration) int {
	s := int((d + time.Second - 1) / time.Second)
	if s < 1 {
		return 1
	}
	return s
}

func decode64(s string) []byte {
	b, _ := base64.StdEncoding.DecodeString(s)
	return b
}

func snippet(r io.Reader) string {
	b, _ := io.ReadAll(io.LimitReader(r, 300))
	return string(b)
}

// Envelope is one Connect streaming message: a flags byte, a 4-byte
// length, the JSON.
func Envelope(message any, flags byte) []byte {
	raw, _ := json.Marshal(message)
	out := make([]byte, 5+len(raw))
	out[0] = flags
	binary.BigEndian.PutUint32(out[1:5], uint32(len(raw)))
	copy(out[5:], raw)
	return out
}

// ReadFrame reads one message of a Connect stream. end is true for the
// last, which says whether the call failed.
func ReadFrame(r *bufio.Reader) (end bool, payload []byte, err error) {
	head := make([]byte, 5)
	if _, err := io.ReadFull(r, head); err != nil {
		return false, nil, err
	}
	payload = make([]byte, binary.BigEndian.Uint32(head[1:5]))
	if _, err := io.ReadFull(r, payload); err != nil {
		return false, nil, err
	}
	return head[0]&0x02 != 0, payload, nil
}
