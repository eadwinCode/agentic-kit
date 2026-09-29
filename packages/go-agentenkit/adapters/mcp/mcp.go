// Package mcp connects an agent to MCP servers. It starts a local server
// over stdio, or talks to one over HTTP ("streamable HTTP"), lists its tools
// and turns each into an ordinary tool that calls the server. The same
// adapter lives in the TS package (agentenkit/adapters/mcp).
package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
	"github.com/zendev-sh/goai"
)

// ProtocolVersion is the MCP version we ask for. A server may answer with an
// older one; we only use initialize, tools/list and tools/call, which every
// version has.
const ProtocolVersion = "2025-06-18"

// Stdio is a local command we start and talk to over stdin/stdout.
type Stdio struct {
	Command string
	Args    []string
	// Env is added to this process's environment.
	Env map[string]string
	Dir string
}

// HTTP is a URL that speaks MCP over HTTP.
type HTTP struct {
	URL     string
	Headers map[string]string
	Client  *http.Client
}

// Server says how to reach one MCP server and which of its tools to use.
type Server struct {
	// Name is short, like "github". It is the default prefix of the tool
	// names, so two servers can both have a "search" tool.
	Name string
	// Set exactly one of Stdio and HTTP.
	Stdio *Stdio
	HTTP  *HTTP
	// Prefix goes before each tool's name. nil means Name + "_"; point it
	// at "" for none.
	Prefix *string
	// Tools keeps only these tools (by the server's own names). Empty: all.
	Tools []string
	// RequiresConfirmation parks these tools behind an approval (§2.5), by
	// the server's own names. ConfirmAll parks every one.
	RequiresConfirmation []string
	ConfirmAll           bool
	// Timeout is how long one request may take. Zero means 60s.
	Timeout time.Duration
}

// Connection is a live server and its tools.
type Connection struct {
	Name string
	// Tools are ready to put in an agent's Tools.
	Tools   []ports.Tool
	closers []func() error
}

// Close stops the server process, or ends the HTTP session.
func (c *Connection) Close() error {
	var errs []error
	for _, f := range c.closers {
		if err := f(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  any             `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type toolInfo struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

// Content is one part of a tool's answer.
type Content struct {
	Type     string `json:"type"`
	Text     string `json:"text,omitempty"`
	MimeType string `json:"mimeType,omitempty"`
	Resource *struct {
		URI  string  `json:"uri,omitempty"`
		Text *string `json:"text,omitempty"`
	} `json:"resource,omitempty"`
}

// CallResult is what tools/call answers.
type CallResult struct {
	Content           []Content       `json:"content"`
	StructuredContent json.RawMessage `json:"structuredContent,omitempty"`
	IsError           bool            `json:"isError,omitempty"`
}

var initParams = map[string]any{
	"protocolVersion": ProtocolVersion,
	"capabilities":    map[string]any{},
	"clientInfo":      map[string]any{"name": "agentenkit", "version": "1"},
}

type channel interface {
	request(ctx context.Context, method string, params any) (json.RawMessage, error)
	notify(ctx context.Context, method string, params any) error
	close() error
}

// Connect connects to one server, lists its tools and wraps each one. Keep
// the connection for as long as the workers run, and Close it on shutdown.
func Connect(ctx context.Context, s Server) (*Connection, error) {
	if s.Name == "" {
		return nil, errors.New("mcp: Name is required")
	}
	timeout := s.Timeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	var ch channel
	var err error
	switch {
	case s.Stdio != nil && s.HTTP == nil:
		ch, err = newStdio(s.Name, *s.Stdio, timeout)
	case s.HTTP != nil && s.Stdio == nil:
		ch = newHTTP(s.Name, *s.HTTP, timeout)
	default:
		return nil, fmt.Errorf("mcp(%s): set exactly one of Stdio and HTTP", s.Name)
	}
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*Connection, error) {
		_ = ch.close()
		return nil, err
	}
	if _, err := ch.request(ctx, "initialize", initParams); err != nil {
		return fail(err)
	}
	if err := ch.notify(ctx, "notifications/initialized", nil); err != nil {
		return fail(err)
	}
	infos, err := listTools(ctx, ch)
	if err != nil {
		return fail(err)
	}
	tools, err := buildTools(s, infos, ch)
	if err != nil {
		return fail(err)
	}
	return &Connection{Name: s.Name, Tools: tools, closers: []func() error{ch.close}}, nil
}

// ConnectAll connects to several servers and merges their tools. Two tools
// with the same final name fail, so one never hides the other.
func ConnectAll(ctx context.Context, servers ...Server) (*Connection, error) {
	type res struct {
		c   *Connection
		err error
	}
	results := make([]res, len(servers))
	var wg sync.WaitGroup
	for i, s := range servers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := Connect(ctx, s)
			results[i] = res{c, err}
		}()
	}
	wg.Wait()
	all := &Connection{}
	names := make([]string, 0, len(servers))
	var firstErr error
	seen := map[string]bool{}
	for i, r := range results {
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
			continue
		}
		names = append(names, servers[i].Name)
		all.closers = append(all.closers, r.c.closers...)
		for _, t := range r.c.Tools {
			if seen[t.Name] && firstErr == nil {
				firstErr = fmt.Errorf("mcp: two tools are called %q; give a server a prefix", t.Name)
			}
			seen[t.Name] = true
			all.Tools = append(all.Tools, t)
		}
	}
	if firstErr != nil {
		_ = all.Close()
		return nil, firstErr
	}
	all.Name = strings.Join(names, ",")
	return all, nil
}

func listTools(ctx context.Context, ch channel) ([]toolInfo, error) {
	var out []toolInfo
	cursor := ""
	for {
		params := map[string]any{}
		if cursor != "" {
			params["cursor"] = cursor
		}
		raw, err := ch.request(ctx, "tools/list", params)
		if err != nil {
			return nil, err
		}
		var page struct {
			Tools      []toolInfo `json:"tools"`
			NextCursor string     `json:"nextCursor"`
		}
		if err := json.Unmarshal(raw, &page); err != nil {
			return nil, fmt.Errorf("tools/list: %w", err)
		}
		out = append(out, page.Tools...)
		if page.NextCursor == "" {
			return out, nil
		}
		cursor = page.NextCursor
	}
}

var badNameChars = regexp.MustCompile(`[^a-zA-Z0-9_-]`)

// ToolName is a name providers accept: letters, digits, _ and -, at most 64.
func ToolName(prefix, name string) string {
	n := badNameChars.ReplaceAllString(prefix+name, "_")
	if len(n) > 64 {
		n = n[:64]
	}
	return n
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func buildTools(s Server, infos []toolInfo, ch channel) ([]ports.Tool, error) {
	prefix := s.Name + "_"
	if s.Prefix != nil {
		prefix = *s.Prefix
	}
	var out []ports.Tool
	seen := map[string]bool{}
	for _, info := range infos {
		if len(s.Tools) > 0 && !contains(s.Tools, info.Name) {
			continue
		}
		name := ToolName(prefix, info.Name)
		if seen[name] {
			return nil, fmt.Errorf("mcp(%s): two tools are called %q", s.Name, name)
		}
		seen[name] = true
		schema := info.InputSchema
		if len(schema) == 0 || schema[0] != '{' {
			schema = json.RawMessage(`{"type":"object","properties":{}}`)
		}
		mcpName := info.Name
		out = append(out, ports.Tool{
			Tool: goai.Tool{
				Name:        name,
				Description: info.Description,
				InputSchema: schema,
				Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
					var args any = map[string]any{}
					if len(bytes.TrimSpace(input)) > 0 {
						args = input
					}
					raw, err := ch.request(ctx, "tools/call", map[string]any{"name": mcpName, "arguments": args})
					if err != nil {
						return failed(s.Name + ": " + err.Error())
					}
					var res CallResult
					if err := json.Unmarshal(raw, &res); err != nil {
						return failed(s.Name + ": " + err.Error())
					}
					text := ContentText(res)
					if res.IsError {
						if text == "" {
							text = "the tool failed"
						}
						return failed(text)
					}
					return text, nil
				},
			},
			RequiresConfirmation: s.ConfirmAll || contains(s.RequiresConfirmation, info.Name),
		})
	}
	for _, want := range s.Tools {
		found := false
		for _, info := range infos {
			if info.Name == want {
				found = true
			}
		}
		if !found {
			return nil, fmt.Errorf("mcp(%s): the server has no tool called %q", s.Name, want)
		}
	}
	return out, nil
}

func failed(msg string) (string, error) {
	b, _ := json.Marshal(map[string]string{"error": msg})
	return string(b), nil
}

// ContentText is what the model reads back: the text parts joined, and a
// short note for anything that is not text. With no content, the structured
// result.
func ContentText(res CallResult) string {
	parts := make([]string, 0, len(res.Content))
	for _, c := range res.Content {
		switch {
		case c.Type == "text":
			parts = append(parts, c.Text)
		case c.Type == "resource" && c.Resource != nil && c.Resource.Text != nil:
			parts = append(parts, *c.Resource.Text)
		default:
			note := "[" + c.Type
			if c.MimeType != "" {
				note += " " + c.MimeType
			}
			if c.Resource != nil && c.Resource.URI != "" {
				note += " " + c.Resource.URI
			}
			parts = append(parts, note+"]")
		}
	}
	if len(parts) == 0 && len(res.StructuredContent) > 0 {
		var buf bytes.Buffer
		if json.Compact(&buf, res.StructuredContent) == nil {
			return buf.String()
		}
	}
	return strings.Join(parts, "\n")
}

// ---- stdio ----

type stdioChannel struct {
	server  string
	timeout time.Duration
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	gone    chan struct{}
	// wmu keeps writes whole. It is not mu, so a write stuck on a full pipe
	// never stops the reader from handing out answers.
	wmu sync.Mutex

	mu      sync.Mutex
	nextID  int
	closed  error
	waiting map[int]chan rpcMessage
}

func newStdio(server string, s Stdio, timeout time.Duration) (*stdioChannel, error) {
	cmd := exec.Command(s.Command, s.Args...)
	cmd.Env = os.Environ()
	for k, v := range s.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.Dir = s.Dir
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("%s: %w", server, err)
	}
	c := &stdioChannel{server: server, timeout: timeout, cmd: cmd, stdin: stdin, gone: make(chan struct{}), nextID: 1, waiting: map[int]chan rpcMessage{}}
	go c.read(stdout)
	return c, nil
}

func (c *stdioChannel) read(stdout io.Reader) {
	sc := bufio.NewScanner(stdout)
	sc.Buffer(make([]byte, 0, 64*1024), 64*1024*1024)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var msg rpcMessage
		if json.Unmarshal(line, &msg) != nil {
			continue // a log line on stdout
		}
		c.handle(msg)
	}
	code := -1
	if err := c.cmd.Wait(); err == nil {
		code = 0
	} else if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	}
	c.failAll(fmt.Errorf("%s: the server exited (code %d)", c.server, code))
	close(c.gone)
}

func (c *stdioChannel) handle(msg rpcMessage) {
	if msg.Method != "" {
		// A request from the server. We offer no client features, so answer
		// ping and turn down the rest.
		if len(msg.ID) == 0 {
			return
		}
		if msg.Method == "ping" {
			_ = c.write(rpcMessage{JSONRPC: "2.0", ID: msg.ID, Result: json.RawMessage(`{}`)})
		} else {
			_ = c.write(rpcMessage{JSONRPC: "2.0", ID: msg.ID, Error: &rpcError{Code: -32601, Message: "method not found: " + msg.Method}})
		}
		return
	}
	var id int
	if json.Unmarshal(msg.ID, &id) != nil {
		return
	}
	c.mu.Lock()
	w, ok := c.waiting[id]
	delete(c.waiting, id)
	c.mu.Unlock()
	if ok {
		w <- msg
	}
}

func (c *stdioChannel) failAll(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed == nil {
		c.closed = err
	}
	for id, w := range c.waiting {
		close(w)
		delete(c.waiting, id)
	}
}

func (c *stdioChannel) write(msg rpcMessage) error {
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed != nil {
		return closed
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_, err = c.stdin.Write(append(b, '\n'))
	return err
}

func (c *stdioChannel) request(ctx context.Context, method string, params any) (json.RawMessage, error) {
	c.mu.Lock()
	if c.closed != nil {
		err := c.closed
		c.mu.Unlock()
		return nil, err
	}
	id := c.nextID
	c.nextID++
	w := make(chan rpcMessage, 1)
	c.waiting[id] = w
	c.mu.Unlock()
	idRaw, _ := json.Marshal(id)
	if err := c.write(rpcMessage{JSONRPC: "2.0", ID: idRaw, Method: method, Params: params}); err != nil {
		c.mu.Lock()
		delete(c.waiting, id)
		c.mu.Unlock()
		return nil, err
	}
	timer := time.NewTimer(c.timeout)
	defer timer.Stop()
	giveUp := func(why string) (json.RawMessage, error) {
		c.mu.Lock()
		delete(c.waiting, id)
		c.mu.Unlock()
		// In the background: the pipe may be full, and giving up must not wait.
		go c.write(rpcMessage{JSONRPC: "2.0", Method: "notifications/cancelled", Params: map[string]any{"requestId": id}})
		return nil, fmt.Errorf("%s: %s %s", c.server, method, why)
	}
	select {
	case msg, ok := <-w:
		if !ok {
			c.mu.Lock()
			err := c.closed
			c.mu.Unlock()
			return nil, err
		}
		if msg.Error != nil {
			return nil, errors.New(msg.Error.Message)
		}
		return msg.Result, nil
	case <-timer.C:
		return giveUp(fmt.Sprintf("timed out after %dms", c.timeout.Milliseconds()))
	case <-ctx.Done():
		return giveUp("was stopped")
	}
}

func (c *stdioChannel) notify(_ context.Context, method string, params any) error {
	return c.write(rpcMessage{JSONRPC: "2.0", Method: method, Params: params})
}

func (c *stdioChannel) close() error {
	c.mu.Lock()
	if c.closed != nil {
		c.mu.Unlock()
		<-c.gone
		return nil
	}
	c.closed = fmt.Errorf("%s: the connection is closed", c.server)
	c.mu.Unlock()
	_ = c.stdin.Close()
	select {
	case <-c.gone:
		return nil
	case <-time.After(2 * time.Second):
	}
	_ = c.cmd.Process.Signal(os.Interrupt)
	select {
	case <-c.gone:
		return nil
	case <-time.After(3 * time.Second):
	}
	_ = c.cmd.Process.Kill()
	<-c.gone
	return nil
}

// ---- http ----

type httpChannel struct {
	server  string
	h       HTTP
	client  *http.Client
	timeout time.Duration

	mu        sync.Mutex
	nextID    int
	sessionID string
	version   string
}

func newHTTP(server string, h HTTP, timeout time.Duration) *httpChannel {
	client := h.Client
	if client == nil {
		client = http.DefaultClient
	}
	return &httpChannel{server: server, h: h, client: client, timeout: timeout, nextID: 1}
}

func (c *httpChannel) headers(req *http.Request) {
	for k, v := range c.h.Headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	c.mu.Lock()
	if c.sessionID != "" {
		req.Header.Set("Mcp-Session-Id", c.sessionID)
	}
	if c.version != "" {
		req.Header.Set("Mcp-Protocol-Version", c.version)
	}
	c.mu.Unlock()
}

func (c *httpChannel) post(ctx context.Context, msg rpcMessage) (*http.Response, error) {
	body, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.h.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	c.headers(req)
	res, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	if sid := res.Header.Get("Mcp-Session-Id"); sid != "" {
		c.mu.Lock()
		c.sessionID = sid
		c.mu.Unlock()
	}
	return res, nil
}

func (c *httpChannel) request(ctx context.Context, method string, params any) (json.RawMessage, error) {
	c.mu.Lock()
	id := c.nextID
	c.nextID++
	c.mu.Unlock()
	idRaw, _ := json.Marshal(id)
	rctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	msg, err := func() (rpcMessage, error) {
		res, err := c.post(rctx, rpcMessage{JSONRPC: "2.0", ID: idRaw, Method: method, Params: params})
		if err != nil {
			return rpcMessage{}, err
		}
		c.mu.Lock()
		hadSession := c.sessionID != ""
		c.mu.Unlock()
		if res.StatusCode == http.StatusNotFound && hadSession && method != "initialize" {
			// The server dropped our session: start a new one and try once more.
			res.Body.Close()
			c.mu.Lock()
			c.sessionID, c.version = "", ""
			c.mu.Unlock()
			if _, err := c.request(rctx, "initialize", initParams); err != nil {
				return rpcMessage{}, err
			}
			if err := c.notify(rctx, "notifications/initialized", nil); err != nil {
				return rpcMessage{}, err
			}
			if res, err = c.post(rctx, rpcMessage{JSONRPC: "2.0", ID: idRaw, Method: method, Params: params}); err != nil {
				return rpcMessage{}, err
			}
		}
		defer res.Body.Close()
		if res.StatusCode < 200 || res.StatusCode > 299 {
			b, _ := io.ReadAll(io.LimitReader(res.Body, 200))
			return rpcMessage{}, fmt.Errorf("HTTP %d %s", res.StatusCode, b)
		}
		if strings.Contains(res.Header.Get("Content-Type"), "text/event-stream") {
			return readSSE(res.Body, id)
		}
		var m rpcMessage
		err = json.NewDecoder(res.Body).Decode(&m)
		return m, err
	}()
	if err != nil {
		switch {
		case ctx.Err() != nil:
			return nil, fmt.Errorf("%s: %s was stopped", c.server, method)
		case rctx.Err() != nil:
			return nil, fmt.Errorf("%s: %s timed out after %dms", c.server, method, c.timeout.Milliseconds())
		}
		return nil, err
	}
	if msg.Error != nil {
		return nil, errors.New(msg.Error.Message)
	}
	if method == "initialize" {
		var r struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		_ = json.Unmarshal(msg.Result, &r)
		c.mu.Lock()
		c.version = r.ProtocolVersion
		if c.version == "" {
			c.version = ProtocolVersion
		}
		c.mu.Unlock()
	}
	return msg.Result, nil
}

func (c *httpChannel) notify(ctx context.Context, method string, params any) error {
	rctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	res, err := c.post(rctx, rpcMessage{JSONRPC: "2.0", Method: method, Params: params})
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, res.Body)
	return res.Body.Close()
}

func (c *httpChannel) close() error {
	c.mu.Lock()
	sid := c.sessionID
	c.mu.Unlock()
	if sid == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.h.URL, nil)
	if err == nil {
		c.headers(req)
		if res, err := c.client.Do(req); err == nil {
			res.Body.Close() // the server may not allow it; that is fine
		}
	}
	c.mu.Lock()
	c.sessionID = ""
	c.mu.Unlock()
	return nil
}

// readSSE reads an event stream until the message for id comes. Other
// messages on the stream (progress, logs) are skipped.
func readSSE(r io.Reader, id int) (rpcMessage, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 64*1024*1024)
	var data []string
	for sc.Scan() {
		line := strings.TrimSuffix(sc.Text(), "\r")
		if line == "" {
			text := strings.Join(data, "\n")
			data = nil
			if text == "" {
				continue
			}
			var msg rpcMessage
			if json.Unmarshal([]byte(text), &msg) != nil {
				continue
			}
			var got int
			if msg.Method == "" && json.Unmarshal(msg.ID, &got) == nil && got == id {
				return msg, nil
			}
		} else if strings.HasPrefix(line, "data:") {
			data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	if err := sc.Err(); err != nil {
		return rpcMessage{}, err
	}
	return rpcMessage{}, errors.New("the event stream ended with no answer")
}
