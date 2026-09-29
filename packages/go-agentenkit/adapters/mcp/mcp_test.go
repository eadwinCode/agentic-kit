package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// The same cases run in the TS package (test/mcp.test.ts). The test binary
// is its own fake server: run with MCP_FAKE=1 it speaks MCP over stdio.

func TestMain(m *testing.M) {
	if os.Getenv("MCP_FAKE") == "1" {
		fmt.Println("fake mcp server starting") // not JSON: the client must skip it
		sc := bufio.NewScanner(os.Stdin)
		for sc.Scan() {
			var msg map[string]any
			if json.Unmarshal(sc.Bytes(), &msg) != nil {
				continue
			}
			if out := answer(msg); out != nil {
				b, _ := json.Marshal(out)
				os.Stdout.Write(append(b, '\n'))
			}
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

var pages = [][]map[string]any{
	{
		{"name": "echo", "description": "Says the text back", "inputSchema": map[string]any{"type": "object", "properties": map[string]any{"text": map[string]any{"type": "string"}}, "required": []string{"text"}}},
		{"name": "fail", "description": "Always fails", "inputSchema": map[string]any{"type": "object", "properties": map[string]any{}}},
	},
	{
		{"name": "add", "description": "Adds two numbers", "inputSchema": map[string]any{"type": "object", "properties": map[string]any{"a": map[string]any{"type": "number"}, "b": map[string]any{"type": "number"}}}},
		{"name": "weird.name", "description": "A name with a dot"},
		{"name": "slow", "description": "Never answers in time", "inputSchema": map[string]any{"type": "object", "properties": map[string]any{}}},
	},
}

func answer(msg map[string]any) map[string]any {
	id, ok := msg["id"]
	if !ok {
		return nil
	}
	okr := func(r any) map[string]any { return map[string]any{"jsonrpc": "2.0", "id": id, "result": r} }
	params, _ := msg["params"].(map[string]any)
	switch msg["method"] {
	case "initialize":
		return okr(map[string]any{"protocolVersion": "2025-06-18", "capabilities": map[string]any{"tools": map[string]any{}}, "serverInfo": map[string]any{"name": "fake", "version": "1"}})
	case "tools/list":
		if params["cursor"] == "p2" {
			return okr(map[string]any{"tools": pages[1]})
		}
		return okr(map[string]any{"tools": pages[0], "nextCursor": "p2"})
	case "tools/call":
		args, _ := params["arguments"].(map[string]any)
		switch params["name"] {
		case "echo":
			return okr(map[string]any{"content": []any{map[string]any{"type": "text", "text": fmt.Sprintf("echo: %v", args["text"])}}})
		case "fail":
			return okr(map[string]any{"content": []any{map[string]any{"type": "text", "text": "it broke"}}, "isError": true})
		case "add":
			return okr(map[string]any{"content": []any{}, "structuredContent": map[string]any{"sum": args["a"].(float64) + args["b"].(float64)}})
		case "weird.name":
			return okr(map[string]any{"content": []any{map[string]any{"type": "text", "text": "hi"}, map[string]any{"type": "image", "mimeType": "image/png", "data": "AA=="}}})
		case "slow":
			return nil
		}
		return map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": -32602, "message": "no tool"}}
	}
	return map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": -32601, "message": "method not found"}}
}

func fake() *Stdio {
	return &Stdio{Command: os.Args[0], Args: []string{"-test.run=^$"}, Env: map[string]string{"MCP_FAKE": "1"}}
}

func names(c *Connection) []string {
	var out []string
	for _, t := range c.Tools {
		out = append(out, t.Name)
	}
	sort.Strings(out)
	return out
}

func call(t *testing.T, c *Connection, name, args string) string {
	t.Helper()
	for _, tool := range c.Tools {
		if tool.Name == name {
			out, err := tool.Execute(context.Background(), json.RawMessage(args))
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			return out
		}
	}
	t.Fatalf("no tool %s", name)
	return ""
}

func mustEqual[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestStdioListsEveryPageNamesAndCalls(t *testing.T) {
	c, err := Connect(context.Background(), Server{Name: "fake", Stdio: fake()})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	mustEqual(t, strings.Join(names(c), ","), "fake_add,fake_echo,fake_fail,fake_slow,fake_weird_name")
	mustEqual(t, call(t, c, "fake_echo", `{"text":"hello"}`), "echo: hello")
	mustEqual(t, call(t, c, "fake_fail", `{}`), `{"error":"it broke"}`)
	mustEqual(t, call(t, c, "fake_add", `{"a":2,"b":3}`), `{"sum":5}`)
	mustEqual(t, call(t, c, "fake_weird_name", `{}`), "hi\n[image image/png]")
}

func TestStdioFilterAndConfirmation(t *testing.T) {
	none := ""
	c, err := Connect(context.Background(), Server{Name: "fake", Prefix: &none, Tools: []string{"echo", "fail"}, RequiresConfirmation: []string{"fail"}, Stdio: fake()})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	mustEqual(t, strings.Join(names(c), ","), "echo,fail")
	for _, tool := range c.Tools {
		mustEqual(t, tool.RequiresConfirmation, tool.Name == "fail")
	}
}

func TestStdioUnknownToolFailsAtConnect(t *testing.T) {
	_, err := Connect(context.Background(), Server{Name: "fake", Tools: []string{"nope"}, Stdio: fake()})
	if err == nil || !strings.Contains(err.Error(), `no tool called "nope"`) {
		t.Fatalf("got %v", err)
	}
}

func TestStdioTimeout(t *testing.T) {
	c, err := Connect(context.Background(), Server{Name: "fake", Timeout: 300 * time.Millisecond, Stdio: fake()})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	mustEqual(t, call(t, c, "fake_slow", `{}`), `{"error":"fake: fake: tools/call timed out after 300ms"}`)
	mustEqual(t, call(t, c, "fake_echo", `{"text":"still up"}`), "echo: still up")
}

func TestStdioAfterClose(t *testing.T) {
	c, err := Connect(context.Background(), Server{Name: "fake", Stdio: fake()})
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	out := call(t, c, "fake_echo", `{"text":"x"}`)
	if !strings.Contains(out, "the connection is closed") {
		t.Fatalf("got %s", out)
	}
}

func TestStdioCannotStart(t *testing.T) {
	if _, err := Connect(context.Background(), Server{Name: "nope", Stdio: &Stdio{Command: "/no/such/mcp-server"}}); err == nil {
		t.Fatal("want an error")
	}
}

func httpFake(opened, deleted *atomic.Int32) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deleted.Add(1)
			return
		}
		var msg map[string]any
		_ = json.NewDecoder(r.Body).Decode(&msg)
		if msg["method"] != "initialize" && r.Header.Get("Mcp-Session-Id") != "sess-1" {
			http.Error(w, "no session", 400)
			return
		}
		out := answer(msg)
		if msg["method"] == "initialize" {
			opened.Add(1)
			w.Header().Set("Mcp-Session-Id", "sess-1")
		}
		if out == nil {
			w.WriteHeader(202)
			return
		}
		b, _ := json.Marshal(out)
		if msg["method"] == "tools/call" {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprintf(w, "event: message\ndata: %s\n\nevent: message\ndata: %s\n\n", `{"jsonrpc":"2.0","method":"notifications/progress","params":{}}`, b)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(b)
	}))
}

func TestHTTPSessionEventStreamAndClose(t *testing.T) {
	var opened, deleted atomic.Int32
	srv := httpFake(&opened, &deleted)
	defer srv.Close()
	c, err := Connect(context.Background(), Server{Name: "web", HTTP: &HTTP{URL: srv.URL + "/mcp"}})
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, len(c.Tools), 5)
	mustEqual(t, call(t, c, "web_echo", `{"text":"over http"}`), "echo: over http")
	mustEqual(t, call(t, c, "web_fail", `{}`), `{"error":"it broke"}`)
	c.Close()
	mustEqual(t, opened.Load(), int32(1))
	mustEqual(t, deleted.Load(), int32(1))
}

func TestConnectAllMergesAndRefusesClashes(t *testing.T) {
	var opened, deleted atomic.Int32
	srv := httpFake(&opened, &deleted)
	defer srv.Close()
	both, err := ConnectAll(context.Background(),
		Server{Name: "a", HTTP: &HTTP{URL: srv.URL}},
		Server{Name: "b", Stdio: fake()})
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, len(both.Tools), 10)
	both.Close()
	none := ""
	_, err = ConnectAll(context.Background(),
		Server{Name: "a", Prefix: &none, HTTP: &HTTP{URL: srv.URL}},
		Server{Name: "b", Prefix: &none, Stdio: fake()})
	if err == nil || !strings.Contains(err.Error(), "two tools are called") {
		t.Fatalf("got %v", err)
	}
}

func TestHelpers(t *testing.T) {
	mustEqual(t, ToolName("gh_", "repos.list/all"), "gh_repos_list_all")
	mustEqual(t, len(ToolName("", strings.Repeat("x", 80))), 64)
	body := "body"
	var res CallResult
	_ = json.Unmarshal([]byte(`{"content":[{"type":"resource","resource":{"uri":"file:///a","text":"body"}}]}`), &res)
	mustEqual(t, ContentText(res), body)
	var audio CallResult
	_ = json.Unmarshal([]byte(`{"content":[{"type":"audio","mimeType":"audio/wav"}]}`), &audio)
	mustEqual(t, ContentText(audio), "[audio audio/wav]")
	mustEqual(t, ContentText(CallResult{}), "")
}
