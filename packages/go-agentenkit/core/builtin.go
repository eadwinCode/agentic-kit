package core

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"

	"github.com/zendev-sh/goai"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// The built-in tools as the model sees them: name, description, input. The
// same in every runtime: packages/parity/tools/<name>.json holds the shared
// copy, and a test fails when the one embedded here drifts from it.
//
//go:embed tooldefs/*.json
var toolDefs embed.FS

// BuiltinToolNames are the built-in tools this runtime has.
var BuiltinToolNames = []string{"web_search", "web_fetch"}

// BuiltinToolDefinition is one built-in tool as the model sees it.
type BuiltinToolDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

// BuiltinToolDefinitionOf reads one tool's definition.
func BuiltinToolDefinitionOf(name string) (BuiltinToolDefinition, error) {
	var def BuiltinToolDefinition
	raw, err := toolDefs.ReadFile("tooldefs/" + name + ".json")
	if err != nil {
		return def, fmt.Errorf("builtinTools: no built-in tool is called %q", name)
	}
	return def, json.Unmarshal(raw, &def)
}

// BuiltinToolOptions are the app's settings for the built-in tools.
type BuiltinToolOptions struct {
	WebSearch WebSearchOptions
	WebFetch  WebFetchOptions
}

// BuildBuiltinTools returns the built-in tools, ready for an agent's Tools.
// Each is an ordinary tool with a fixed name, description and input, so any
// model that can call tools can use it. A name this runtime does not know,
// or a tool whose port was not set up, is an error here, at startup.
func BuildBuiltinTools(tp ports.BuiltinToolPorts, names []string, opts BuiltinToolOptions) ([]ports.Tool, error) {
	out := make([]ports.Tool, 0, len(names))
	for _, name := range names {
		def, err := BuiltinToolDefinitionOf(name)
		if err != nil {
			return nil, err
		}
		var execute func(ctx context.Context, args map[string]any, run ToolRun) (string, error)
		switch name {
		case "web_search":
			if tp.Search == nil {
				return nil, fmt.Errorf("builtinTools: web_search needs RuntimeOptions.Tools.Search")
			}
			execute = func(ctx context.Context, args map[string]any, run ToolRun) (string, error) {
				return RunWebSearch(ctx, tp.Search, opts.WebSearch, args, run)
			}
		case "web_fetch":
			if tp.Fetcher == nil {
				return nil, fmt.Errorf("builtinTools: web_fetch needs RuntimeOptions.Tools.Fetcher")
			}
			execute = func(ctx context.Context, args map[string]any, run ToolRun) (string, error) {
				return RunWebFetch(ctx, tp.Fetcher, opts.WebFetch, args, run)
			}
		}
		toolName := name
		out = append(out, ports.WrapTool(goai.Tool{
			Name:        def.Name,
			Description: def.Description,
			InputSchema: def.InputSchema,
			Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
				run, ok := ToolRunFromContext(ctx)
				if !ok {
					return failed(toolName + " runs only inside an agentenkit run")
				}
				args := map[string]any{}
				if len(input) > 0 {
					if err := json.Unmarshal(input, &args); err != nil {
						return failed(toolName + ": the input is not a JSON object")
					}
				}
				return execute(ctx, args, run)
			},
		}))
	}
	return out, nil
}
