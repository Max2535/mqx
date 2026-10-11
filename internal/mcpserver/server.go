// Package mcpserver serves mqx's read-only broker tools over the Model Context
// Protocol, so MCP clients such as Claude Code, Claude Desktop or Cursor can
// query one context. It reuses the tools of the TUI assistant and, like them,
// never changes broker state.
package mcpserver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Max2535/mqx/internal/assistant"
)

// Info describes the server and the context it is bound to.
type Info struct {
	Version  string
	Context  string
	Broker   string
	ReadOnly bool
}

// New returns an MCP server offering tools. Each call is bounded by timeout
// (0 means no bound beyond the client's own cancellation).
func New(info Info, tools []assistant.Tool, timeout time.Duration) *mcp.Server {
	s := mcp.NewServer(
		&mcp.Implementation{Name: "mqx", Title: "mqx (" + info.Context + ")", Version: info.Version},
		&mcp.ServerOptions{Instructions: instructions(info, tools)},
	)
	for _, t := range tools {
		s.AddTool(&mcp.Tool{
			Name:        t.Name,
			Description: t.Description,
			InputSchema: inputSchema(t),
			Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true, IdempotentHint: true},
		}, handler(tools, t.Name, timeout))
	}
	return s
}

func inputSchema(t assistant.Tool) map[string]any {
	props := t.Params
	if props == nil {
		props = map[string]any{}
	}
	schema := map[string]any{"type": "object", "properties": props}
	if len(t.Required) > 0 {
		schema["required"] = t.Required
	}
	return schema
}

// handler runs one tool. Broker and input errors go back as tool errors, not
// protocol errors, so the client's model can read them and try again.
func handler(tools []assistant.Tool, name string, timeout time.Duration) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		if timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
		out, err := assistant.Invoke(ctx, tools, name, req.Params.Arguments)
		if err != nil {
			return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: err.Error()}}}, nil
		}
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: out}}}, nil
	}
}

func instructions(info Info, tools []assistant.Tool) string {
	names := make([]string, len(tools))
	for i, t := range tools {
		names[i] = t.Name
	}
	mode := "writable"
	if info.ReadOnly {
		mode = "read_only: mqx refuses every change on it"
	}
	return fmt.Sprintf(`mqx exposes one message broker context: %q (%s, %s).
The tools (%s) query it read-only and return JSON; none of them changes the broker.
To change something, give the user the mqx command to run, for example
"mqx topic add-partitions <name> --total <n>" or "mqx group reset-offsets <group> --topic <topic> --to earliest --dry-run".`,
		info.Context, info.Broker, mode, strings.Join(names, ", "))
}
