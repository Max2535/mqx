package mcpserver

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Max2535/mqx/internal/assistant"
	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/testutil/fakebroker"
)

// connect serves the fake broker's tools and returns a client session to it.
func connect(t *testing.T, opts assistant.Options) *mcp.ClientSession {
	t.Helper()
	f, _ := fakebroker.New(t)
	f.Caps = []string{broker.CapTopicDescriber, broker.CapClusterInspector, broker.CapConsumerInspector,
		broker.CapGroupInspector, broker.CapLagReporter}
	f.AddTopic("orders", 2)
	f.AddGroup(broker.GroupDescription{Name: "billing", State: "Stable", GroupProtocol: "classic", ProtocolType: "consumer"},
		map[string]map[int32]int64{"orders": {0: 1, 1: 0}})

	ctx := context.Background()
	srv := New(Info{Version: "test", Context: "dev", Broker: "kafka"}, assistant.Tools(f, opts), time.Second)
	st, ct := mcp.NewInMemoryTransports()
	ss, err := srv.Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ss.Close() })
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

func text(t *testing.T, res *mcp.CallToolResult) string {
	t.Helper()
	if len(res.Content) != 1 {
		t.Fatalf("content = %+v", res.Content)
	}
	tc, ok := res.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("content[0] = %T", res.Content[0])
	}
	return tc.Text
}

func TestListToolsOffersReadOnlyTools(t *testing.T) {
	cs := connect(t, assistant.Options{})
	res, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, tool := range res.Tools {
		names = append(names, tool.Name)
		if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint {
			t.Errorf("%s is not annotated read-only", tool.Name)
		}
	}
	for _, want := range []string{"list_topics", "describe_group", "group_lag", "diagnose_group"} {
		if !slices.Contains(names, want) {
			t.Errorf("tools = %v, missing %s", names, want)
		}
	}
	if slices.Contains(names, "peek_messages") {
		t.Errorf("peek_messages offered without SendPayloads: %v", names)
	}
	if cs.InitializeResult().Instructions == "" || !strings.Contains(cs.InitializeResult().Instructions, `"dev" (kafka, writable)`) {
		t.Errorf("instructions = %q", cs.InitializeResult().Instructions)
	}
}

func TestPayloadToolNeedsOptIn(t *testing.T) {
	cs := connect(t, assistant.Options{SendPayloads: true})
	res, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(res.Tools, func(tool *mcp.Tool) bool { return tool.Name == "peek_messages" }) {
		t.Fatal("peek_messages missing with SendPayloads")
	}
}

func TestCallTool(t *testing.T) {
	tests := []struct {
		name     string
		tool     string
		args     map[string]any
		wantErr  bool
		wantText string
	}{
		{name: "list topics", tool: "list_topics", wantText: `"orders"`},
		{name: "group lag", tool: "group_lag", args: map[string]any{"group": "billing"}, wantText: `"orders"`},
		{name: "missing argument is a tool error", tool: "describe_group", args: map[string]any{}, wantErr: true,
			wantText: `missing required string argument "group"`},
		{name: "broker error is a tool error", tool: "describe_group", args: map[string]any{"group": "nope"}, wantErr: true},
	}
	cs := connect(t, assistant.Options{})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: tt.tool, Arguments: tt.args})
			if err != nil {
				t.Fatal(err)
			}
			got := text(t, res)
			if res.IsError != tt.wantErr {
				t.Fatalf("IsError = %v, want %v (%s)", res.IsError, tt.wantErr, got)
			}
			if !strings.Contains(got, tt.wantText) {
				t.Fatalf("text = %s, want it to contain %s", got, tt.wantText)
			}
		})
	}
}
