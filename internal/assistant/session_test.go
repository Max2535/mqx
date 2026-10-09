package assistant

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/testutil/fakebroker"
)

var kafkaCaps = []string{broker.CapTopicDescriber, broker.CapClusterInspector, broker.CapConsumerInspector,
	broker.CapGroupInspector, broker.CapLagReporter, broker.CapTopicAdmin}

// script is a fake API: it answers each request with the next reply and
// records what it was sent.
type script struct {
	replies []string // BetaMessage JSON
	sent    []anthropic.BetaMessageNewParams
	err     error
}

func (s *script) send(_ context.Context, p anthropic.BetaMessageNewParams) (*anthropic.BetaMessage, error) {
	s.sent = append(s.sent, p)
	if s.err != nil {
		return nil, s.err
	}
	if len(s.replies) == 0 {
		return nil, errors.New("script: no reply left")
	}
	var m anthropic.BetaMessage
	if err := json.Unmarshal([]byte(s.replies[0]), &m); err != nil {
		return nil, err
	}
	s.replies = s.replies[1:]
	return &m, nil
}

func reply(stop string, content ...string) string {
	return `{"id":"msg_1","type":"message","role":"assistant","model":"claude-opus-5-5","stop_reason":"` + stop +
		`","usage":{"input_tokens":1,"output_tokens":1},"content":[` + strings.Join(content, ",") + `]}`
}

func textBlock(s string) string {
	b, _ := json.Marshal(s)
	return `{"type":"text","text":` + string(b) + `}`
}

func toolUse(id, name, input string) string {
	return `{"type":"tool_use","id":"` + id + `","name":"` + name + `","input":` + input + `}`
}

func newFake(t *testing.T) *fakebroker.Fake {
	t.Helper()
	f, _ := fakebroker.New(t)
	f.Caps = kafkaCaps
	f.AddTopic("orders", 2)
	f.AddGroup(broker.GroupDescription{Name: "billing", State: "Stable", GroupProtocol: "classic", ProtocolType: "consumer"},
		map[string]map[int32]int64{"orders": {0: 1, 1: 0}})
	return f
}

func TestStepRunsToolsUntilTheAnswer(t *testing.T) {
	f := newFake(t)
	api := &script{replies: []string{
		reply("tool_use", textBlock("Checking the group."), toolUse("tu_1", "describe_group", `{"group":"billing"}`),
			toolUse("tu_2", "list_nodes", `{}`)),
		reply("end_turn", textBlock("billing is Stable.")),
	}}
	s := NewSession(api.send, Settings{}, Context{Name: "dev", Broker: "kafka"}, Tools(f, Options{}))
	s.Ask("is billing healthy?")

	turn, err := s.Step(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if turn.Done || turn.Text != "Checking the group." || len(turn.Calls) != 2 || turn.Calls[0].Tool != "describe_group" {
		t.Fatalf("first turn = %+v", turn)
	}
	turn, err = s.Step(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !turn.Done || turn.Text != "billing is Stable." {
		t.Fatalf("second turn = %+v", turn)
	}

	// The second request carries both tool results in one user message.
	second := api.sent[1]
	last := second.Messages[len(second.Messages)-1]
	if last.Role != anthropic.BetaMessageParamRoleUser || len(last.Content) != 2 {
		t.Fatalf("last message = %+v", last)
	}
	result := last.Content[0].OfToolResult
	if result == nil || result.ToolUseID != "tu_1" || result.IsError.Value {
		t.Fatalf("first result = %+v", last.Content[0])
	}
	if got := result.Content[0].OfText.Text; !strings.Contains(got, `"billing"`) || !strings.Contains(got, `"Stable"`) {
		t.Fatalf("describe_group result = %s", got)
	}

	// Defaults and the server-side fallback go on every request.
	p := api.sent[0]
	if p.Model != DefaultModel || p.OutputConfig.Effort != "medium" || p.MaxTokens != 16000 ||
		!slices.Contains(p.Betas, anthropic.AnthropicBetaServerSideFallback2026_07_01) {
		t.Fatalf("request settings = model %s effort %s max %d betas %v", p.Model, p.OutputConfig.Effort, p.MaxTokens, p.Betas)
	}
	if fb, _ := json.Marshal(p.Fallbacks); string(fb) != `"default"` {
		t.Fatalf("fallbacks = %s", fb)
	}
	if !strings.Contains(p.System[0].Text, `context "dev", broker kafka, writable`) {
		t.Fatalf("system prompt lacks the connection:\n%s", p.System[0].Text)
	}
}

func TestToolErrorsGoBackToTheModel(t *testing.T) {
	f := newFake(t)
	api := &script{replies: []string{
		reply("tool_use", toolUse("tu_1", "describe_group", `{}`), toolUse("tu_2", "drop_topic", `{"name":"orders"}`)),
		reply("end_turn", textBlock("ok")),
	}}
	s := NewSession(api.send, Settings{}, Context{}, Tools(f, Options{}))
	s.Ask("q")
	turn, err := s.Step(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if turn.Calls[0].Err != "missing group" || turn.Calls[1].Err != "unknown tool" {
		t.Fatalf("calls = %+v", turn.Calls)
	}
	if _, err := s.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	results := api.sent[1].Messages[2].Content
	for i, r := range results {
		if !r.OfToolResult.IsError.Value {
			t.Errorf("result %d is not an error: %+v", i, r.OfToolResult)
		}
	}
	if len(f.Calls) != 0 {
		t.Errorf("broker calls = %q, want none", f.Calls)
	}
}

func TestFailedRequestDropsTheQuestion(t *testing.T) {
	api := &script{err: errors.New("boom")}
	s := NewSession(api.send, Settings{Model: "claude-sonnet-5-5", Effort: "low"}, Context{}, nil)
	s.Ask("first")
	if _, err := s.Step(context.Background()); err == nil {
		t.Fatal("want an error")
	}
	api.err = nil
	api.replies = []string{reply("end_turn", textBlock("hi"))}
	s.Ask("second")
	if _, err := s.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	p := api.sent[1]
	if len(p.Messages) != 1 || p.Model != "claude-sonnet-5-5" || p.OutputConfig.Effort != "low" {
		t.Fatalf("retry sent %d messages, model %s, effort %s", len(p.Messages), p.Model, p.OutputConfig.Effort)
	}
}

func TestStopsAfterTooManyRounds(t *testing.T) {
	f := newFake(t)
	api := &script{}
	for range maxRounds {
		api.replies = append(api.replies, reply("tool_use", toolUse("tu", "list_topics", `{}`)))
	}
	s := NewSession(api.send, Settings{}, Context{}, Tools(f, Options{}))
	s.Ask("loop")
	for i := range maxRounds {
		turn, err := s.Step(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if done := i == maxRounds-1; turn.Done != done {
			t.Fatalf("round %d done = %v", i, turn.Done)
		}
	}
	// The next question still alternates roles.
	api.replies = []string{reply("end_turn", textBlock("ok"))}
	s.Ask("next")
	if _, err := s.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	msgs := api.sent[len(api.sent)-1].Messages
	for i := 1; i < len(msgs); i++ {
		if msgs[i].Role == msgs[i-1].Role {
			t.Fatalf("messages %d and %d are both %s", i-1, i, msgs[i].Role)
		}
	}
}

func TestPeekNeedsSendPayloads(t *testing.T) {
	f := newFake(t)
	f.AddMessages("orders", 0, broker.Message{Key: []byte("k"), Value: []byte(`{"total":1}`)},
		broker.Message{Value: []byte{0xff, 0x00}})
	names := func(tools []Tool) []string {
		var out []string
		for _, t := range tools {
			out = append(out, t.Name)
		}
		return out
	}
	if slices.Contains(names(Tools(f, Options{})), "peek_messages") {
		t.Fatal("peek_messages offered without send_payloads")
	}
	tools := Tools(f, Options{SendPayloads: true})
	i := slices.Index(names(tools), "peek_messages")
	if i < 0 {
		t.Fatal("peek_messages missing with send_payloads")
	}
	got, err := tools[i].Run(context.Background(), Input{"topic": "orders", "limit": float64(5)})
	if err != nil {
		t.Fatal(err)
	}
	out := encodeResult(got)
	if !strings.Contains(out, `"key":"k"`) || !strings.Contains(out, `<2 bytes of binary>`) {
		t.Fatalf("peek result = %s", out)
	}
}

func TestToolsFollowCapabilities(t *testing.T) {
	f, _ := fakebroker.New(t)
	f.Caps = []string{broker.CapTopologyInspector, broker.CapRouteSimulator}
	var names []string
	for _, tool := range Tools(f, Options{}) {
		names = append(names, tool.Name)
	}
	want := []string{"list_topics", "list_exchanges", "list_bindings", "route"}
	if !slices.Equal(names, want) {
		t.Fatalf("tools = %v, want %v", names, want)
	}
}
