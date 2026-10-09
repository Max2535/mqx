package tui

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Max2535/mqx/internal/config"
)

// fakeModel answers assistant requests from a list of BetaMessage JSON replies.
type fakeModel struct {
	replies []string
	sent    int
}

func (m *fakeModel) send(_ context.Context, _ anthropic.BetaMessageNewParams) (*anthropic.BetaMessage, error) {
	m.sent++
	var msg anthropic.BetaMessage
	err := json.Unmarshal([]byte(m.replies[0]), &msg)
	m.replies = m.replies[1:]
	return &msg, err
}

func modelReply(stop, content string) string {
	return `{"id":"m","type":"message","role":"assistant","model":"x","stop_reason":"` + stop +
		`","usage":{"input_tokens":1,"output_tokens":1},"content":[` + content + `]}`
}

func TestAssistantAnswersWithBrokerQueries(t *testing.T) {
	f, c := fakeCtx(t, "dev", true, kafkaCaps)
	seedKafka(f)
	model := &fakeModel{replies: []string{
		modelReply("tool_use", `{"type":"tool_use","id":"t1","name":"group_lag","input":{"group":"billing"}}`),
		modelReply("end_turn", `{"type":"text","text":"billing lags by 1 on orders[0]. Run: mqx group lag billing"}`),
	}}
	h := newHarnessWith(t, Options{}, &assistSetup{send: model.send}, c)

	h.gotoPanel("Assistant")
	h.wantView("Ask about dev", "Only metadata is sent to the API")
	h.keys("enter")
	h.typeText("why does billing lag?")
	// q, c and tab are text while typing.
	h.keys("q", "backspace", "enter")

	h.wantView("› why does billing lag?", "⋯ group_lag group=billing", "billing lags by 1 on orders[0]")
	if model.sent != 2 {
		t.Fatalf("model requests = %d, want 2", model.sent)
	}
	wantCalls(t, f) // only reads: nothing recorded as a mutation

	h.keys("x")
	h.wantNotView("why does billing lag?")
	h.wantView("Ask about dev")
}

func TestAssistantPanelFollowsConfig(t *testing.T) {
	cfg := &config.Config{Contexts: []config.Context{{Name: "dev", Broker: "kafka"}}}
	if startConfig(context.Background(), cfg, Options{}).assist == nil {
		t.Fatal("assistant missing by default")
	}
	cfg.Assistant.Disabled = true
	if startConfig(context.Background(), cfg, Options{}).assist != nil {
		t.Fatal("assistant present with assistant.disabled")
	}
}

func TestCompactArgs(t *testing.T) {
	for in, want := range map[string]string{
		"":                                     "",
		"{}":                                   "",
		`{"group":"billing"}`:                  "group=billing",
		`{"exchange":"x","routing_key":"a.b"}`: "exchange=x routing_key=a.b",
		"not json":                             "not json",
	} {
		if got := compactArgs(in); got != want {
			t.Errorf("compactArgs(%q) = %q, want %q", in, got, want)
		}
	}
	if !strings.Contains(compactArgs(`{"limit":5}`), "limit=5") {
		t.Error("numbers not rendered")
	}
}
