// Package assistant answers questions about the connected broker with Claude,
// using read-only broker queries as tools. It never changes broker state:
// changes are suggested as mqx commands for the user to run.
package assistant

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"
)

// DefaultModel is used when the config names none.
const DefaultModel = "claude-opus-5-5"

// maxRounds caps the tool rounds spent on one question.
const maxRounds = 12

// Sender sends one Messages API request. Client.Send is the real one; tests
// pass a fake.
type Sender func(ctx context.Context, p anthropic.BetaMessageNewParams) (*anthropic.BetaMessage, error)

// Context describes the connection the assistant works on.
type Context struct {
	Name     string
	Broker   string // "kafka", "rabbitmq", ...
	ReadOnly bool
}

// Settings are the request settings from the config.
type Settings struct {
	Model  string
	Effort string // low, medium, high, xhigh or max; "" means medium
}

// Session is one conversation. It is not safe for concurrent use: the TUI
// runs one Step at a time.
type Session struct {
	send     Sender
	settings Settings
	system   string
	tools    []Tool
	params   []anthropic.BetaToolUnionParam
	history  []anthropic.BetaMessageParam
	asked    int // len(history) before the current question
	rounds   int
}

// NewSession starts a conversation about ctx using tools.
func NewSession(send Sender, settings Settings, ctx Context, tools []Tool) *Session {
	if settings.Model == "" {
		settings.Model = DefaultModel
	}
	if settings.Effort == "" {
		settings.Effort = "medium"
	}
	s := &Session{send: send, settings: settings, system: systemPrompt(ctx, tools), tools: tools}
	for _, t := range tools {
		props := t.Params
		if props == nil {
			props = map[string]any{}
		}
		s.params = append(s.params, anthropic.BetaToolUnionParam{OfTool: &anthropic.BetaToolParam{
			Name:        t.Name,
			Description: anthropic.String(t.Description),
			InputSchema: anthropic.BetaToolInputSchemaParam{Properties: props, Required: t.Required},
		}})
	}
	return s
}

// Ask starts a new question; Step then works on it until a Turn is Done.
func (s *Session) Ask(question string) {
	s.asked, s.rounds = len(s.history), 0
	s.history = append(s.history, anthropic.NewBetaUserMessage(anthropic.NewBetaTextBlock(question)))
}

// Call is one tool call the model made.
type Call struct {
	Tool string
	Args string // compact JSON
	Err  string
}

// Turn is what one Step produced.
type Turn struct {
	Text  string
	Calls []Call
	// Done means the answer is complete and the next Step needs a new question.
	Done bool
	// Note explains an early stop (token limit, refusal, too many rounds).
	Note string
}

// Step sends the conversation, runs the tools the model asked for and
// reports what happened. On an error the current question is dropped, so the
// user can ask again.
func (s *Session) Step(ctx context.Context) (Turn, error) {
	resp, err := s.send(ctx, anthropic.BetaMessageNewParams{
		Model:        s.settings.Model,
		MaxTokens:    16000,
		System:       []anthropic.BetaTextBlockParam{{Text: s.system}},
		Messages:     s.history,
		Tools:        s.params,
		OutputConfig: anthropic.BetaOutputConfigParam{Effort: anthropic.BetaOutputConfigEffort(s.settings.Effort)},
		// A request the model declines is answered by a fallback model instead of stopping.
		Fallbacks: anthropic.BetaFallbacksParamOfDefault(),
		Betas:     []anthropic.AnthropicBeta{anthropic.AnthropicBetaServerSideFallback2026_07_01},
	})
	if err != nil {
		s.history = s.history[:s.asked]
		return Turn{}, err
	}
	s.history = append(s.history, resp.ToParam())
	var turn Turn
	var texts []string
	var results []anthropic.BetaContentBlockParamUnion
	for _, block := range resp.Content {
		switch b := block.AsAny().(type) {
		case anthropic.BetaTextBlock:
			texts = append(texts, b.Text)
		case anthropic.BetaToolUseBlock:
			call, result, isErr := s.run(ctx, b)
			turn.Calls = append(turn.Calls, call)
			results = append(results, anthropic.NewBetaToolResultBlock(b.ID, result, isErr))
		}
	}
	turn.Text = strings.TrimSpace(strings.Join(texts, "\n\n"))
	switch resp.StopReason {
	case anthropic.BetaStopReasonToolUse:
		s.history = append(s.history, anthropic.NewBetaUserMessage(results...))
		if s.rounds++; s.rounds >= maxRounds {
			// End the round with an assistant turn so the next question keeps the
			// history alternating between user and assistant.
			turn.Done, turn.Note = true, fmt.Sprintf("stopped after %d rounds of queries; ask a narrower question", maxRounds)
			s.history = append(s.history, anthropic.BetaMessageParam{
				Role:    anthropic.BetaMessageParamRoleAssistant,
				Content: []anthropic.BetaContentBlockParamUnion{anthropic.NewBetaTextBlock("(stopped: too many queries for one question)")},
			})
		}
	case anthropic.BetaStopReasonMaxTokens:
		turn.Done, turn.Note = true, "the answer hit the length limit and is cut"
	case anthropic.BetaStopReasonRefusal:
		turn.Done, turn.Note = true, "the model declined to answer this"
	default:
		turn.Done = true
	}
	return turn, nil
}

func (s *Session) run(ctx context.Context, use anthropic.BetaToolUseBlock) (call Call, result string, isErr bool) {
	raw := use.JSON.Input.Raw()
	call = Call{Tool: use.Name, Args: raw}
	var in Input
	if err := json.Unmarshal([]byte(raw), &in); err != nil {
		call.Err = "invalid input"
		return call, "invalid JSON input: " + err.Error(), true
	}
	for _, t := range s.tools {
		if t.Name != use.Name {
			continue
		}
		for _, req := range t.Required {
			if in.String(req) == "" {
				call.Err = "missing " + req
				return call, fmt.Sprintf("missing required string argument %q", req), true
			}
		}
		v, err := t.Run(ctx, in)
		if err != nil {
			call.Err = err.Error()
			return call, err.Error(), true
		}
		return call, encodeResult(v), false
	}
	call.Err = "unknown tool"
	return call, "unknown tool " + use.Name, true
}

func systemPrompt(ctx Context, tools []Tool) string {
	var names []string
	for _, t := range tools {
		names = append(names, t.Name)
	}
	mode := "writable"
	if ctx.ReadOnly {
		mode = "read_only: mqx refuses every change on it"
	}
	return fmt.Sprintf(`You are the assistant inside mqx, a terminal UI for Kafka and RabbitMQ.
You help a backend developer or platform engineer understand and troubleshoot the cluster they are connected to.

Connection: context %q, broker %s, %s.
Your tools (%s) query the broker read-only and return JSON. Use them to check facts before answering; do not guess names, counts or states.

You cannot change anything. When a change would help, say what it does and give the exact mqx command for the user to run, for example:
  mqx topic create <name> --partitions 6 --replication-factor 3
  mqx topic alter-config <name> --set retention.ms=86400000
  mqx topic add-partitions <name> --total <n>
  mqx group reset-offsets <group> --topic <topic> --to earliest --dry-run
  mqx group remove-member <group> --instance-id <id>
  mqx purge <queue|topic>
  mqx bind --source <exchange> --destination <queue> --key <key>
Mutating commands ask for confirmation or take --yes; suggest --dry-run where it exists.

Answers are shown in a narrow terminal panel: be brief, lead with the finding, use plain text and short "-" lists, no tables or headings.`,
		ctx.Name, ctx.Broker, mode, strings.Join(names, ", "))
}
