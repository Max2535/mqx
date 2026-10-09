package tui

import (
	"context"
	"fmt"
	"time"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/config"
)

// env is what every panel shares for one open context: the broker, the
// context it came from and the helpers that run broker I/O off the UI loop.
// It is replaced (with a new generation) whenever the context changes, so
// results of requests made against an old broker are dropped.
type env struct {
	ctx     config.Context
	b       broker.Broker
	base    context.Context
	timeout time.Duration
	now     func() time.Time
	// tick schedules msg after d; nil disables timers (tests drive them by hand).
	tick func(d time.Duration, msg tea.Msg) tea.Cmd
	gen  int
	ids  *int

	// topic is the topic or queue last selected in the Topics panel; the
	// Consumers and Metrics panels follow it.
	topic string
	// topics are the topics or queues of the last Topics listing by name, and
	// nodes the number of brokers then (0 when unknown). Forms use them for
	// hints, and publishing to an internal topic needs its name typed.
	topics map[string]broker.Topic
	nodes  int
	// history keeps form submissions across contexts.
	history *formHistory
	// assist enables the Assistant panel; nil when the config disables it.
	assist *assistSetup
	// spin is the current spinner frame, updated by the root model.
	spin string
}

// routedMsg carries the result of a request back to the view that made it.
type routedMsg struct {
	gen int
	to  int
	msg tea.Msg
}

// newID returns a view id unique within this env.
func (e *env) newID() int {
	*e.ids++
	return *e.ids
}

// call runs fn with a timeout-bounded context in a tea.Cmd and routes its
// result to view to.
func (e *env) call(to int, fn func(ctx context.Context) tea.Msg) tea.Cmd {
	return e.callFor(to, e.timeout, fn)
}

// callFor is call with its own timeout, for work longer than one broker request.
func (e *env) callFor(to int, timeout time.Duration, fn func(ctx context.Context) tea.Msg) tea.Cmd {
	gen, base := e.gen, e.base
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(base, timeout)
		defer cancel()
		return routedMsg{gen: gen, to: to, msg: fn(ctx)}
	}
}

// send routes msg to view to without doing any I/O.
func (e *env) send(to int, msg tea.Msg) tea.Cmd {
	gen := e.gen
	return func() tea.Msg { return routedMsg{gen: gen, to: to, msg: msg} }
}

// push opens v on top of the panel stack that holds view from.
func (e *env) push(from int, v panel) tea.Cmd { return e.send(from, pushMsg{v: v}) }

// after routes msg to view to after d, or never when timers are disabled.
func (e *env) after(to int, d time.Duration, msg tea.Msg) tea.Cmd {
	if e.tick == nil {
		return nil
	}
	return e.tick(d, routedMsg{gen: e.gen, to: to, msg: msg})
}

// has reports whether the broker supports a capability.
func (e *env) has(capability string) bool { return e.b != nil && broker.Has(e.b, capability) }

// kafkaLike reports whether Kafka-only fields (partitions, follow) apply.
func (e *env) kafkaLike() bool {
	return e.b != nil && (e.b.Name() == "kafka" || e.has(broker.CapGroupInspector))
}

// rabbitLike reports whether RabbitMQ-only fields (exchange, routing key, properties) apply.
func (e *env) rabbitLike() bool {
	return e.b != nil && (e.b.Name() == "rabbitmq" || e.has(broker.CapTopologyInspector))
}

// kind is what the broker calls a topic: "queue" for RabbitMQ-like brokers.
func (e *env) kind() string {
	if e.rabbitLike() && !e.kafkaLike() {
		return "queue"
	}
	return "topic"
}

// as returns the broker as capability T. Callers only reach it for
// capabilities they checked with has, so a miss yields the zero value.
func as[T any](e *env) T {
	v, _ := e.b.(T)
	return v
}

// ---------------------------------------------------------------------------
// Mutation guard

// writable reports whether mutating actions may be offered at all.
func (e *env) writable() bool { return e.b != nil && !e.ctx.ReadOnly }

// mut returns b enabled only on writable contexts. A disabled binding never
// matches a key and is left out of help, so mutating actions are hidden on
// read_only contexts.
func (e *env) mut(b key.Binding) key.Binding {
	b.SetEnabled(e.writable())
	return b
}

// confirmMsg asks the root model to show a confirm dialog and run run on
// confirmation. With typed set, the user must type it instead of pressing y.
type confirmMsg struct {
	prompt string
	detail string
	typed  string
	run    tea.Cmd
}

// mutate is the TUI's single mutation gate. It refuses read_only contexts
// and otherwise asks for confirmation before running fn; typed, when set,
// must be typed to confirm. Adapters never check read_only themselves.
func (e *env) mutate(to int, action, detail, typed string, fn func(ctx context.Context) tea.Msg) tea.Cmd {
	if e.ctx.ReadOnly {
		return statusErr(fmt.Errorf("refusing to %s: context %q is read_only", lowerFirst(action), e.ctx.Name))
	}
	msg := confirmMsg{
		prompt: fmt.Sprintf("%s on %s?", action, e.ctx.Name),
		detail: detail,
		typed:  typed,
		run:    e.call(to, fn),
	}
	return func() tea.Msg { return msg }
}

func lowerFirst(s string) string {
	if s == "" || s[0] < 'A' || s[0] > 'Z' {
		return s
	}
	return string(s[0]+'a'-'A') + s[1:]
}
