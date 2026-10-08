package tui

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/Max2535/mqx/internal/broker"
)

const (
	defaultPeekLimit = 100
	maxFollowBuffer  = 1000
)

type browserKeys struct {
	refresh, filter, follow, raw, scrollUp, scrollDown key.Binding
}

// messageBrowser peeks a topic or queue: a message list with a viewer pane
// for the selected message, a filter form, follow mode (Kafka) and a raw view.
type messageBrowser struct {
	e       *env
	id      int
	topic   string
	filter  values
	opts    broker.PeekOptions
	msgs    []broker.Message
	t       *table
	loading bool
	err     error
	raw     bool
	scroll  int
	keys    browserKeys
	publish action

	following bool
	seq       int
	cancel    context.CancelFunc
}

type messagesMsg struct {
	msgs []broker.Message
	err  error
}

type filterMsg struct {
	v    values
	opts broker.PeekOptions
}

type streamStartedMsg struct {
	seq int
	ch  <-chan broker.Message
	err error
}

type streamMsg struct {
	seq int
	m   broker.Message
	ok  bool
	ch  <-chan broker.Message
}

func newMessageBrowser(e *env, topic string) *messageBrowser {
	mb := &messageBrowser{e: e, id: e.newID(), topic: topic, filter: values{"limit": strconv.Itoa(defaultPeekLimit)}}
	if e.kafkaLike() {
		mb.filter["from"] = "-20"
	}
	mb.opts, _ = peekOptions(mb.filter, e.now())
	mb.t = newTable(mb.cols()...)
	mb.keys = browserKeys{
		refresh:    key.NewBinding(key.WithKeys("r"), key.WithHelp("r", "reload")),
		filter:     key.NewBinding(key.WithKeys("/"), key.WithHelp("/", "filter")),
		follow:     key.NewBinding(key.WithKeys("f"), key.WithHelp("f", "follow")),
		raw:        key.NewBinding(key.WithKeys("w"), key.WithHelp("w", "raw view")),
		scrollUp:   key.NewBinding(key.WithKeys("ctrl+u", "K"), key.WithHelp("K", "scroll payload up")),
		scrollDown: key.NewBinding(key.WithKeys("ctrl+d", "J"), key.WithHelp("J", "scroll payload down")),
	}
	mb.keys.follow.SetEnabled(e.kafkaLike())
	mb.publish = publishAction(e, func(*row) string { return topic }, false)
	return mb
}

func (mb *messageBrowser) cols() []string {
	if mb.e.kind() == "queue" {
		return []string{"#", "EXCHANGE", "ROUTING KEY", "TIMESTAMP", "SIZE", "REDELIVERED"}
	}
	return []string{"#", "PART", "OFFSET", "KEY", "TIMESTAMP", "SIZE"}
}

// ID implements panel.
func (mb *messageBrowser) ID() int { return mb.id }

// Title implements panel.
func (mb *messageBrowser) Title() string { return "Messages" }

// Capturing implements panel.
func (mb *messageBrowser) Capturing() bool { return false }

// Init implements panel.
func (mb *messageBrowser) Init() tea.Cmd { return mb.load() }

func (mb *messageBrowser) close() {
	if mb.cancel != nil {
		mb.cancel()
		mb.cancel = nil
	}
	mb.following = false
}

func (mb *messageBrowser) load() tea.Cmd {
	mb.close()
	mb.loading = true
	topic, opts, b := mb.topic, mb.opts, mb.e.b
	return mb.e.call(mb.id, func(ctx context.Context) tea.Msg {
		ch, err := b.Peek(ctx, topic, opts)
		if err != nil {
			return messagesMsg{err: err}
		}
		var msgs []broker.Message
		for m := range ch {
			if m.Err != nil {
				return messagesMsg{msgs: msgs, err: m.Err}
			}
			msgs = append(msgs, m)
		}
		if err := ctx.Err(); err != nil && len(msgs) == 0 {
			return messagesMsg{err: err}
		}
		return messagesMsg{msgs: msgs}
	})
}

// startFollow streams new messages until stopped; it is not bounded by the request timeout.
func (mb *messageBrowser) startFollow() tea.Cmd {
	mb.close()
	ctx, cancel := context.WithCancel(mb.e.base)
	mb.cancel, mb.following = cancel, true
	mb.seq++
	seq, topic, b, gen, id := mb.seq, mb.topic, mb.e.b, mb.e.gen, mb.id
	opts := mb.opts
	opts.From, opts.To, opts.Follow, opts.Limit = broker.Position{Kind: broker.Latest}, nil, true, 0
	return func() tea.Msg {
		ch, err := b.Peek(ctx, topic, opts)
		return routedMsg{gen: gen, to: id, msg: streamStartedMsg{seq: seq, ch: ch, err: err}}
	}
}

func (mb *messageBrowser) next(ch <-chan broker.Message) tea.Cmd {
	seq, gen, id := mb.seq, mb.e.gen, mb.id
	return func() tea.Msg {
		m, ok := <-ch
		return routedMsg{gen: gen, to: id, msg: streamMsg{seq: seq, m: m, ok: ok, ch: ch}}
	}
}

// Update implements panel.
func (mb *messageBrowser) Update(msg tea.Msg) (panel, tea.Cmd) {
	if ok, cmd, reload, _ := handleAction(mb.e, mb.id, msg); ok {
		if reload && !mb.following {
			return mb, tea.Batch(cmd, mb.load())
		}
		return mb, cmd
	}
	switch m := msg.(type) {
	case messagesMsg:
		mb.loading, mb.err = false, m.err
		mb.setMessages(m.msgs)
	case filterMsg:
		mb.filter, mb.opts = m.v, m.opts
		return mb, mb.load()
	case streamStartedMsg:
		if m.seq != mb.seq || !mb.following {
			return mb, nil
		}
		if m.err != nil {
			mb.close()
			return mb, statusErr(fmt.Errorf("follow %s: %w", mb.topic, m.err))
		}
		return mb, tea.Batch(statusInfo("following "+mb.topic), mb.next(m.ch))
	case streamMsg:
		return mb, mb.streamed(m)
	case tea.KeyMsg:
		return mb, mb.key(m)
	}
	return mb, nil
}

func (mb *messageBrowser) streamed(m streamMsg) tea.Cmd {
	if m.seq != mb.seq || !mb.following {
		return nil
	}
	if !m.ok {
		mb.close()
		return statusInfo("follow ended")
	}
	if m.m.Err != nil {
		mb.close()
		return statusErr(fmt.Errorf("follow %s: %w", mb.topic, m.m.Err))
	}
	atEnd := mb.t.cursor >= len(mb.t.visible)-1
	mb.msgs = append(mb.msgs, m.m)
	if len(mb.msgs) > maxFollowBuffer {
		mb.msgs = mb.msgs[len(mb.msgs)-maxFollowBuffer:]
	}
	mb.setMessages(mb.msgs)
	if atEnd {
		mb.t.cursor = max(0, len(mb.t.visible)-1)
	}
	return mb.next(m.ch)
}

func (mb *messageBrowser) setMessages(msgs []broker.Message) {
	mb.msgs = msgs
	rows := make([][]string, len(msgs))
	for i, m := range msgs {
		ts := ""
		if !m.Timestamp.IsZero() {
			ts = m.Timestamp.Format("2006-01-02 15:04:05")
		}
		if mb.e.kind() == "queue" {
			rows[i] = []string{itoa(i + 1), orDefault(m.Exchange), m.RoutingKey, ts, itoa(len(m.Value)), yesNo(m.Redelivered)}
		} else {
			rows[i] = []string{itoa(i + 1), itoa(m.Partition), itoa(m.Offset), printable(m.Key), ts, itoa(len(m.Value))}
		}
	}
	mb.t.setRows(rows)
	mb.scroll = 0
}

func (mb *messageBrowser) key(m tea.KeyMsg) tea.Cmd {
	if used, cmd := mb.t.updateNav(m); used {
		mb.scroll = 0
		return cmd
	}
	switch {
	case key.Matches(m, mb.keys.refresh):
		return mb.load()
	case key.Matches(m, mb.keys.filter):
		return mb.openFilter()
	case key.Matches(m, mb.keys.follow):
		if mb.following {
			mb.close()
			return statusInfo("follow stopped")
		}
		return mb.startFollow()
	case key.Matches(m, mb.keys.raw):
		mb.raw = !mb.raw
	case key.Matches(m, mb.keys.scrollUp):
		mb.scroll = max(0, mb.scroll-5)
	case key.Matches(m, mb.keys.scrollDown):
		mb.scroll += 5
	case mb.publish.applies(mb.e, nil) && key.Matches(m, mb.publish.key):
		return startAction(mb.e, mb.id, mb.publish, nil)
	}
	return nil
}

func (mb *messageBrowser) openFilter() tea.Cmd {
	f := mb.filter
	fields := []field{
		{key: "key", label: "Key regex", value: f["key"]},
		{key: "value", label: "Value regex", value: f["value"]},
		{key: "header", label: "Header", hint: "name or name=regex", value: f["header"]},
		{key: "from", label: "From", hint: positionHint, value: f["from"]},
	}
	if mb.e.kafkaLike() {
		fields = append(fields, field{key: "partitions", label: "Partitions", hint: "0,2 (empty = all)", value: f["partitions"]})
	}
	fields = append(fields, field{key: "limit", label: "Limit", hint: "0 = no limit", value: f["limit"]})
	now := mb.e.now
	return openOverlay(newForm("Filter "+mb.topic, fields, func(v values) (tea.Cmd, error) {
		opts, err := peekOptions(v, now())
		if err != nil {
			return nil, err
		}
		return mb.e.send(mb.id, filterMsg{v: v, opts: opts}), nil
	}))
}

// peekOptions builds PeekOptions from the filter form.
func peekOptions(v values, now time.Time) (broker.PeekOptions, error) {
	var opts broker.PeekOptions
	var err error
	if s := v["key"]; s != "" {
		if opts.Filter.Key, err = regexp.Compile(s); err != nil {
			return opts, fmt.Errorf("key regex: %w", err)
		}
	}
	if s := v["value"]; s != "" {
		if opts.Filter.Value, err = regexp.Compile(s); err != nil {
			return opts, fmt.Errorf("value regex: %w", err)
		}
	}
	if s := v["header"]; s != "" {
		hm, err := broker.ParseHeaderMatch(s)
		if err != nil {
			return opts, err
		}
		opts.Filter.Headers = []broker.HeaderMatch{hm}
	}
	if opts.From, err = parsePosition(v["from"], now); err != nil {
		return opts, err
	}
	if opts.Partitions, err = parsePartitions(v["partitions"]); err != nil {
		return opts, err
	}
	if s := strings.TrimSpace(v["limit"]); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 0 {
			return opts, fmt.Errorf("invalid limit %q", s)
		}
		opts.Limit = n
	}
	return opts, nil
}

// selected returns the message under the cursor.
func (mb *messageBrowser) selected() (broker.Message, bool) {
	i := mb.t.selected()
	if i < 0 || i >= len(mb.msgs) {
		return broker.Message{}, false
	}
	return mb.msgs[i], true
}

// Keys implements panel.
func (mb *messageBrowser) Keys() []key.Binding {
	keys := []key.Binding{mb.t.keys.up, mb.t.keys.down, mb.keys.refresh, mb.keys.filter, mb.keys.follow, mb.keys.raw,
		mb.keys.scrollDown, mb.keys.scrollUp}
	if mb.publish.applies(mb.e, nil) {
		keys = append(keys, mb.publish.key)
	}
	return keys
}

// View implements panel.
func (mb *messageBrowser) View(width, height int) string {
	head := st.title.Render(mb.e.kind()+" "+mb.topic) + st.muted.Render(fmt.Sprintf("  %d messages", len(mb.msgs)))
	if s := filterSummary(mb.filter); s != "" {
		head += st.muted.Render("  filter: " + s)
	}
	if mb.following {
		head += " " + st.ok.Render("● following")
	}
	if mb.loading {
		head += " " + st.muted.Render(mb.e.spin+" loading…")
	}
	lines := []string{truncate(head, width)}
	if mb.err != nil {
		lines = append(lines, st.err.Render(truncate("error: "+mb.err.Error(), width)))
	}
	listH := max(4, (height-len(lines))*2/5)
	lines = append(lines, mb.t.view(width, listH))
	viewH := height - len(lines) - listH
	m, ok := mb.selected()
	if !ok || viewH < 2 {
		return strings.Join(lines, "\n")
	}
	payload, format := renderPayload(m.Value, mb.raw)
	label := "Payload (" + format + ")"
	if mb.raw {
		label += " raw"
	}
	body := describeMessage(m) + "\n" + st.header.Render(label) + "\n" + payload
	bl := strings.Split(body, "\n")
	mb.scroll = min(mb.scroll, max(0, len(bl)-1))
	body = strings.Join(bl[mb.scroll:], "\n")
	sep := st.muted.Render(strings.Repeat("─", width))
	return strings.Join(lines, "\n") + "\n" + sep + "\n" + clipLines(body, width, viewH-1)
}

func filterSummary(v values) string {
	var parts []string
	for _, k := range []string{"key", "value", "header", "from", "partitions", "limit"} {
		if v[k] != "" {
			parts = append(parts, k+"="+v[k])
		}
	}
	return strings.Join(parts, " ")
}
