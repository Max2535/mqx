// Package tui implements mqx's interactive terminal UI with Bubble Tea.
//
// The root model shows a header, a navigation list of panels, the active
// panel and a footer. Panels are built from the broker's capabilities, so
// only supported features appear. All broker I/O runs in tea.Cmds with a
// timeout, and every mutating action passes one guard (env.mutate) that hides
// it on read_only contexts and asks for confirmation elsewhere.
package tui

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/config"
)

// Options are the TUI's entry flags.
type Options struct {
	ConfigPath string // --config; "" = $MQX_CONFIG, then the default path
	Context    string // open this context instead of current-context
	Topic      string // open this topic's message browser
	Group      string // open this consumer group's detail
}

// requestTimeout bounds each broker request made by the TUI.
const requestTimeout = 30 * time.Second

// Run loads the config, opens the selected context and runs the full-screen
// UI until the user quits or ctx ends.
func Run(ctx context.Context, opts Options) error {
	cfg, err := loadConfig(opts.ConfigPath)
	if err != nil {
		return err
	}
	a := newApp(startConfig(ctx, cfg, opts))
	a.animate = true
	a.tick = func(d time.Duration, msg tea.Msg) tea.Cmd {
		return tea.Tick(d, func(time.Time) tea.Msg { return msg })
	}
	final, err := tea.NewProgram(a, tea.WithAltScreen(), tea.WithContext(ctx)).Run()
	if fa, ok := final.(*app); ok {
		fa.shutdown()
	} else {
		a.shutdown()
	}
	if err != nil && ctx.Err() != nil {
		return nil // cancelled (e.g. SIGINT): a clean exit
	}
	if err != nil {
		return fmt.Errorf("tui: %w", err)
	}
	return nil
}

// startConfig picks the context to open: --context when it exists, else
// current-context. An unknown --context is reported in the status bar.
func startConfig(ctx context.Context, cfg *config.Config, opts Options) appConfig {
	c := appConfig{
		cfg:     cfg,
		initial: cfg.CurrentContext,
		links:   deepLinks{topic: opts.Topic, group: opts.Group},
		open:    broker.Open,
		base:    ctx,
		timeout: requestTimeout,
		now:     time.Now,
	}
	if opts.Context != "" {
		if _, err := cfg.Find(opts.Context); err != nil {
			c.status = statusMsg{text: err.Error(), err: true}
			c.links = deepLinks{}
		} else {
			c.initial = opts.Context
		}
	}
	if c.initial != "" {
		if _, err := cfg.Find(c.initial); err != nil {
			c.status = statusMsg{text: fmt.Sprintf("current-context: %v", err), err: true}
			c.initial = ""
		}
	}
	return c
}

// loadConfig loads and validates the config like the CLI does.
func loadConfig(path string) (*config.Config, error) {
	if path == "" {
		p, err := config.DefaultPath()
		if err != nil {
			return nil, err
		}
		path = p
	}
	cfg, err := config.Load(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("no config at %s; create it (see README) or pass --config: %w", path, err)
	}
	if err != nil {
		return nil, err
	}
	if err := cfg.Validate(broker.Validator()); err != nil {
		return nil, fmt.Errorf("invalid config %s:\n%w", path, err)
	}
	return cfg, nil
}
