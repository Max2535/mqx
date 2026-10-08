package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"

	"github.com/spf13/cobra"

	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/config"
)

var errNoTUI = errors.New("this build has no TUI")

// loadConfig loads and validates the config file.
func (o *options) loadConfig() (*config.Config, string, error) {
	path, err := o.path()
	if err != nil {
		return nil, "", err
	}
	cfg, err := config.Load(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, path, fmt.Errorf("no config at %s; create it (see README) or pass --config: %w", path, err)
	}
	if err != nil {
		return nil, path, err
	}
	if err := cfg.Validate(broker.Validator()); err != nil {
		return nil, path, fmt.Errorf("invalid config %s:\n%w", path, err)
	}
	return cfg, path, nil
}

// selectContext returns --context, else current-context.
func (o *options) selectContext() (config.Context, error) {
	cfg, path, err := o.loadConfig()
	if err != nil {
		return config.Context{}, err
	}
	name := o.contextName
	if name == "" {
		name = cfg.CurrentContext
	}
	if name == "" {
		return config.Context{}, fmt.Errorf("no context selected in %s; pass --context or run `mqx ctx use <name>`", path)
	}
	return cfg.Find(name)
}

// session is an open connection to the selected context's broker.
type session struct {
	cfg config.Context
	b   broker.Broker
	o   *options
}

// open connects to the selected context. Callers must Close the session.
func (o *options) open(cmd *cobra.Command) (*session, error) {
	c, err := o.selectContext()
	if err != nil {
		return nil, err
	}
	ctx, cancel := o.requestContext(cmd)
	defer cancel()
	b, err := broker.Open(ctx, c)
	if err != nil {
		return nil, err
	}
	return &session{cfg: c, b: b, o: o}, nil
}

func (s *session) Close() error { return s.b.Close() }

// requestContext bounds one broker request by --timeout.
func (o *options) requestContext(cmd *cobra.Command) (context.Context, context.CancelFunc) {
	if o.timeout <= 0 {
		return context.WithCancel(cmd.Context())
	}
	return context.WithTimeout(cmd.Context(), o.timeout)
}

// withSession opens a session, runs fn with a request-bounded context and closes it.
func (o *options) withSession(cmd *cobra.Command, fn func(ctx context.Context, s *session) error) error {
	s, err := o.open(cmd)
	if err != nil {
		return err
	}
	defer s.Close()
	ctx, cancel := o.requestContext(cmd)
	defer cancel()
	return fn(ctx, s)
}

// capability returns the session's broker as capability T or an actionable error.
func capability[T any](s *session, name string) (T, error) {
	c, err := broker.As[T](s.b, name)
	if err != nil {
		var zero T
		return zero, fmt.Errorf("context %q (%s) does not support %s; run `mqx ctx describe` to list what it supports: %w",
			s.cfg.Name, s.b.Name(), name, broker.ErrUnsupported)
	}
	return c, nil
}

// ErrReadOnly is returned for a mutating command on a read_only context.
var ErrReadOnly = errors.New("context is read_only")

// ErrNotConfirmed is returned when a mutating command was not confirmed.
var ErrNotConfirmed = errors.New("not confirmed")

// guard is the single gate every mutating command passes. It refuses read_only
// contexts and asks for confirmation unless --yes was given. Adapters never
// check read_only themselves.
func (s *session) guard(cmd *cobra.Command, action string) error {
	return s.o.guard(cmd, s.cfg, action)
}

func (o *options) guard(cmd *cobra.Command, c config.Context, action string) error {
	if c.ReadOnly {
		return fmt.Errorf("refusing to %s: context %q is read_only; use a writable context with --context: %w",
			action, c.Name, ErrReadOnly)
	}
	if o.yes {
		return nil
	}
	if !o.isTerminal() {
		return fmt.Errorf("refusing to %s on context %q without confirmation; re-run with --yes: %w",
			action, c.Name, ErrNotConfirmed)
	}
	fmt.Fprintf(cmd.ErrOrStderr(), "About to %s on context %q (%s). Continue? [y/N] ", action, c.Name, c.Broker)
	line, err := bufio.NewReader(stdin(cmd)).ReadString('\n')
	if err != nil && line == "" {
		return fmt.Errorf("read confirmation: %w", ErrNotConfirmed)
	}
	switch strings.ToLower(strings.TrimSpace(line)) {
	case "y", "yes":
		return nil
	}
	return fmt.Errorf("%s: %w", action, ErrNotConfirmed)
}
