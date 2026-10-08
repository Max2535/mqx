// Package cli implements mqx's cobra command tree.
package cli

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/Max2535/mqx/internal/config"
)

// options carries root flags and process I/O to subcommands without package-level state.
type options struct {
	configPath  string
	contextName string
	output      string
	yes         bool
	timeout     time.Duration

	// isTerminal reports whether stdin and stdout are both terminals; tests replace it.
	isTerminal func() bool
	// runTUI starts the interactive UI; nil until the TUI is wired in.
	runTUI func(ctx context.Context, opts TUIOptions) error
}

// TUIOptions are the deep-link flags of `mqx tui`.
type TUIOptions struct {
	ConfigPath string
	Context    string
	Topic      string
	Group      string
}

// path returns --config if set, else $MQX_CONFIG, else ~/.config/mqx/config.yaml.
func (o *options) path() (string, error) {
	if o.configPath != "" {
		return o.configPath, nil
	}
	return config.DefaultPath()
}

func stdioIsTerminal() bool {
	return term.IsTerminal(int(os.Stdin.Fd())) && term.IsTerminal(int(os.Stdout.Fd()))
}

// Option customises the command tree; used to wire the TUI and by tests.
type Option func(*options)

// WithTUI sets the function `mqx` and `mqx tui` use to start the interactive UI.
func WithTUI(run func(ctx context.Context, opts TUIOptions) error) Option {
	return func(o *options) { o.runTUI = run }
}

// withTerminal overrides TTY detection (tests).
func withTerminal(isTTY bool) Option {
	return func(o *options) { o.isTerminal = func() bool { return isTTY } }
}

// NewRootCmd builds a fresh mqx command tree.
func NewRootCmd(opts ...Option) *cobra.Command {
	o := &options{isTerminal: stdioIsTerminal}
	for _, opt := range opts {
		opt(o)
	}
	root := &cobra.Command{
		Use:   "mqx",
		Short: "Inspect, peek, publish and administer Kafka and RabbitMQ from one CLI/TUI",
		Long: `mqx inspects, peeks, publishes and administers message brokers through one interface.

Run bare "mqx" in a terminal to open the TUI. Every command takes --context to
pick a named context from the config file; mutating commands refuse read_only
contexts and ask for confirmation (or --yes).`,
		Args: cobra.NoArgs,
		// Cobra validates args before this hook, so usage still prints for
		// argument errors but not for runtime errors.
		PersistentPreRun: func(cmd *cobra.Command, _ []string) { cmd.SilenceUsage = true },
		RunE: func(cmd *cobra.Command, _ []string) error {
			// Bare mqx: TUI on a terminal, help otherwise, so scripts never block.
			if o.runTUI != nil && o.isTerminal() {
				return o.runTUI(cmd.Context(), TUIOptions{ConfigPath: o.configPath, Context: o.contextName})
			}
			return cmd.Help()
		},
	}
	pf := root.PersistentFlags()
	pf.StringVar(&o.configPath, "config", "", "config file (default $MQX_CONFIG, then ~/.config/mqx/config.yaml)")
	pf.StringVarP(&o.contextName, "context", "c", "", "context to use instead of current-context")
	pf.StringVarP(&o.output, "output", "o", "table", "output format: table or json")
	pf.BoolVarP(&o.yes, "yes", "y", false, "confirm mutating commands without prompting")
	pf.DurationVar(&o.timeout, "timeout", 30*time.Second, "timeout for each broker request (0 = none)")

	root.AddGroup(
		&cobra.Group{ID: groupBrowse, Title: "Browse and messages:"},
		&cobra.Group{ID: groupConsumers, Title: "Consumers:"},
		&cobra.Group{ID: groupAdmin, Title: "Administration:"},
		&cobra.Group{ID: groupKafka, Title: "Kafka ecosystem:"},
		&cobra.Group{ID: groupRabbit, Title: "RabbitMQ:"},
	)
	add := func(group string, cmds ...*cobra.Command) {
		for _, c := range cmds {
			c.GroupID = group
			root.AddCommand(c)
		}
	}
	root.AddCommand(newVersionCmd(), newCtxCmd(o), newTUICmd(o))
	add(groupBrowse, newPingCmd(o), newTopicsCmd(o), newTopicCmd(o), newPeekCmd(o), newPublishCmd(o),
		newNodesCmd(o), newMetricsCmd(o))
	add(groupConsumers, newConsumersCmd(o), newGroupsCmd(o), newGroupCmd(o), newConnectionsCmd(o), newChannelsCmd(o))
	add(groupAdmin, newPurgeCmd(o))
	add(groupKafka, newSchemaCmd(o), newConnectCmd(o), newKSQLCmd(o), newACLCmd(o))
	add(groupRabbit, newExchangesCmd(o), newExchangeCmd(o), newBindingsCmd(o), newBindCmd(o), newUnbindCmd(o),
		newRouteCmd(o), newVHostsCmd(o), newVHostCmd(o), newUserCmd(o), newPermissionCmd(o), newPolicyCmd(o),
		newShovelCmd(o), newFederationCmd(o))
	return root
}

const (
	groupBrowse    = "browse"
	groupConsumers = "consumers"
	groupAdmin     = "admin"
	groupKafka     = "kafka"
	groupRabbit    = "rabbit"
)

// Execute runs mqx and returns the process exit code. Cobra prints errors to stderr.
func Execute(ctx context.Context, opts ...Option) int {
	if err := NewRootCmd(opts...).ExecuteContext(ctx); err != nil {
		return 1
	}
	return 0
}

func newTUICmd(o *options) *cobra.Command {
	var t TUIOptions
	cmd := &cobra.Command{
		Use:   "tui",
		Short: "Open the interactive terminal UI (--context, --topic, --group deep-link)",
		Example: `  mqx tui
  mqx tui --context prod-kafka --topic orders --group billing`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if o.runTUI == nil {
				return errNoTUI
			}
			t.ConfigPath, t.Context = o.configPath, o.contextName
			return o.runTUI(cmd.Context(), t)
		},
	}
	cmd.Flags().StringVar(&t.Topic, "topic", "", "open this topic or queue")
	cmd.Flags().StringVar(&t.Group, "group", "", "open this consumer group")
	return cmd
}

// stdin returns the command's input, for publish and confirmation prompts.
func stdin(cmd *cobra.Command) io.Reader { return cmd.InOrStdin() }
