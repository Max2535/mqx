// Package cli implements mqx's cobra command tree.
package cli

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/Max2535/mqx/internal/config"
)

// options carries root flags to subcommands without package-level state.
type options struct {
	configPath string
}

// path returns --config if set, else $MQX_CONFIG, else ~/.config/mqx/config.yaml.
func (o *options) path() (string, error) {
	if o.configPath != "" {
		return o.configPath, nil
	}
	return config.DefaultPath()
}

// NewRootCmd builds a fresh mqx command tree.
func NewRootCmd() *cobra.Command {
	opts := &options{}
	root := &cobra.Command{
		Use:   "mqx",
		Short: "Inspect, peek and publish messages across brokers",
		// Cobra validates args before this hook, so usage still prints for
		// argument errors but not for runtime errors.
		PersistentPreRun: func(cmd *cobra.Command, _ []string) { cmd.SilenceUsage = true },
	}
	root.PersistentFlags().StringVar(&opts.configPath, "config", "",
		"config file (default $MQX_CONFIG, then ~/.config/mqx/config.yaml)")
	root.AddCommand(newVersionCmd())
	return root
}

// Execute runs mqx and returns the process exit code. Cobra prints errors to stderr.
func Execute(ctx context.Context) int {
	if err := NewRootCmd().ExecuteContext(ctx); err != nil {
		return 1
	}
	return 0
}
