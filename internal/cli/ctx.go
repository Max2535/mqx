package cli

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/Max2535/mqx/internal/config"
)

func newCtxCmd(opts *options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ctx",
		Short: "Manage named broker contexts",
	}
	cmd.AddCommand(newCtxListCmd(opts), newCtxUseCmd(opts))
	return cmd
}

func newCtxListCmd(opts *options) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List contexts; * marks the current one",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			path, err := opts.path()
			if err != nil {
				return err
			}
			cfg, err := config.Load(path)
			if errors.Is(err, fs.ErrNotExist) {
				cmd.PrintErrf("no config at %s; create one (see README) or pass --config\n", path)
				return nil
			}
			if err != nil {
				return err
			}
			if err := printContexts(cmd.OutOrStdout(), cfg); err != nil {
				return err
			}
			if err := cfg.Validate(); err != nil {
				return fmt.Errorf("invalid config %s:\n%w", path, err)
			}
			return nil
		},
	}
}

func newCtxUseCmd(opts *options) *cobra.Command {
	return &cobra.Command{
		Use:   "use <name>",
		Short: "Set the current context",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			path, err := opts.path()
			if err != nil {
				return err
			}
			cfg, err := config.Load(path)
			if errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("no config at %s; create it first (see README) or pass --config: %w", path, err)
			}
			if err != nil {
				return err
			}
			if err := cfg.Validate(); err != nil {
				return fmt.Errorf("invalid config %s:\n%w", path, err)
			}
			if err := cfg.Use(args[0]); err != nil {
				return err
			}
			if err := cfg.Save(path); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Switched to context %q.\n", args[0])
			return nil
		},
	}
}

func printContexts(w io.Writer, cfg *config.Config) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "CURRENT\tNAME\tBROKER\tMODE\tENDPOINT")
	for _, c := range cfg.Contexts {
		mark := ""
		if cfg.CurrentContext != "" && c.Name == cfg.CurrentContext {
			mark = "*"
		}
		mode := "rw"
		if c.ReadOnly {
			mode = "ro"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", mark, c.Name, c.Broker, mode, endpoint(c))
	}
	return tw.Flush()
}

// endpoint shows where a context points. URL passwords are redacted, because
// list runs before validation and must never print a secret.
func endpoint(c config.Context) string {
	if len(c.Brokers) > 0 {
		return strings.Join(c.Brokers, ",")
	}
	u, err := url.Parse(c.URL)
	if err != nil || u.Host == "" {
		return "<invalid url>"
	}
	return u.Redacted()
}
