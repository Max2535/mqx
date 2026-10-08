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

	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/config"
)

func newCtxCmd(opts *options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ctx",
		Short: "Manage named broker contexts",
	}
	cmd.AddCommand(newCtxListCmd(opts), newCtxUseCmd(opts), newCtxDescribeCmd(opts))
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
			if err := cfg.Validate(broker.Validator()); err != nil {
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
			if err := cfg.Validate(broker.Validator()); err != nil {
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
		shown := make([]string, len(c.Brokers))
		for i, b := range c.Brokers {
			shown[i] = b
			if strings.Contains(b, "@") {
				shown[i] = "<redacted>"
			}
		}
		return strings.Join(shown, ",")
	}
	if c.URL == "" {
		return ""
	}
	u, err := url.Parse(c.URL)
	if err != nil || u.Host == "" {
		return "<invalid url>"
	}
	return u.Redacted()
}

func newCtxDescribeCmd(opts *options) *cobra.Command {
	var offline bool
	cmd := &cobra.Command{
		Use:   "describe [name]",
		Short: "Show a context, check connectivity and list what its broker supports",
		Example: `  mqx ctx describe
  mqx ctx describe prod-kafka --offline`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 {
				opts.contextName = args[0]
			}
			c, err := opts.selectContext()
			if err != nil {
				return err
			}
			info := contextInfo{Name: c.Name, Broker: c.Broker, Endpoint: endpoint(c), ReadOnly: c.ReadOnly}
			for _, ep := range []struct {
				name string
				e    *config.Endpoint
			}{{"schema_registry", c.SchemaRegistry}, {"connect", c.Connect}, {"ksqldb", c.KSQLDB}} {
				if ep.e != nil {
					info.Services = append(info.Services, ep.name+"="+redactURL(ep.e.URL))
				}
			}
			if !offline {
				info.Reachable, info.Capabilities, info.Error = probe(cmd, opts)
			}
			return opts.render(cmd, info, func() *table {
				t := newTable("FIELD", "VALUE")
				t.add("name", info.Name)
				t.add("broker", info.Broker)
				t.add("endpoint", info.Endpoint)
				t.add("read_only", info.ReadOnly)
				t.add("services", info.Services)
				if !offline {
					t.add("reachable", info.Reachable)
					if info.Error != "" {
						t.add("error", info.Error)
					}
					t.add("capabilities", info.Capabilities)
				}
				return t
			})
		},
	}
	cmd.Flags().BoolVar(&offline, "offline", false, "do not connect to the broker")
	return cmd
}

type contextInfo struct {
	Name         string   `json:"name"`
	Broker       string   `json:"broker"`
	Endpoint     string   `json:"endpoint"`
	ReadOnly     bool     `json:"read_only"`
	Services     []string `json:"services,omitempty"`
	Reachable    bool     `json:"reachable"`
	Capabilities []string `json:"capabilities,omitempty"`
	Error        string   `json:"error,omitempty"`
}

// probe connects, pings and lists capabilities; failures are reported, not returned.
func probe(cmd *cobra.Command, opts *options) (bool, []string, string) {
	s, err := opts.open(cmd)
	if err != nil {
		return false, nil, err.Error()
	}
	defer s.Close()
	ctx, cancel := opts.requestContext(cmd)
	defer cancel()
	if err := s.b.Ping(ctx); err != nil {
		return false, broker.Capabilities(s.b), err.Error()
	}
	return true, broker.Capabilities(s.b), ""
}

func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "<invalid url>"
	}
	return u.Redacted()
}
