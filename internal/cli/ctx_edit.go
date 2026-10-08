package cli

import (
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/config"
)

// fieldFlags registers one flag per editable context field, named after its
// key: tls.ca_file becomes --tls-ca-file.
type fieldFlags struct {
	strs  map[string]*string
	bools map[string]*bool
}

func flagName(key string) string { return strings.NewReplacer(".", "-", "_", "-").Replace(key) }

func addFieldFlags(cmd *cobra.Command) *fieldFlags {
	ff := &fieldFlags{strs: map[string]*string{}, bools: map[string]*bool{}}
	for _, f := range config.Fields() {
		help := f.Help
		if len(f.Choices) > 0 {
			help += " (" + strings.Join(f.Choices[1:], ", ") + ")"
		}
		if f.Kind == config.FieldBool {
			ff.bools[f.Key] = cmd.Flags().Bool(flagName(f.Key), false, help)
			continue
		}
		ff.strs[f.Key] = cmd.Flags().String(flagName(f.Key), "", help)
	}
	return ff
}

// apply sets the fields whose flags were given, refusing ones the broker does not use.
func (ff *fieldFlags) apply(cmd *cobra.Command, c *config.Context) (changed int, err error) {
	allowed := broker.ContextFields(c.Broker)
	var errs []error
	for _, f := range config.Fields() {
		name := flagName(f.Key)
		if !cmd.Flags().Changed(name) {
			continue
		}
		if !slices.ContainsFunc(allowed, func(a config.Field) bool { return a.Key == f.Key }) {
			errs = append(errs, fmt.Errorf("--%s does not apply to %s contexts", name, c.Broker))
			continue
		}
		value := ""
		if p, ok := ff.strs[f.Key]; ok {
			value = *p
		} else {
			value = strconv.FormatBool(*ff.bools[f.Key])
		}
		if err := c.Set(f.Key, value); err != nil {
			errs = append(errs, fmt.Errorf("--%s: %w", name, errors.Unwrap(err)))
			continue
		}
		changed++
	}
	return changed, errors.Join(errs...)
}

// loadForEdit loads the config without validating it first, so an edit can fix
// it; Update validates the result. A missing file is an empty config when allowMissing.
func (o *options) loadForEdit(allowMissing bool) (*config.Config, string, error) {
	path, err := o.path()
	if err != nil {
		return nil, "", err
	}
	cfg, err := config.Load(path)
	if errors.Is(err, fs.ErrNotExist) && allowMissing {
		return &config.Config{}, path, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return nil, "", fmt.Errorf("no config at %s; add a context with `mqx ctx add` or pass --config: %w", path, err)
	}
	return cfg, path, err
}

func newCtxAddCmd(opts *options) *cobra.Command {
	var brokerType string
	var use bool
	var ff *fieldFlags
	cmd := &cobra.Command{
		Use:   "add <name> --broker <type>",
		Short: "Add a context to the config file",
		Long: `Add a context to the config file, creating the file if needed.

Credentials are never stored: --username-env and --password-env name the
environment variables that hold them. The file keeps its comments.`,
		Example: `  mqx ctx add local --broker kafka --brokers localhost:9092
  mqx ctx add prod --broker kafka --brokers k1:9093,k2:9093 --sasl-mechanism SCRAM-SHA-512 \
    --username-env PROD_USER --password-env PROD_PASS --tls-enabled --tls-ca-file /etc/ssl/ca.pem --read-only
  mqx ctx add dev-rabbit --broker rabbitmq --url amqp://dev:5672/ --management-url http://dev:15672 --use`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if !slices.Contains(broker.Types(), brokerType) {
				return fmt.Errorf("--broker must be one of %s", strings.Join(broker.Types(), ", "))
			}
			cfg, path, err := opts.loadForEdit(true)
			if err != nil {
				return err
			}
			c := config.Context{Name: args[0], Broker: brokerType}
			if _, err := ff.apply(cmd, &c); err != nil {
				return err
			}
			err = cfg.Update(path, broker.Validator(), func(cfg *config.Config) error {
				if err := cfg.Add(c); err != nil {
					return err
				}
				if use || cfg.CurrentContext == "" {
					cfg.CurrentContext = c.Name
				}
				return nil
			})
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Added context %q to %s.", c.Name, path)
			if cfg.CurrentContext == c.Name {
				fmt.Fprint(cmd.OutOrStdout(), " It is the current context.")
			}
			fmt.Fprintf(cmd.OutOrStdout(), "\nCheck it with: mqx ctx describe %s\n", c.Name)
			return nil
		},
	}
	cmd.Flags().StringVar(&brokerType, "broker", "", "broker type (required): "+strings.Join(broker.Types(), ", "))
	cmd.Flags().BoolVar(&use, "use", false, "make it the current context")
	_ = cmd.MarkFlagRequired("broker")
	ff = addFieldFlags(cmd)
	return cmd
}

func newCtxSetCmd(opts *options) *cobra.Command {
	var rename string
	var ff *fieldFlags
	cmd := &cobra.Command{
		Use:   "set <name>",
		Short: "Change fields of a context; only the flags given change",
		Example: `  mqx ctx set prod --read-only=false
  mqx ctx set prod --brokers k3:9093 --tls-ca-file ""   # an empty value clears a field
  mqx ctx set old-name --name new-name`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, path, err := opts.loadForEdit(false)
			if err != nil {
				return err
			}
			old, err := cfg.Find(args[0])
			if err != nil {
				return err
			}
			c := old.Clone()
			changed, err := ff.apply(cmd, &c)
			if err != nil {
				return err
			}
			if cmd.Flags().Changed("name") {
				c.Name = rename
				changed++
			}
			if changed == 0 {
				return errors.New("nothing to change; pass at least one field flag (see --help)")
			}
			if err := cfg.Update(path, broker.Validator(), func(cfg *config.Config) error {
				return cfg.Replace(old.Name, c)
			}); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Updated context %q in %s.\n", c.Name, path)
			return nil
		},
	}
	cmd.Flags().StringVar(&rename, "name", "", "rename the context")
	ff = addFieldFlags(cmd)
	return cmd
}

func newCtxDeleteCmd(opts *options) *cobra.Command {
	return &cobra.Command{
		Use:     "delete <name>",
		Aliases: []string{"rm"},
		Short:   "Remove a context from the config file (the broker is not touched)",
		Example: `  mqx ctx delete old-kafka --yes`,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			cfg, path, err := opts.loadForEdit(false)
			if err != nil {
				return err
			}
			if _, err := cfg.Find(name); err != nil {
				return err
			}
			if err := opts.confirm(cmd, "delete context", fmt.Sprintf("delete context %q", name),
				fmt.Sprintf("About to delete context %q from %s.", name, path)); err != nil {
				return err
			}
			wasCurrent := cfg.CurrentContext == name
			if err := cfg.Update(path, broker.Validator(), func(cfg *config.Config) error {
				return cfg.Remove(name)
			}); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Deleted context %q from %s.\n", name, path)
			if wasCurrent {
				fmt.Fprintln(cmd.OutOrStdout(), "It was the current context; pick another with: mqx ctx use <name>")
			}
			return nil
		},
	}
}
