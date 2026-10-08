package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/Max2535/mqx/internal/broker"
)

func newUserCmd(o *options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "user",
		Short: "Manage RabbitMQ users",
	}
	cmd.AddCommand(newUserListCmd(o), newUserCreateCmd(o), newUserDeleteCmd(o), newUserSetTagsCmd(o))
	return cmd
}

func newUserListCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Short:   "List users and their tags",
		Example: `  mqx user list -o json`,
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				ua, err := capability[broker.UserAdmin](s, broker.CapUserAdmin)
				if err != nil {
					return err
				}
				us, err := ua.Users(ctx)
				if err != nil {
					return err
				}
				return o.render(cmd, nonNilList(us), func() *table {
					t := newTable("NAME", "TAGS")
					for _, u := range us {
						t.add(u.Name, u.Tags)
					}
					return t
				})
			})
		},
	}
}

func newUserCreateCmd(o *options) *cobra.Command {
	var (
		passwordEnv string
		tags        []string
	)
	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a user (or reset its password and tags)",
		Long: `Create a user, or reset an existing user's password and tags. The password
is read from the environment variable named by --password-env, or prompted for
without echo on a terminal. It is never accepted as a flag value.`,
		Example: `  APP_PW=... mqx user create billing-app --password-env APP_PW --tags monitoring
  mqx user create alice --tags administrator   # prompts for the password`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, pos []string) error {
			name := pos[0]
			password, err := o.readPassword(cmd, name, passwordEnv)
			if err != nil {
				return err
			}
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				ua, err := capability[broker.UserAdmin](s, broker.CapUserAdmin)
				if err != nil {
					return err
				}
				if err := s.guard(cmd, fmt.Sprintf("create or update user %q", name)); err != nil {
					return err
				}
				if err := ua.PutUser(ctx, broker.User{Name: name, Tags: nonNilList(tags)}, password); err != nil {
					return err
				}
				return o.done(cmd, "created user %q", name)
			})
		},
	}
	cmd.Flags().StringVar(&passwordEnv, "password-env", "", "environment variable holding the password")
	cmd.Flags().StringSliceVar(&tags, "tags", nil, "comma-separated tags, e.g. administrator, monitoring, management")
	return cmd
}

// readPassword reads a password from the named env var or, on a terminal, a prompt.
func (o *options) readPassword(cmd *cobra.Command, user, env string) (string, error) {
	if env != "" {
		v, ok := os.LookupEnv(env)
		if !ok || v == "" {
			return "", fmt.Errorf("--password-env: environment variable %s is not set or empty", env)
		}
		return v, nil
	}
	if !o.isTerminal() {
		return "", errors.New("pass --password-env VAR naming an environment variable that holds the password " +
			"(passwords are never taken as flag values)")
	}
	fmt.Fprintf(cmd.ErrOrStderr(), "Password for user %q: ", user)
	var pw string
	in := cmd.InOrStdin()
	if f, ok := in.(*os.File); ok && term.IsTerminal(int(f.Fd())) {
		b, err := term.ReadPassword(int(f.Fd()))
		fmt.Fprintln(cmd.ErrOrStderr())
		if err != nil {
			return "", fmt.Errorf("read password: %w", err)
		}
		pw = string(b)
	} else {
		line, err := readLine(in)
		if err != nil {
			return "", fmt.Errorf("read password: %w", err)
		}
		pw = line
	}
	if pw == "" {
		return "", errors.New("empty password; the user would not be able to log in with a password")
	}
	return pw, nil
}

// readLine reads one line byte by byte, so later readers of r see the rest.
func readLine(r io.Reader) (string, error) {
	var b strings.Builder
	buf := make([]byte, 1)
	for {
		n, err := r.Read(buf)
		if n == 1 {
			if buf[0] == '\n' {
				break
			}
			b.WriteByte(buf[0])
		}
		if errors.Is(err, io.EOF) {
			if b.Len() == 0 {
				return "", io.ErrUnexpectedEOF
			}
			break
		}
		if err != nil {
			return "", err
		}
	}
	return strings.TrimRight(b.String(), "\r"), nil
}

func newUserDeleteCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:     "delete <name>",
		Short:   "Delete a user",
		Example: `  mqx user delete billing-app --yes`,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, pos []string) error {
			name := pos[0]
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				ua, err := capability[broker.UserAdmin](s, broker.CapUserAdmin)
				if err != nil {
					return err
				}
				if err := s.guard(cmd, fmt.Sprintf("delete user %q", name)); err != nil {
					return err
				}
				if err := ua.DeleteUser(ctx, name); err != nil {
					return err
				}
				return o.done(cmd, "deleted user %q", name)
			})
		},
	}
}

func newUserSetTagsCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:   "set-tags <name> [tag...]",
		Short: "Replace a user's tags, keeping its password (no tags clears them)",
		Example: `  mqx user set-tags alice administrator
  mqx user set-tags billing-app   # clear tags`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, pos []string) error {
			u := broker.User{Name: pos[0], Tags: append([]string{}, pos[1:]...)}
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				ua, err := capability[broker.UserAdmin](s, broker.CapUserAdmin)
				if err != nil {
					return err
				}
				if err := s.guard(cmd, fmt.Sprintf("set tags of user %q to [%s]", u.Name, strings.Join(u.Tags, ","))); err != nil {
					return err
				}
				if err := ua.PutUser(ctx, u, ""); err != nil {
					return err
				}
				return o.done(cmd, "set tags of user %q to [%s]", u.Name, strings.Join(u.Tags, ","))
			})
		},
	}
}

func newPermissionCmd(o *options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "permission",
		Short: "Manage users' permissions on virtual hosts",
	}
	cmd.AddCommand(newPermissionListCmd(o), newPermissionSetCmd(o), newPermissionClearCmd(o))
	return cmd
}

func newPermissionListCmd(o *options) *cobra.Command {
	var vhost, user string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List permissions",
		Example: `  mqx permission list
  mqx permission list --vhost / --user alice`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				ua, err := capability[broker.UserAdmin](s, broker.CapUserAdmin)
				if err != nil {
					return err
				}
				all, err := ua.Permissions(ctx)
				if err != nil {
					return err
				}
				ps := []broker.Permission{}
				for _, p := range all {
					if (vhost == "" || p.VHost == vhost) && (user == "" || p.User == user) {
						ps = append(ps, p)
					}
				}
				return o.render(cmd, ps, func() *table {
					t := newTable("USER", "VHOST", "CONFIGURE", "WRITE", "READ")
					for _, p := range ps {
						t.add(p.User, p.VHost, quoteRegex(p.Configure), quoteRegex(p.Write), quoteRegex(p.Read))
					}
					return t
				})
			})
		},
	}
	cmd.Flags().StringVar(&vhost, "vhost", "", "only this vhost")
	cmd.Flags().StringVar(&user, "user", "", "only this user")
	return cmd
}

// quoteRegex shows an empty permission regex (no access) visibly.
func quoteRegex(re string) string {
	if re == "" {
		return `""`
	}
	return re
}

func newPermissionSetCmd(o *options) *cobra.Command {
	var p broker.Permission
	cmd := &cobra.Command{
		Use:   "set <user>",
		Short: "Set a user's configure/write/read permissions on a vhost",
		Long: `Set a user's permissions on a vhost (the context's vhost by default). Each
permission is a regex over resource names; an empty regex grants nothing.`,
		Example: `  mqx permission set alice --configure '.*' --write '.*' --read '.*'
  mqx permission set reporter --vhost prod --read '^reports\.'`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, pos []string) error {
			p.User = pos[0]
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				ua, err := capability[broker.UserAdmin](s, broker.CapUserAdmin)
				if err != nil {
					return err
				}
				if err := s.guard(cmd, fmt.Sprintf("set permissions of user %q on vhost %s", p.User, vhostLabel(p.VHost))); err != nil {
					return err
				}
				if err := ua.SetPermission(ctx, p); err != nil {
					return err
				}
				return o.done(cmd, "set permissions of user %q on vhost %s", p.User, vhostLabel(p.VHost))
			})
		},
	}
	f := cmd.Flags()
	f.StringVar(&p.VHost, "vhost", "", "vhost (default: the context's vhost)")
	f.StringVar(&p.Configure, "configure", "", "configure regex")
	f.StringVar(&p.Write, "write", "", "write regex")
	f.StringVar(&p.Read, "read", "", "read regex")
	return cmd
}

func vhostLabel(v string) string {
	if v == "" {
		return "of the context"
	}
	return fmt.Sprintf("%q", v)
}

func newPermissionClearCmd(o *options) *cobra.Command {
	var vhost string
	cmd := &cobra.Command{
		Use:     "clear <user>",
		Short:   "Remove a user's permissions on a vhost",
		Example: `  mqx permission clear alice --vhost / --yes`,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, pos []string) error {
			user := pos[0]
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				ua, err := capability[broker.UserAdmin](s, broker.CapUserAdmin)
				if err != nil {
					return err
				}
				if err := s.guard(cmd, fmt.Sprintf("clear permissions of user %q on vhost %s", user, vhostLabel(vhost))); err != nil {
					return err
				}
				if err := ua.ClearPermission(ctx, user, vhost); err != nil {
					return err
				}
				return o.done(cmd, "cleared permissions of user %q on vhost %s", user, vhostLabel(vhost))
			})
		},
	}
	cmd.Flags().StringVar(&vhost, "vhost", "", "vhost (default: the context's vhost)")
	return cmd
}
