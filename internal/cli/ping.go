package cli

import (
	"context"
	"time"

	"github.com/spf13/cobra"
)

func newPingCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:     "ping",
		Short:   "Check that the context's broker is reachable",
		Example: "  mqx ping --context prod-kafka",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			start := time.Now()
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				if err := s.b.Ping(ctx); err != nil {
					return err
				}
				return o.done(cmd, "%s (%s) is reachable (%s).", s.cfg.Name, s.b.Name(), time.Since(start).Round(time.Millisecond))
			})
		},
	}
}
