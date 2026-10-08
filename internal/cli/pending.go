package cli

import (
	"errors"

	"github.com/spf13/cobra"
)

// pending is a placeholder for a command whose implementation is in progress.
func pending(use string) *cobra.Command {
	return &cobra.Command{
		Use:    use,
		Short:  "(not implemented yet)",
		Hidden: true,
		RunE:   func(*cobra.Command, []string) error { return errors.New(use + ": not implemented yet") },
	}
}
