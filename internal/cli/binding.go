package cli

import "github.com/spf13/cobra"

func newBindingsCmd(*options) *cobra.Command { return pending("bindings") }

func newBindCmd(*options) *cobra.Command { return pending("bind") }

func newUnbindCmd(*options) *cobra.Command { return pending("unbind") }
