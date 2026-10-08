package cli

import "github.com/spf13/cobra"

func newPolicyCmd(*options) *cobra.Command { return pending("policy") }

func newShovelCmd(*options) *cobra.Command { return pending("shovel") }

func newFederationCmd(*options) *cobra.Command { return pending("federation") }
