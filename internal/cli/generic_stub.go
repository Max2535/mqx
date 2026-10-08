package cli

import "github.com/spf13/cobra"

func newPingCmd(*options) *cobra.Command { return pending("ping") }

func newTopicsCmd(*options) *cobra.Command { return pending("topics") }

func newTopicCmd(*options) *cobra.Command { return pending("topic") }

func newPeekCmd(*options) *cobra.Command { return pending("peek") }

func newPublishCmd(*options) *cobra.Command { return pending("publish") }

func newNodesCmd(*options) *cobra.Command { return pending("nodes") }

func newMetricsCmd(*options) *cobra.Command { return pending("metrics") }

func newConsumersCmd(*options) *cobra.Command { return pending("consumers") }

func newPurgeCmd(*options) *cobra.Command { return pending("purge") }
