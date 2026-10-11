package cli

import (
	"io"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/spf13/cobra"

	"github.com/Max2535/mqx/internal/assistant"
	"github.com/Max2535/mqx/internal/mcpserver"
)

func newMCPCmd(o *options) *cobra.Command {
	var allowPayloads bool
	cmd := &cobra.Command{
		Use:   "mcp",
		Short: "Serve read-only broker tools to MCP clients over stdio",
		Long: `Serve one context's read-only broker tools over the Model Context Protocol
(stdio), so MCP clients such as Claude Code, Claude Desktop or Cursor can list
topics, inspect groups and lag, run group diagnose and route dry-runs.

The server is bound to the context chosen with --context (or current-context)
and never changes the broker. Message keys, headers and values stay private
unless --allow-payloads adds the peek_messages tool.

Example client config (.mcp.json for Claude Code, claude_desktop_config.json
for Claude Desktop):

  {
    "mcpServers": {
      "mqx-prod": {
        "command": "mqx",
        "args": ["mcp", "--context", "prod-kafka"],
        "env": { "PROD_KAFKA_USER": "...", "PROD_KAFKA_PASS": "..." }
      }
    }
  }`,
		Example: `  mqx mcp --context local-kafka
  mqx mcp --context local-rabbit --allow-payloads
  claude mcp add mqx-local -- mqx mcp --context local-kafka`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			s, err := o.open(cmd)
			if err != nil {
				return err
			}
			defer s.Close()
			info := mcpserver.Info{Version: currentBuild().version, Context: s.cfg.Name, Broker: s.cfg.Broker, ReadOnly: s.cfg.ReadOnly}
			tools := assistant.Tools(s.b, assistant.Options{SendPayloads: allowPayloads})
			srv := mcpserver.New(info, tools, o.timeout)
			// stdout carries the protocol; nothing else may write to it.
			return srv.Run(cmd.Context(), &mcp.IOTransport{
				Reader: io.NopCloser(cmd.InOrStdin()),
				Writer: nopWriteCloser{cmd.OutOrStdout()},
			})
		},
	}
	cmd.Flags().BoolVar(&allowPayloads, "allow-payloads", false,
		"add peek_messages, which sends message keys, headers and values to the client")
	return cmd
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }
