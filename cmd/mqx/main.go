// Command mqx inspects, peeks, publishes and administers message brokers.
package main

import (
	"context"
	"os"
	"os/signal"

	// Each adapter registers itself; adding a broker means adding one import here.
	_ "github.com/Max2535/mqx/internal/broker/kafka"
	_ "github.com/Max2535/mqx/internal/broker/rabbitmq"
	"github.com/Max2535/mqx/internal/cli"
	"github.com/Max2535/mqx/internal/tui"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	code := cli.Execute(ctx, cli.WithTUI(runTUI))
	stop() // not deferred: os.Exit skips defers
	os.Exit(code)
}

func runTUI(ctx context.Context, o cli.TUIOptions) error {
	return tui.Run(ctx, tui.Options{ConfigPath: o.ConfigPath, Context: o.Context, Topic: o.Topic, Group: o.Group})
}
