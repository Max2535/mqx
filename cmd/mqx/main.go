// Command mqx inspects, peeks and publishes messages across brokers.
package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/Max2535/mqx/internal/cli"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	code := cli.Execute(ctx)
	stop() // not deferred: os.Exit skips defers
	os.Exit(code)
}
