package cli

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/Max2535/mqx/internal/broker"
)

func newMetricsCmd(o *options) *cobra.Command {
	var (
		interval time.Duration
		count    int
	)
	cmd := &cobra.Command{
		Use:   "metrics [topic|queue]",
		Short: "Print message rates and gauges over time",
		Long: `Print message rates and gauges over time.

Samples the broker every --interval and prints per-second rates of its counters
(messages in/out, ...) and current gauges (lag, depth, connections, ...).
Without an argument it reports the whole cluster or vhost.`,
		Example: `  mqx metrics orders --interval 5s
  mqx metrics --count 10 -o json`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if interval <= 0 {
				return fmt.Errorf("--interval must be positive")
			}
			target := ""
			if len(args) == 1 {
				target = args[0]
			}
			s, err := o.open(cmd)
			if err != nil {
				return err
			}
			defer s.Close()
			mr, err := capability[broker.MetricsReporter](s, broker.CapMetricsReporter)
			if err != nil {
				return err
			}
			sample := func() (broker.MetricSample, error) {
				ctx, cancel := o.requestContext(cmd)
				defer cancel()
				return mr.Sample(ctx, target)
			}
			prev, err := sample()
			if err != nil {
				return err
			}
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for i := 0; count == 0 || i < count; i++ {
				select {
				case <-cmd.Context().Done():
					return nil
				case <-ticker.C:
				}
				cur, err := sample()
				if err != nil {
					return err
				}
				if err := o.writeRates(cmd, i == 0, prev, cur); err != nil {
					return err
				}
				prev = cur
			}
			return nil
		},
	}
	cmd.Flags().DurationVar(&interval, "interval", 2*time.Second, "time between samples")
	cmd.Flags().IntVar(&count, "count", 0, "stop after this many rate lines (0 = until interrupted)")
	return cmd
}

type rateLine struct {
	Time   time.Time          `json:"time"`
	Rates  map[string]float64 `json:"rates_per_sec"`
	Gauges map[string]float64 `json:"gauges"`
}

func (o *options) writeRates(cmd *cobra.Command, first bool, prev, cur broker.MetricSample) error {
	line := rateLine{Time: cur.Time, Rates: broker.Rates(prev, cur), Gauges: cur.Gauges}
	w := cmd.OutOrStdout()
	if o.output == "json" {
		return json.NewEncoder(w).Encode(line)
	}
	rates, gauges := sortedKeys(line.Rates), sortedKeys(line.Gauges)
	if first {
		hdr := fmt.Sprintf("%-8s", "TIME")
		for _, k := range rates {
			hdr += fmt.Sprintf("  %14s", k+"/s")
		}
		for _, k := range gauges {
			hdr += fmt.Sprintf("  %12s", k)
		}
		fmt.Fprintln(w, hdr)
	}
	row := cur.Time.Local().Format("15:04:05")
	for _, k := range rates {
		row += fmt.Sprintf("  %14.1f", line.Rates[k])
	}
	for _, k := range gauges {
		row += fmt.Sprintf("  %12s", cell(line.Gauges[k]))
	}
	_, err := fmt.Fprintln(w, row)
	return err
}
