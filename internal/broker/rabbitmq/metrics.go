package rabbitmq

import (
	"context"
	"fmt"
	"time"

	"github.com/Max2535/mqx/internal/broker"
)

// Extra metric names reported besides broker.Metric*.
const (
	MetricAcks      = "acks"
	MetricRedeliver = "redeliver"
	MetricReady     = "ready"
	MetricUnacked   = "unacked"
)

// Sample reads a queue's counters, or the cluster-wide totals from
// /api/overview when target is "". Counters are monotonic totals since the
// stats were last reset; the management plugin refreshes them every
// collect_statistics_interval (5s by default).
func (r *RabbitMQ) Sample(ctx context.Context, target string) (broker.MetricSample, error) {
	if target == "" {
		return r.sampleOverview(ctx)
	}
	var q queueJSON
	if err := r.mgmt.get(ctx, apiPath("queues", r.settings.vhost, target), &q); err != nil {
		return broker.MetricSample{}, fmt.Errorf("sample queue %q: %w", target, err)
	}
	s := newSample(q.MessageStats)
	s.Gauges[broker.MetricDepth] = float64(deref(q.Messages))
	s.Gauges[MetricReady] = float64(deref(q.MessagesReady))
	s.Gauges[MetricUnacked] = float64(deref(q.MessagesUnacknowledged))
	if q.Consumers != nil {
		s.Gauges[broker.MetricConsumers] = float64(*q.Consumers)
	}
	return s, nil
}

func (r *RabbitMQ) sampleOverview(ctx context.Context) (broker.MetricSample, error) {
	var o struct {
		MessageStats messageStats `json:"message_stats"`
		QueueTotals  struct {
			Messages               int64 `json:"messages"`
			MessagesReady          int64 `json:"messages_ready"`
			MessagesUnacknowledged int64 `json:"messages_unacknowledged"`
		} `json:"queue_totals"`
		ObjectTotals struct {
			Consumers   int `json:"consumers"`
			Connections int `json:"connections"`
		} `json:"object_totals"`
	}
	if err := r.mgmt.get(ctx, apiPath("overview"), &o); err != nil {
		return broker.MetricSample{}, fmt.Errorf("sample overview: %w", err)
	}
	s := newSample(o.MessageStats)
	s.Gauges[broker.MetricDepth] = float64(o.QueueTotals.Messages)
	s.Gauges[MetricReady] = float64(o.QueueTotals.MessagesReady)
	s.Gauges[MetricUnacked] = float64(o.QueueTotals.MessagesUnacknowledged)
	s.Gauges[broker.MetricConsumers] = float64(o.ObjectTotals.Consumers)
	s.Gauges[broker.MetricConnections] = float64(o.ObjectTotals.Connections)
	return s, nil
}

func newSample(ms messageStats) broker.MetricSample {
	return broker.MetricSample{
		Time: time.Now(),
		Counters: map[string]float64{
			broker.MetricMessagesIn:  ms.Publish,
			broker.MetricMessagesOut: ms.DeliverGet,
			MetricAcks:               ms.Ack,
			MetricRedeliver:          ms.Redeliver,
		},
		Gauges: map[string]float64{},
	}
}

func deref(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}
