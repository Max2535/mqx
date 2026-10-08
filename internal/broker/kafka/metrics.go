package kafka

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/Max2535/mqx/internal/broker"
)

// Sample implements broker.MetricsReporter. Counter messages_in is the sum of
// high watermarks of the target topic (or every non-internal topic); gauges
// are the total lag of the groups committed on it and its partition count.
func (k *Kafka) Sample(ctx context.Context, target string) (broker.MetricSample, error) {
	var topics []string
	if target != "" {
		if _, err := k.topicDetail(ctx, target); err != nil {
			return broker.MetricSample{}, err
		}
		topics = []string{target}
	} else {
		tds, err := k.adm.ListTopics(ctx) // excludes internal topics
		if err != nil {
			return broker.MetricSample{}, wrapErr("list topics", err)
		}
		topics = tds.Names()
	}
	s := broker.MetricSample{Time: time.Now(), Counters: map[string]float64{}, Gauges: map[string]float64{}}
	if len(topics) == 0 {
		s.Counters[broker.MetricMessagesIn] = 0
		s.Gauges[broker.MetricLag] = 0
		s.Gauges["partitions"] = 0
		return s, nil
	}
	ends, err := k.adm.ListEndOffsets(ctx, topics...)
	if err != nil {
		return broker.MetricSample{}, wrapErr("list end offsets", err)
	}
	var in, partitions float64
	wanted := map[string]bool{}
	for _, t := range topics {
		wanted[t] = true
		for p, o := range ends[t] {
			if p >= 0 && o.Err == nil {
				in += float64(o.Offset)
				partitions++
			}
		}
	}
	lag, err := k.totalLag(ctx, wanted, ends)
	if err != nil {
		return broker.MetricSample{}, err
	}
	s.Counters[broker.MetricMessagesIn] = in
	s.Gauges[broker.MetricLag] = lag
	s.Gauges["partitions"] = partitions
	return s, nil
}

// totalLag sums the lag of every group's commits on the wanted topics.
func (k *Kafka) totalLag(ctx context.Context, wanted map[string]bool, ends kadm.ListedOffsets) (float64, error) {
	groups, err := k.adm.ListGroups(ctx)
	if err != nil {
		return 0, wrapErr("list groups", err)
	}
	if len(groups) == 0 {
		return 0, nil
	}
	var lag float64
	for _, resp := range k.adm.FetchManyOffsets(ctx, groups.Groups()...) {
		if resp.Err != nil {
			continue // a group deleted meanwhile
		}
		for t, ps := range resp.Fetched {
			if !wanted[t] {
				continue
			}
			for p, c := range ps {
				if end, ok := ends[t][p]; ok && end.Err == nil && c.Err == nil && c.At >= 0 {
					lag += float64(max(0, end.Offset-c.At))
				}
			}
		}
	}
	return lag, nil
}
