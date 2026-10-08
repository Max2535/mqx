package kafka

import (
	"context"
	"fmt"

	"github.com/Max2535/mqx/internal/broker"
)

// resolvePositions turns a position into an absolute offset for each
// partition, clamped to [low, high]. Times use ListOffsetsAfterMilli; a time
// after the last record resolves to the high watermark.
func (k *Kafka) resolvePositions(ctx context.Context, topic string, parts []int32, m marks, pos broker.Position) (map[int32]int64, error) {
	out := make(map[int32]int64, len(parts))
	var byTime map[int32]int64
	if pos.Kind == broker.AtTime {
		listed, err := k.adm.ListOffsetsAfterMilli(ctx, pos.Time.UnixMilli(), topic)
		if err != nil {
			return nil, wrapErr("list offsets by time", err)
		}
		byTime = map[int32]int64{}
		for p, o := range listed[topic] {
			if o.Err == nil {
				byTime[p] = o.Offset
			}
		}
	}
	for _, p := range parts {
		low, high := m.get(topic, p)
		var off int64
		switch pos.Kind {
		case broker.Earliest:
			off = low
		case broker.Latest:
			off = high
		case broker.AtOffset:
			off = pos.Offset
		case broker.Tail:
			off = high - pos.Offset
		case broker.AtTime:
			o, ok := byTime[p]
			if !ok || o < 0 {
				o = high
			}
			off = o
		default:
			return nil, fmt.Errorf("position kind %d: %w", pos.Kind, broker.ErrUnsupported)
		}
		out[p] = min(max(off, low), high)
	}
	return out, nil
}
