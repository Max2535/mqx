package groupdiag

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Max2535/mqx/internal/broker"
)

// EventKind classifies a timeline event.
type EventKind string

// Event kinds.
const (
	EventState             EventKind = "state"
	EventJoin              EventKind = "join"
	EventLeave             EventKind = "leave"
	EventEpoch             EventKind = "epoch"
	EventAssignment        EventKind = "assignment"
	EventRebalanceComplete EventKind = "rebalance-complete"
)

// Event is one change between two observations of a group.
type Event struct {
	At     time.Time `json:"at"`
	Kind   EventKind `json:"kind"`
	From   string    `json:"from,omitempty"` // states or epochs
	To     string    `json:"to,omitempty"`
	Member string    `json:"member,omitempty"`
	Detail string    `json:"detail"`
	// Duration is set on rebalance-complete: time since leaving Stable.
	Duration time.Duration `json:"duration,omitempty"`
}

// Watcher turns successive descriptions of one group into a timeline.
type Watcher struct {
	prev *broker.GroupDescription
	// rebalanceStart is when the group was first seen out of Stable; zero while Stable.
	rebalanceStart time.Time
}

// NewWatcher returns a Watcher with no observation yet.
func NewWatcher() *Watcher { return &Watcher{} }

const stable = "Stable"

// Observe compares d with the previous observation and returns what changed.
// The first call returns a baseline "state" event.
func (w *Watcher) Observe(d *broker.GroupDescription) []Event {
	if d == nil {
		return nil
	}
	at := d.DescribedAt
	if at.IsZero() {
		at = time.Now()
	}
	prev := w.prev
	w.prev = d
	if prev == nil {
		if d.State != stable {
			w.rebalanceStart = at
		}
		return []Event{{
			At: at, Kind: EventState, To: d.State,
			Detail: fmt.Sprintf("group %s is %s with %d members%s", d.Name, d.State, len(d.Members), epochLabel(d.Epoch)),
		}}
	}
	var out []Event
	if prev.State != d.State {
		out = append(out, Event{At: at, Kind: EventState, From: prev.State, To: d.State,
			Detail: fmt.Sprintf("state %s -> %s", prev.State, d.State)})
		if prev.State == stable {
			w.rebalanceStart = at
		}
	}
	out = append(out, memberEvents(at, prev, d)...)
	if prev.Epoch != d.Epoch && prev.Epoch >= 0 && d.Epoch >= 0 {
		out = append(out, Event{At: at, Kind: EventEpoch, From: strconv.Itoa(int(prev.Epoch)), To: strconv.Itoa(int(d.Epoch)),
			Detail: fmt.Sprintf("group epoch %d -> %d", prev.Epoch, d.Epoch)})
	}
	if d.State == stable && prev.State != stable && !w.rebalanceStart.IsZero() {
		dur := at.Sub(w.rebalanceStart)
		out = append(out, Event{At: at, Kind: EventRebalanceComplete, Duration: dur,
			Detail: fmt.Sprintf("rebalance complete after %s with %d members", dur.Round(time.Millisecond), len(d.Members))})
		w.rebalanceStart = time.Time{}
	}
	return out
}

func epochLabel(epoch int32) string {
	if epoch < 0 {
		return ""
	}
	return fmt.Sprintf(" (epoch %d)", epoch)
}

// memberEvents reports leaves, joins and assignment changes, in that order.
func memberEvents(at time.Time, prev, cur *broker.GroupDescription) []Event {
	before := map[string]broker.GroupMember{}
	for _, m := range prev.Members {
		before[m.MemberID] = m
	}
	after := map[string]broker.GroupMember{}
	for _, m := range cur.Members {
		after[m.MemberID] = m
	}
	var out []Event
	for _, m := range prev.Members {
		if _, ok := after[m.MemberID]; !ok {
			out = append(out, Event{At: at, Kind: EventLeave, Member: m.MemberID, Detail: "left: " + describeMember(m)})
		}
	}
	for _, m := range cur.Members {
		if _, ok := before[m.MemberID]; !ok {
			out = append(out, Event{At: at, Kind: EventJoin, Member: m.MemberID, Detail: "joined: " + describeMember(m)})
		}
	}
	for _, m := range cur.Members {
		old, ok := before[m.MemberID]
		if !ok || assignmentString(old.Assignment) == assignmentString(m.Assignment) {
			continue
		}
		out = append(out, Event{At: at, Kind: EventAssignment, Member: m.MemberID,
			From: assignmentString(old.Assignment), To: assignmentString(m.Assignment),
			Detail: fmt.Sprintf("%s: %s -> %s", memberName(m), orNone(assignmentString(old.Assignment)), orNone(assignmentString(m.Assignment)))})
	}
	return out
}

func describeMember(m broker.GroupMember) string {
	s := memberLabel(m)
	if len(m.Subscriptions) > 0 {
		s += " subscribed to " + strings.Join(m.Subscriptions, ",")
	}
	return s
}

func assignmentString(tps []broker.TopicPartitions) string {
	parts := make([]string, 0, len(tps))
	for _, t := range tps {
		ps := slices.Clone(t.Partitions)
		slices.Sort(ps)
		nums := make([]string, len(ps))
		for i, p := range ps {
			nums[i] = strconv.Itoa(int(p))
		}
		parts = append(parts, t.Topic+"["+strings.Join(nums, ",")+"]")
	}
	slices.Sort(parts)
	return strings.Join(parts, " ")
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}
