package groupdiag

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Max2535/mqx/internal/broker"
)

func desc(at time.Duration, state string, epoch int32, members ...broker.GroupMember) *broker.GroupDescription {
	return &broker.GroupDescription{Name: "g", State: state, Epoch: epoch, Members: members, DescribedAt: t0.Add(at)}
}

type ev struct {
	kind     EventKind
	from, to string
	member   string
	duration time.Duration
}

func evs(events []Event) []ev {
	out := make([]ev, len(events))
	for i, e := range events {
		out[i] = ev{e.Kind, e.From, e.To, e.Member, e.Duration}
	}
	return out
}

func TestWatcher(t *testing.T) {
	a := member("a", []string{"orders"}, parts("orders", 0, 1))
	aHalf := member("a", []string{"orders"}, parts("orders", 0))
	b := member("b", []string{"orders"}, parts("orders", 1))
	tests := []struct {
		name  string
		steps []*broker.GroupDescription
		want  [][]ev // events per step
	}{
		{
			name:  "baseline then nothing",
			steps: []*broker.GroupDescription{desc(0, "Stable", 3, a), desc(time.Second, "Stable", 3, a)},
			want:  [][]ev{{{kind: EventState, to: "Stable"}}, nil},
		},
		{
			name: "member joins and the group rebalances",
			steps: []*broker.GroupDescription{
				desc(0, "Stable", 3, a),
				desc(2*time.Second, "PreparingRebalance", 3, a, member("b", []string{"orders"})),
				desc(3*time.Second, "CompletingRebalance", 4, a, member("b", []string{"orders"})),
				desc(5*time.Second, "Stable", 4, aHalf, b),
			},
			want: [][]ev{
				{{kind: EventState, to: "Stable"}},
				{{kind: EventState, from: "Stable", to: "PreparingRebalance"}, {kind: EventJoin, member: "b"}},
				{{kind: EventState, from: "PreparingRebalance", to: "CompletingRebalance"}, {kind: EventEpoch, from: "3", to: "4"}},
				{
					{kind: EventState, from: "CompletingRebalance", to: "Stable"},
					{kind: EventAssignment, from: "orders[0,1]", to: "orders[0]", member: "a"},
					{kind: EventAssignment, from: "", to: "orders[1]", member: "b"},
					{kind: EventRebalanceComplete, duration: 3 * time.Second},
				},
			},
		},
		{
			name: "member leaves and the group empties",
			steps: []*broker.GroupDescription{
				desc(0, "Stable", -1, a),
				desc(time.Second, "Empty", -1),
			},
			want: [][]ev{
				{{kind: EventState, to: "Stable"}},
				{{kind: EventState, from: "Stable", to: "Empty"}, {kind: EventLeave, member: "a"}},
			},
		},
		{
			name: "watch starting mid-rebalance measures from the first observation",
			steps: []*broker.GroupDescription{
				desc(0, "Reconciling", 7, a),
				desc(1500*time.Millisecond, "Stable", 7, a),
			},
			want: [][]ev{
				{{kind: EventState, to: "Reconciling"}},
				{{kind: EventState, from: "Reconciling", to: "Stable"}, {kind: EventRebalanceComplete, duration: 1500 * time.Millisecond}},
			},
		},
		{
			name: "kip-848 epoch bump without a state change",
			steps: []*broker.GroupDescription{
				desc(0, "Stable", 10, a),
				desc(time.Second, "Stable", 11, aHalf, b),
			},
			want: [][]ev{
				{{kind: EventState, to: "Stable"}},
				{
					{kind: EventJoin, member: "b"},
					{kind: EventAssignment, from: "orders[0,1]", to: "orders[0]", member: "a"},
					{kind: EventEpoch, from: "10", to: "11"},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWatcher()
			for i, d := range tt.steps {
				got := w.Observe(d)
				if !reflect.DeepEqual(evs(got), tt.want[i]) && (len(got) != 0 || len(tt.want[i]) != 0) {
					t.Fatalf("step %d events =\n%+v\nwant\n%+v", i, evs(got), tt.want[i])
				}
				for _, e := range got {
					if e.Detail == "" || !e.At.Equal(d.DescribedAt) {
						t.Errorf("step %d: event %+v lacks detail or time", i, e)
					}
				}
			}
		})
	}
}

func TestEventJSON(t *testing.T) {
	data, err := json.Marshal(Event{At: t0, Kind: EventRebalanceComplete, From: "a", To: "b", Detail: "d", Duration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"from":"a"`, `"to":"b"`, `"kind":"rebalance-complete"`, `"duration":1000000000`} {
		if !strings.Contains(string(data), want) {
			t.Errorf("%s lacks %s", data, want)
		}
	}
	if w := NewWatcher(); w.Observe(nil) != nil {
		t.Error("Observe(nil) returned events")
	}
}
