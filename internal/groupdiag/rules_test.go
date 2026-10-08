package groupdiag

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Max2535/mqx/internal/broker"
)

var t0 = time.Date(2026, 10, 8, 9, 0, 0, 0, time.UTC)

func member(id string, subs []string, assign ...broker.TopicPartitions) broker.GroupMember {
	return broker.GroupMember{MemberID: id, ClientID: "app", Host: "/10.0.0.1", Subscriptions: subs, Assignment: assign}
}

func parts(topic string, ps ...int32) broker.TopicPartitions {
	return broker.TopicPartitions{Topic: topic, Partitions: ps}
}

func lag(topic string, p int32, committed, end int64, owner string) broker.PartitionLag {
	l := broker.PartitionLag{Topic: topic, Partition: p, Committed: committed, End: end, Lag: end - committed, MemberID: owner}
	if committed < 0 {
		l.Lag = -1
	}
	return l
}

func snap(at time.Duration, state string, members []broker.GroupMember, lags []broker.PartitionLag, partitions map[string]int) Snapshot {
	return Snapshot{
		At:         t0.Add(at),
		Group:      &broker.GroupDescription{Name: "g", State: state, Members: members, Epoch: -1},
		Lag:        lags,
		Partitions: partitions,
	}
}

func rulesOf(fs []Finding) []string {
	out := make([]string, len(fs))
	for i, f := range fs {
		out[i] = f.Rule
	}
	return out
}

func TestDiagnose(t *testing.T) {
	orders := map[string]int{"orders": 2}
	a := member("a", []string{"orders"}, parts("orders", 0))
	b := member("b", []string{"orders"}, parts("orders", 1))
	static := b
	static.InstanceID = "pod-2"
	tests := []struct {
		name       string
		snaps      []Snapshot
		want       []string // rule ids, in output order
		wantText   string   // substring of the first finding's detail or fix
		wantMember []string // members of the first finding
	}{
		{name: "no snapshots"},
		{
			name: "healthy group",
			snaps: []Snapshot{
				snap(0, "Stable", []broker.GroupMember{a, b}, []broker.PartitionLag{lag("orders", 0, 5, 5, "a"), lag("orders", 1, 5, 6, "b")}, orders),
				snap(10*time.Second, "Stable", []broker.GroupMember{a, b}, []broker.PartitionLag{lag("orders", 0, 9, 9, "a"), lag("orders", 1, 8, 8, "b")}, orders),
			},
		},
		{
			name: "stale dynamic member",
			snaps: []Snapshot{
				snap(0, "Stable", []broker.GroupMember{a, b}, []broker.PartitionLag{lag("orders", 0, 5, 5, "a"), lag("orders", 1, 5, 5, "b")}, orders),
				snap(10*time.Second, "Stable", []broker.GroupMember{a, b}, []broker.PartitionLag{lag("orders", 0, 9, 9, "a"), lag("orders", 1, 5, 9, "b")}, orders),
			},
			want: []string{RuleStaleMember}, wantText: "session.timeout.ms", wantMember: []string{"b"},
		},
		{
			name: "stale static member points at remove-member",
			snaps: []Snapshot{
				snap(0, "Stable", []broker.GroupMember{a, static}, []broker.PartitionLag{lag("orders", 1, -1, 3, "b")}, orders),
				snap(5*time.Second, "Stable", []broker.GroupMember{a, static}, []broker.PartitionLag{lag("orders", 1, -1, 7, "b")}, orders),
			},
			want: []string{RuleStaleMember, RuleUncommittedPartitions}, wantText: "mqx group remove-member g --instance-id pod-2",
		},
		{
			name: "no new messages is not stale",
			snaps: []Snapshot{
				snap(0, "Stable", []broker.GroupMember{b}, []broker.PartitionLag{lag("orders", 1, 5, 9, "b")}, map[string]int{"orders": 1}),
				snap(5*time.Second, "Stable", []broker.GroupMember{b}, []broker.PartitionLag{lag("orders", 1, 5, 9, "b")}, map[string]int{"orders": 1}),
			},
		},
		{
			name:  "single snapshot cannot judge staleness",
			snaps: []Snapshot{snap(0, "Stable", []broker.GroupMember{b}, []broker.PartitionLag{lag("orders", 1, 5, 9, "b")}, map[string]int{"orders": 2})},
		},
		{
			name: "more members than partitions",
			snaps: []Snapshot{snap(0, "Stable", []broker.GroupMember{
				member("a", []string{"orders"}, parts("orders", 0)), member("b", []string{"orders"}, parts("orders", 1)),
				member("c", []string{"orders"}), member("d", []string{"orders"}),
			}, nil, orders)},
			want: []string{RuleMoreMembers}, wantText: "2 are idle", wantMember: []string{"c", "d"},
		},
		{
			name: "shared group id",
			snaps: []Snapshot{snap(0, "Stable", []broker.GroupMember{
				member("a", []string{"orders"}, parts("orders", 0, 1)), member("b", []string{"payments"}, parts("payments", 0)),
			}, nil, map[string]int{"orders": 2, "payments": 1})},
			want: []string{RuleSharedGroupID}, wantText: "{orders}: a; {payments}: b", wantMember: []string{"a", "b"},
		},
		{
			name: "pattern subscription counts as a set",
			snaps: []Snapshot{snap(0, "Stable", []broker.GroupMember{
				{MemberID: "a", Pattern: "orders.*", Subscriptions: []string{"orders"}, Assignment: []broker.TopicPartitions{parts("orders", 0)}},
				member("b", []string{"orders"}, parts("orders", 1)),
			}, nil, orders)},
			want: []string{RuleSharedGroupID}, wantText: "pattern orders.*",
		},
		{
			name:  "rebalancing once is info",
			snaps: []Snapshot{snap(0, "PreparingRebalance", []broker.GroupMember{a}, nil, orders)},
			want:  []string{RuleRebalancing},
		},
		{
			name: "stuck rebalance is a warning",
			snaps: []Snapshot{
				snap(0, "CompletingRebalance", []broker.GroupMember{a}, nil, orders),
				snap(30*time.Second, "Reconciling", []broker.GroupMember{a}, nil, orders),
			},
			want: []string{RuleRebalancing}, wantText: "30s across 2 samples",
		},
		{
			name:  "empty with lag",
			snaps: []Snapshot{snap(0, "Empty", nil, []broker.PartitionLag{lag("orders", 0, 1, 10, "")}, orders)},
			want:  []string{RuleEmptyWithLag}, wantText: "mqx group delete g",
		},
		{
			name:  "empty without lag",
			snaps: []Snapshot{snap(0, "Empty", nil, []broker.PartitionLag{lag("orders", 0, 10, 10, "")}, orders)},
		},
		{
			name: "uneven assignment",
			snaps: []Snapshot{snap(0, "Stable", []broker.GroupMember{
				member("a", []string{"orders"}, parts("orders", 0, 1, 2, 3)), member("b", []string{"orders"}, parts("orders", 4)),
			}, nil, map[string]int{"orders": 5})},
			want: []string{RuleUnevenAssignment}, wantMember: []string{"a"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Diagnose(tt.snaps)
			if !slices.Equal(rulesOf(got), tt.want) && (len(got) != 0 || len(tt.want) != 0) {
				t.Fatalf("rules = %v, want %v\n%+v", rulesOf(got), tt.want, got)
			}
			if len(got) == 0 {
				return
			}
			f := got[0]
			if f.Title == "" || f.Detail == "" || f.Fix == "" || f.Severity == "" {
				t.Errorf("incomplete finding %+v", f)
			}
			if tt.wantText != "" && !strings.Contains(f.Detail+" "+f.Fix, tt.wantText) {
				t.Errorf("finding %+v does not mention %q", f, tt.wantText)
			}
			if tt.wantMember != nil && !slices.Equal(f.Members, tt.wantMember) {
				t.Errorf("members = %v, want %v", f.Members, tt.wantMember)
			}
		})
	}
}

func TestDiagnoseOrdersBySeverity(t *testing.T) {
	a := member("a", []string{"orders"}, parts("orders", 0))
	b := member("b", []string{"payments"}, parts("payments", 0))
	snaps := []Snapshot{
		snap(0, "Stable", []broker.GroupMember{a, b}, []broker.PartitionLag{lag("orders", 0, 1, 1, "a"), lag("payments", 0, -1, 4, "b")},
			map[string]int{"orders": 1, "payments": 1}),
		snap(time.Minute, "Stable", []broker.GroupMember{a, b}, []broker.PartitionLag{lag("orders", 0, 1, 5, "a"), lag("payments", 0, -1, 4, "b")},
			map[string]int{"orders": 1, "payments": 1}),
	}
	got := Diagnose(snaps)
	want := []string{RuleStaleMember, RuleSharedGroupID, RuleUncommittedPartitions}
	if !slices.Equal(rulesOf(got), want) {
		t.Fatalf("rules = %v, want %v", rulesOf(got), want)
	}
	if got[0].Severity != SeverityError || got[1].Severity != SeverityWarning || got[2].Severity != SeverityInfo {
		t.Errorf("severities = %s %s %s", got[0].Severity, got[1].Severity, got[2].Severity)
	}
}
