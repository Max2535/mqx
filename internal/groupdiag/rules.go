package groupdiag

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/Max2535/mqx/internal/broker"
)

// Severity ranks a finding.
type Severity string

// Severities, most severe first.
const (
	SeverityError   Severity = "error"
	SeverityWarning Severity = "warning"
	SeverityInfo    Severity = "info"
)

func (s Severity) rank() int {
	switch s {
	case SeverityError:
		return 0
	case SeverityWarning:
		return 1
	}
	return 2
}

// Finding is one problem a rule detected, with how to fix it.
type Finding struct {
	Severity Severity `json:"severity"`
	Rule     string   `json:"rule"` // stable id, e.g. "stale-member"
	Title    string   `json:"title"`
	Detail   string   `json:"detail"`
	Fix      string   `json:"fix"`
	Members  []string `json:"members,omitempty"`
}

// Rule ids.
const (
	RuleStaleMember           = "stale-member"
	RuleMoreMembers           = "more-members-than-partitions"
	RuleSharedGroupID         = "shared-group-id"
	RuleRebalancing           = "rebalancing"
	RuleEmptyWithLag          = "empty-with-lag"
	RuleUncommittedPartitions = "uncommitted-partitions"
	RuleUnevenAssignment      = "uneven-assignment"
)

type rule func(first, last Snapshot, all []Snapshot) []Finding

var rules = []rule{
	staleMembers, moreMembersThanPartitions, sharedGroupID, rebalancing,
	emptyWithLag, uncommittedPartitions, unevenAssignment,
}

// Diagnose runs every rule. Rules needing progress over time use the first and last snapshot.
func Diagnose(snaps []Snapshot) []Finding {
	snaps = slices.DeleteFunc(slices.Clone(snaps), func(s Snapshot) bool { return s.Group == nil })
	if len(snaps) == 0 {
		return nil
	}
	first, last := snaps[0], snaps[len(snaps)-1]
	var out []Finding
	for _, r := range rules {
		out = append(out, r(first, last, snaps)...)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Severity.rank() != out[j].Severity.rank() {
			return out[i].Severity.rank() < out[j].Severity.rank()
		}
		return out[i].Rule < out[j].Rule
	})
	return out
}

type tp struct {
	topic     string
	partition int32
}

func (t tp) String() string { return fmt.Sprintf("%s[%d]", t.topic, t.partition) }

func lagByPartition(s Snapshot) map[tp]broker.PartitionLag {
	out := make(map[tp]broker.PartitionLag, len(s.Lag))
	for _, l := range s.Lag {
		out[tp{l.Topic, l.Partition}] = l
	}
	return out
}

func owned(m broker.GroupMember) []tp {
	var out []tp
	for _, a := range m.Assignment {
		for _, p := range a.Partitions {
			out = append(out, tp{a.Topic, p})
		}
	}
	return out
}

func memberLabel(m broker.GroupMember) string {
	label := m.MemberID
	if m.InstanceID != "" {
		label = m.InstanceID + " (" + m.MemberID + ")"
	}
	if m.ClientID != "" || m.Host != "" {
		label += fmt.Sprintf(" [%s@%s]", m.ClientID, strings.TrimPrefix(m.Host, "/"))
	}
	return label
}

func memberName(m broker.GroupMember) string {
	if m.InstanceID != "" {
		return m.InstanceID
	}
	return m.MemberID
}

// staleMembers finds members present in the first and last snapshot whose
// partitions received messages while their committed offsets did not move:
// typically a consumer that died without leaving the group.
func staleMembers(first, last Snapshot, _ []Snapshot) []Finding {
	if !last.At.After(first.At) {
		return nil
	}
	before, after := lagByPartition(first), lagByPartition(last)
	present := map[string]bool{}
	for _, m := range first.Group.Members {
		present[m.MemberID] = true
	}
	var out []Finding
	for _, m := range last.Group.Members {
		if !present[m.MemberID] {
			continue
		}
		var stuck []string
		var lagBefore, lagAfter int64
		progressed := false
		for _, p := range owned(m) {
			b, okB := before[p]
			a, okA := after[p]
			if !okB || !okA || a.End <= b.End {
				continue // no new messages: nothing to judge
			}
			if a.Committed != b.Committed {
				progressed = true
				continue
			}
			stuck = append(stuck, p.String())
			lagBefore += max(b.Lag, 0)
			lagAfter += max(a.Lag, 0)
		}
		if progressed || len(stuck) == 0 {
			continue
		}
		f := Finding{
			Severity: SeverityError,
			Rule:     RuleStaleMember,
			Title:    "member " + memberName(m) + " is not consuming its partitions",
			Detail: fmt.Sprintf("%s owns %s; over %s new messages arrived (lag %d -> %d) but its committed offsets did not move. "+
				"This is typical of a consumer that stopped without leaving the group.",
				memberLabel(m), strings.Join(stuck, ", "), last.At.Sub(first.At).Round(time.Second), lagBefore, lagAfter),
			Members: []string{m.MemberID},
		}
		if m.InstanceID != "" {
			f.Fix = fmt.Sprintf("Static members are kept until session.timeout.ms expires or the instance rejoins. "+
				"Restart the instance with group.instance.id=%s, or remove it now with `mqx group remove-member %s --instance-id %s`.",
				m.InstanceID, last.Group.Name, m.InstanceID)
		} else {
			f.Fix = "The coordinator removes a dead member after session.timeout.ms (45s by default); lower it if failover is too slow. " +
				"If the member stays, the process is alive but stuck: check its logs and max.poll.interval.ms."
		}
		out = append(out, f)
	}
	return out
}

// moreMembersThanPartitions finds members that can never get a partition.
func moreMembersThanPartitions(_, last Snapshot, _ []Snapshot) []Finding {
	members := last.Group.Members
	total, known := 0, true
	for t := range subscribedTopics(last.Group) {
		n, ok := last.Partitions[t]
		if !ok {
			known = false
		}
		total += n
	}
	if !known || len(members) <= total || total == 0 {
		return nil
	}
	var idle []string
	for _, m := range members {
		if len(owned(m)) == 0 {
			idle = append(idle, m.MemberID)
		}
	}
	return []Finding{{
		Severity: SeverityWarning,
		Rule:     RuleMoreMembers,
		Title:    fmt.Sprintf("%d members share %d partitions", len(members), total),
		Detail: fmt.Sprintf("The subscribed topics have %d partitions in total, so at most %d of the %d members get work; "+
			"%d are idle and only take over on failure.", total, total, len(members), len(members)-total),
		Fix: fmt.Sprintf("Scale the consumers down to %d, or add partitions with `mqx topic add-partitions <topic> --total N` "+
			"(this changes key-to-partition mapping).", total),
		Members: idle,
	}}
}

// sharedGroupID finds members of one group subscribing to different topics,
// usually different applications configured with the same group.id.
func sharedGroupID(_, last Snapshot, _ []Snapshot) []Finding {
	bySet := map[string][]string{}
	for _, m := range last.Group.Members {
		key := strings.Join(m.Subscriptions, ",")
		if m.Pattern != "" {
			key = "pattern " + m.Pattern
		}
		if key == "" {
			continue // subscription unknown (e.g. a non-consumer protocol)
		}
		bySet[key] = append(bySet[key], memberName(m))
	}
	if len(bySet) < 2 {
		return nil
	}
	sets := make([]string, 0, len(bySet))
	var members []string
	for k := range bySet {
		sets = append(sets, k)
	}
	sort.Strings(sets)
	parts := make([]string, len(sets))
	for i, s := range sets {
		parts[i] = fmt.Sprintf("{%s}: %s", s, strings.Join(bySet[s], ", "))
		members = append(members, bySet[s]...)
	}
	return []Finding{{
		Severity: SeverityWarning,
		Rule:     RuleSharedGroupID,
		Title:    fmt.Sprintf("members subscribe to %d different topic sets", len(sets)),
		Detail: "One group id seems shared by different applications; each subscription change rebalances all of them " +
			"and partitions can be left unassigned. Subscriptions: " + strings.Join(parts, "; ") + ".",
		Fix:     "Give each application its own group.id. Move consumed offsets first with `mqx group reset-offsets` on the new group.",
		Members: members,
	}}
}

// rebalancing reports a group that is not Stable.
func rebalancing(_, last Snapshot, all []Snapshot) []Finding {
	if !isRebalancing(last.Group.State) {
		return nil
	}
	persistent := true
	for _, s := range all {
		persistent = persistent && isRebalancing(s.Group.State)
	}
	f := Finding{
		Severity: SeverityInfo,
		Rule:     RuleRebalancing,
		Title:    "group is rebalancing (" + last.Group.State + ")",
		Detail:   "Consumption pauses for the affected partitions until the rebalance completes.",
		Fix:      "Usually transient. Follow it with `mqx group watch " + last.Group.Name + "`.",
	}
	if persistent && len(all) > 1 {
		f.Severity = SeverityWarning
		f.Detail = fmt.Sprintf("The group stayed in a rebalancing state for %s across %d samples.",
			last.At.Sub(all[0].At).Round(time.Second), len(all))
		f.Fix = "A member may be slow to rejoin: check consumers whose poll loop exceeds max.poll.interval.ms, " +
			"or members joining and leaving repeatedly (`mqx group watch " + last.Group.Name + "`)."
	}
	return []Finding{f}
}

func isRebalancing(state string) bool {
	switch state {
	case "PreparingRebalance", "CompletingRebalance", "Assigning", "Reconciling":
		return true
	}
	return false
}

// emptyWithLag reports unconsumed messages in a group without members.
func emptyWithLag(_, last Snapshot, _ []Snapshot) []Finding {
	if len(last.Group.Members) > 0 {
		return nil
	}
	var total int64
	for _, l := range last.Lag {
		total += max(l.Lag, 0)
	}
	if total == 0 {
		return nil
	}
	return []Finding{{
		Severity: SeverityWarning,
		Rule:     RuleEmptyWithLag,
		Title:    fmt.Sprintf("no members, %d messages waiting", total),
		Detail:   "The group has committed offsets and unconsumed messages but no consumer is running.",
		Fix: "Start the consumers, or if the group is retired delete it with `mqx group delete " + last.Group.Name +
			"` so it stops showing up in lag alerts.",
	}}
}

// uncommittedPartitions lists subscribed partitions without a committed offset.
func uncommittedPartitions(_, last Snapshot, _ []Snapshot) []Finding {
	var parts []string
	for _, l := range last.Lag {
		if l.Committed < 0 {
			parts = append(parts, tp{l.Topic, l.Partition}.String())
		}
	}
	if len(parts) == 0 {
		return nil
	}
	return []Finding{{
		Severity: SeverityInfo,
		Rule:     RuleUncommittedPartitions,
		Title:    fmt.Sprintf("%d partitions have no committed offset", len(parts)),
		Detail: "No offset is committed for " + strings.Join(parts, ", ") +
			"; after a restart the consumer starts from auto.offset.reset, which may skip or replay messages.",
		Fix: "Check that the consumer commits (enable.auto.commit or explicit commits), " +
			"or set a starting point with `mqx group reset-offsets`.",
	}}
}

// unevenAssignment reports members with very different partition counts.
func unevenAssignment(_, last Snapshot, _ []Snapshot) []Finding {
	members := last.Group.Members
	if len(members) < 2 || isRebalancing(last.Group.State) {
		return nil
	}
	lo, hi := -1, 0
	var heavy []string
	for _, m := range members {
		n := len(owned(m))
		if lo < 0 || n < lo {
			lo = n
		}
		hi = max(hi, n)
	}
	if hi-lo <= 1 || lo == 0 && moreMembersThanPartitions(last, last, nil) != nil {
		return nil
	}
	for _, m := range members {
		if len(owned(m)) == hi {
			heavy = append(heavy, m.MemberID)
		}
	}
	return []Finding{{
		Severity: SeverityInfo,
		Rule:     RuleUnevenAssignment,
		Title:    fmt.Sprintf("assignment is uneven: %d to %d partitions per member", lo, hi),
		Detail:   "Some members do far more work than others, which caps throughput at the busiest member.",
		Fix: "With several topics prefer the cooperative-sticky or round-robin assignor over range, " +
			"or make the partition count a multiple of the member count.",
		Members: heavy,
	}}
}
