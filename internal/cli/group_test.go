package cli

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/config"
	"github.com/Max2535/mqx/internal/groupdiag"
	"github.com/Max2535/mqx/internal/testutil/fakebroker"
)

// kafkaCLIEnv is a fake broker behind a config file, for the Kafka ecosystem commands.
type kafkaCLIEnv struct {
	f    *fakebroker.Fake
	path string
}

func newKafkaCLIEnv(t *testing.T, readOnly bool) kafkaCLIEnv {
	t.Helper()
	f, c := fakebroker.New(t)
	c.ReadOnly = readOnly
	data, err := yaml.Marshal(config.Config{CurrentContext: c.Name, Contexts: []config.Context{c}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return kafkaCLIEnv{f: f, path: path}
}

func (e kafkaCLIEnv) mqx(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	return run(t, append([]string{"--config", e.path}, args...)...)
}

// seedGroups adds topic orders (2 partitions, 5+3 messages), a Stable group
// billing with one member and an Empty group archive.
func seedGroups(e kafkaCLIEnv) {
	e.f.AddTopic("orders", 2)
	for range 5 {
		e.f.AddMessages("orders", 0, broker.NewMessage([]byte("m")))
	}
	for range 3 {
		e.f.AddMessages("orders", 1, broker.NewMessage([]byte("m")))
	}
	e.f.AddGroup(broker.GroupDescription{
		Name: "billing", State: "Stable", ProtocolType: "consumer", GroupProtocol: "classic", Assignor: "range", Epoch: -1,
		Coordinator: broker.Node{ID: "1", Host: "kafka-1", Port: 9092},
		Members: []broker.GroupMember{{
			MemberID: "m-1", InstanceID: "pod-1", ClientID: "billing-app", Host: "/10.0.0.7", Subscriptions: []string{"orders"},
			Assignment: []broker.TopicPartitions{{Topic: "orders", Partitions: []int32{0, 1}}},
		}},
	}, map[string]map[int32]int64{"orders": {0: 2, 1: 3}})
	e.f.AddGroup(broker.GroupDescription{Name: "archive", State: "Empty", ProtocolType: "consumer", GroupProtocol: "consumer", Epoch: 4},
		map[string]map[int32]int64{"orders": {0: 1}})
}

func TestGroupsAndDescribe(t *testing.T) {
	e := newKafkaCLIEnv(t, true)
	seedGroups(e)
	tests := []struct {
		name string
		args []string
		want [][]string // leading fields of lines
	}{
		{name: "groups", args: []string{"groups"}, want: [][]string{
			{"NAME", "STATE", "PROTOCOL", "MEMBERS"}, {"archive", "Empty", "consumer", "0"}, {"billing", "Stable", "classic", "1"}}},
		{name: "describe", args: []string{"group", "describe", "billing"}, want: [][]string{
			{"Group:", "billing"}, {"State:", "Stable"}, {"Coordinator:", "1", "(kafka-1:9092)"}, {"Protocol", "type:", "consumer"},
			{"Group", "protocol:", "classic"}, {"Assignor:", "range"}, {"Epoch:", "-"}, {"Members:", "1"},
			{"MEMBER", "INSTANCE", "CLIENT", "HOST", "SUBSCRIPTIONS", "ASSIGNMENT"},
			{"m-1", "pod-1", "billing-app", "/10.0.0.7", "orders", "orders[0,1]"}}},
		{name: "lag", args: []string{"group", "lag", "billing"}, want: [][]string{
			{"TOPIC", "PARTITION", "COMMITTED", "END", "LAG", "MEMBER", "CLIENT", "HOST"},
			{"orders", "0", "2", "5", "3", "m-1", "billing-app", "/10.0.0.7"},
			{"orders", "1", "3", "3", "0", "m-1", "billing-app", "/10.0.0.7"},
			{"Total", "lag:", "3"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, _, err := e.mqx(t, tt.args...)
			checkErr(t, err, "")
			var got [][]string
			for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
				if f := strings.Fields(line); len(f) > 0 {
					got = append(got, f)
				}
			}
			if len(got) != len(tt.want) {
				t.Fatalf("output:\n%s", out)
			}
			for i, w := range tt.want {
				if !slices.Equal(got[i][:min(len(w), len(got[i]))], w) {
					t.Errorf("line %d = %q, want prefix %q", i, got[i], w)
				}
			}
		})
	}
}

func TestGroupJSON(t *testing.T) {
	e := newKafkaCLIEnv(t, true)
	seedGroups(e)
	out, _, err := e.mqx(t, "group", "lag", "billing", "-o", "json")
	checkErr(t, err, "")
	var lag groupLag
	if err := json.Unmarshal([]byte(out), &lag); err != nil || lag.TotalLag != 3 || len(lag.Partitions) != 2 {
		t.Errorf("lag json = %s (%v)", out, err)
	}
	out, _, err = e.mqx(t, "group", "describe", "archive", "-o", "json")
	checkErr(t, err, "")
	var d broker.GroupDescription
	if err := json.Unmarshal([]byte(out), &d); err != nil || d.Name != "archive" || d.Epoch != 4 {
		t.Errorf("describe json = %s (%v)", out, err)
	}
	out, _, err = e.mqx(t, "groups", "-o", "json")
	checkErr(t, err, "")
	var groups []broker.GroupSummary
	if err := json.Unmarshal([]byte(out), &groups); err != nil || len(groups) != 2 {
		t.Errorf("groups json = %s (%v)", out, err)
	}
	_, _, err = e.mqx(t, "group", "describe", "nope")
	if !errors.Is(err, broker.ErrNotFound) {
		t.Errorf("missing group: err = %v", err)
	}
}

func TestGroupWatch(t *testing.T) {
	e := newKafkaCLIEnv(t, true)
	seedGroups(e)
	out, _, err := e.mqx(t, "group", "watch", "billing", "--interval", "1ms", "--count", "2")
	checkErr(t, err, "")
	if lines := strings.Split(strings.TrimSpace(out), "\n"); len(lines) != 1 || !strings.Contains(lines[0], "group billing is Stable with 1 members") {
		t.Errorf("watch output:\n%s", out)
	}
	out, _, err = e.mqx(t, "group", "watch", "archive", "--interval", "1ms", "--count", "1", "-o", "json")
	checkErr(t, err, "")
	var ev groupdiag.Event
	if err := json.Unmarshal([]byte(out), &ev); err != nil || ev.Kind != groupdiag.EventState || ev.To != "Empty" {
		t.Errorf("watch json = %s (%v)", out, err)
	}
	_, _, err = e.mqx(t, "group", "watch", "billing", "--interval", "0s")
	checkErr(t, err, "--interval must be positive")
}

func TestGroupDiagnose(t *testing.T) {
	e := newKafkaCLIEnv(t, true)
	seedGroups(e)
	out, _, err := e.mqx(t, "group", "diagnose", "billing", "--samples", "1")
	checkErr(t, err, "")
	if strings.TrimSpace(out) != "No problems found in group billing." {
		t.Errorf("diagnose healthy:\n%s", out)
	}
	out, _, err = e.mqx(t, "group", "diagnose", "archive", "--samples", "2", "--interval", "1ms")
	checkErr(t, err, "")
	if !strings.Contains(out, "[warning] empty-with-lag: no members, 4 messages waiting") || !strings.Contains(out, "Fix: ") {
		t.Errorf("diagnose archive:\n%s", out)
	}
	out, _, err = e.mqx(t, "group", "diagnose", "archive", "--samples", "1", "-o", "json")
	checkErr(t, err, "")
	var fs []groupdiag.Finding
	if err := json.Unmarshal([]byte(out), &fs); err != nil || len(fs) == 0 || fs[0].Rule != groupdiag.RuleEmptyWithLag {
		t.Errorf("diagnose json = %s (%v)", out, err)
	}
	out, _, err = e.mqx(t, "group", "diagnose", "billing", "--samples", "1", "-o", "json")
	checkErr(t, err, "")
	if strings.TrimSpace(out) != "[]" {
		t.Errorf("no findings json = %s", out)
	}
	_, _, err = e.mqx(t, "group", "diagnose", "billing", "--samples", "0")
	checkErr(t, err, "--samples must be at least 1")
}

func TestGroupMutations(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		readOnly  bool
		wantErr   string
		wantCalls []string
		wantOut   []string
	}{
		{name: "dry run needs no guard on read-only", readOnly: true,
			args:    []string{"group", "reset-offsets", "archive", "--topic", "orders", "--to", "earliest", "--dry-run"},
			wantOut: []string{"TOPIC PARTITION OLD NEW CHANGE", "orders 0 1 0 -1", "orders 1 - 0 -"}},
		{name: "reset refused on read-only", readOnly: true,
			args:    []string{"group", "reset-offsets", "archive", "--topic", "orders", "--to", "earliest", "--yes"},
			wantErr: "read_only"},
		{name: "reset needs confirmation", args: []string{"group", "reset-offsets", "archive", "--topic", "orders", "--to", "earliest"},
			wantErr: "without confirmation"},
		{name: "reset with --yes", args: []string{"group", "reset-offsets", "archive", "--topic", "orders", "--shift", "-2", "--partitions", "0", "--yes"},
			wantCalls: []string{"ResetOffsets archive orders"}, wantOut: []string{"TOPIC PARTITION OLD NEW CHANGE", "orders 0 1 0 -1"}},
		{name: "reset needs one of to and shift", args: []string{"group", "reset-offsets", "archive", "--topic", "orders"},
			wantErr: "exactly one of --to or --shift"},
		{name: "reset needs a topic", args: []string{"group", "reset-offsets", "archive", "--to", "latest"}, wantErr: "--topic is required"},
		{name: "reset bad position", args: []string{"group", "reset-offsets", "archive", "--topic", "orders", "--to", "soon"},
			wantErr: "invalid position"},
		{name: "reset with members fails", args: []string{"group", "reset-offsets", "billing", "--topic", "orders", "--to", "latest", "--yes"},
			wantErr: "active members"},
		{name: "delete refused on read-only", readOnly: true, args: []string{"group", "delete", "archive", "--yes"}, wantErr: "read_only"},
		{name: "delete", args: []string{"group", "delete", "archive", "--yes"}, wantCalls: []string{"DeleteGroup archive"},
			wantOut: []string{"Deleted group archive."}},
		{name: "remove member refused on read-only", readOnly: true,
			args: []string{"group", "remove-member", "billing", "--instance-id", "pod-1", "--yes"}, wantErr: "read_only"},
		{name: "remove member", args: []string{"group", "remove-member", "billing", "--instance-id", "pod-1", "--reason", "gone", "--yes"},
			wantCalls: []string{"RemoveMember billing pod-1"}, wantOut: []string{"Removed member pod-1 from group billing."}},
		{name: "remove member needs instance id", args: []string{"group", "remove-member", "billing", "--yes"}, wantErr: "--instance-id is required"},
		{name: "remove unknown member", args: []string{"group", "remove-member", "billing", "--instance-id", "pod-9", "--yes"}, wantErr: "not found"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newKafkaCLIEnv(t, tt.readOnly)
			seedGroups(e)
			out, _, err := e.mqx(t, tt.args...)
			checkErr(t, err, tt.wantErr)
			if !slices.Equal(e.f.Calls, tt.wantCalls) {
				t.Errorf("calls = %q, want %q", e.f.Calls, tt.wantCalls)
			}
			var got []string
			for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
				if line != "" {
					got = append(got, strings.Join(strings.Fields(line), " "))
				}
			}
			if tt.wantOut != nil && !slices.Equal(got, tt.wantOut) {
				t.Errorf("output = %q, want %q", got, tt.wantOut)
			}
		})
	}
}

func TestGroupDeleteConfirmsOnTTY(t *testing.T) {
	for _, tt := range []struct {
		answer    string
		wantErr   string
		wantCalls []string
	}{
		{answer: "y\n", wantCalls: []string{"DeleteGroup archive"}},
		{answer: "n\n", wantErr: "not confirmed"},
	} {
		e := newKafkaCLIEnv(t, false)
		seedGroups(e)
		cmd := NewRootCmd(withTerminal(true))
		var out, errOut strings.Builder
		cmd.SetOut(&out)
		cmd.SetErr(&errOut)
		cmd.SetIn(strings.NewReader(tt.answer))
		cmd.SetArgs([]string{"--config", e.path, "group", "delete", "archive"})
		checkErr(t, cmd.Execute(), tt.wantErr)
		if !strings.Contains(errOut.String(), `About to delete group archive on context "fake"`) {
			t.Errorf("prompt = %q", errOut.String())
		}
		if !slices.Equal(e.f.Calls, tt.wantCalls) {
			t.Errorf("answer %q: calls = %q", tt.answer, e.f.Calls)
		}
	}
}

func TestGroupCommandsNeedCapability(t *testing.T) {
	e := newKafkaCLIEnv(t, false)
	seedGroups(e)
	e.f.Caps = []string{}
	for _, args := range [][]string{{"groups"}, {"group", "lag", "billing"}, {"group", "delete", "x", "--yes"}} {
		_, _, err := e.mqx(t, args...)
		if !errors.Is(err, broker.ErrUnsupported) {
			t.Errorf("%v: err = %v, want ErrUnsupported", args, err)
		}
	}
	_, _, err := e.mqx(t, "group", "diagnose", "billing", "--samples", "1")
	checkErr(t, err, "cannot diagnose groups")
}
