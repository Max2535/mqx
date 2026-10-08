package tui

import (
	"slices"
	"strings"
	"testing"
)

func TestParseKVAcceptsJSONObject(t *testing.T) {
	tests := []struct {
		in      string
		want    [][2]string
		wantErr string
	}{
		{in: "a=1\nb = two, c=", want: [][2]string{{"a", "1"}, {"b", "two"}, {"c", ""}}},
		{in: ` {"name":"Max", "n": 1, "ok": true, "o": {"a": [1, 2]}, "z": null} `,
			want: [][2]string{{"name", "Max"}, {"n", "1"}, {"ok", "true"}, {"o", `{"a":[1,2]}`}, {"z", ""}}},
		{in: `{"b":"x,y","a":"1=2"}`, want: [][2]string{{"b", "x,y"}, {"a", "1=2"}}}, // commas and = stay in values
		{in: "{}", want: nil},
		{in: `{"name":"Max"`, wantErr: "invalid JSON object"},
		{in: `{"a":1} b=2`, wantErr: "unexpected text after"},
		{in: `{"":1}`, wantErr: "empty key"},
		{in: "{\n\"name\":\"Max\"\n}\n", want: [][2]string{{"name", "Max"}}},
		{in: "nokey", wantErr: "want key=value or a JSON object"},
	}
	for _, tt := range tests {
		got, err := parseKV(tt.in)
		if tt.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("parseKV(%q) error = %v, want %q", tt.in, err, tt.wantErr)
			}
			continue
		}
		if err != nil || !slices.Equal(got, tt.want) {
			t.Errorf("parseKV(%q) = %q, %v; want %q", tt.in, got, err, tt.want)
		}
	}
}

func TestFuzzyScore(t *testing.T) {
	if _, ok := fuzzyScore("ntp", "new topic"); !ok {
		t.Error("ntp should match new topic")
	}
	if _, ok := fuzzyScore("pub ord", "publish orders"); !ok {
		t.Error("each word should match on its own")
	}
	if _, ok := fuzzyScore("xyz", "new topic"); ok {
		t.Error("xyz should not match")
	}
	start, _ := fuzzyScore("grp", "Groups go to")
	mid, _ := fuzzyScore("grp", "big report")
	if start <= mid {
		t.Errorf("word-start match %d should beat scattered match %d", start, mid)
	}
}

func TestPaletteRunsActionsThroughTheirKeys(t *testing.T) {
	f, c := fakeCtx(t, "dev", false, kafkaCaps)
	seedKafka(f)
	_, prod := fakeCtx(t, "prod", true, kafkaCaps)
	h := newHarness(t, Options{}, c, prod)

	h.keys(":")
	h.wantView("Commands", "new topic", "go to Groups", "orders")
	h.typeText("new top")
	h.keys("enter")
	h.wantView("New topic", "Partitions") // the same form the n key opens
	h.keys("esc")

	// Recently run commands come first.
	h.keys(":")
	if p := h.a.overlay.(*palette); p.shown[0].label != "new topic" {
		t.Errorf("first entry = %q, want the last command run", p.shown[0].label)
	}
	h.keys("esc")

	// A row of the current list.
	h.keys(":")
	h.typeText("payments")
	h.keys("enter")
	if h.a.e.topic != "payments" {
		t.Errorf("selected topic = %q, want payments", h.a.e.topic)
	}

	h.keys(":")
	h.typeText("go groups")
	h.keys("enter")
	if got := h.a.panels[h.a.active].Title(); got != "Groups" {
		t.Errorf("active panel = %q, want Groups", got)
	}

	// The palette goes through the guard: a read_only context offers no mutations.
	h.keys(":")
	h.typeText("switch prod")
	h.wantView("switch to prod")
	h.keys("enter")
	h.wantStatus("connected to prod", false)
	h.gotoPanel("Topics")
	h.keys(":")
	h.typeText("new topic")
	h.wantView("no matching commands")
	h.keys("esc")
	if h.a.overlay != nil {
		t.Errorf("esc left %T open", h.a.overlay)
	}
}

func TestFormRecallsEarlierSubmissions(t *testing.T) {
	f, c := fakeCtx(t, "dev", false, kafkaCaps)
	seedKafka(f)
	h := newHarness(t, Options{}, c)

	h.keys("p")
	h.wantNotView("ctrl+r previous")
	h.typeText("k-1")
	focusField(h, "headers")
	h.typeText(`{"name":"Max"}`)
	focusField(h, "payload")
	h.typeText(`{"message":"test"}`)
	h.keys("ctrl+s", "y")
	h.wantStatus("published", false)
	msgs := f.Messages("orders", 0)
	if last := msgs[len(msgs)-1]; len(last.Headers) != 1 || last.Headers[0].Key != "name" || string(last.Headers[0].Value) != "Max" {
		t.Errorf("JSON headers published as %+v", last.Headers)
	}

	h.keys("p")
	h.wantView("ctrl+r previous (1/1)")
	h.keys("ctrl+r")
	h.wantView(`{"message":"test"}`, `{"name":"Max"}`, "k-1")
	h.keys("ctrl+s")
	h.wantView("Publish 18 bytes to topic orders")
}

func TestPublishToInternalTopicNeedsTypedName(t *testing.T) {
	f, c := fakeCtx(t, "dev", false, kafkaCaps)
	seedKafka(f)
	f.AddTopic("__transaction_state", 1)
	h := newHarness(t, Options{}, c)

	h.keys(":")
	h.typeText("__transaction_state")
	h.keys("enter", "p")
	h.wantView("Publish to topic __transaction_state")
	focusField(h, "payload")
	h.typeText("x")
	h.keys("ctrl+s")
	h.wantView("internal topic that the broker manages itself", "Type __transaction_state to confirm")

	h.keys("y", "enter") // y is just text here
	h.wantView("does not match")
	wantCalls(t, f)
	h.keys("backspace")
	h.typeText("__transaction_state")
	h.keys("enter")
	wantCalls(t, f, "Publish __transaction_state 0 x")
	h.wantStatus("published", false)

	// Ordinary topics keep the one-key confirmation.
	h.keys("esc", ":")
	h.typeText("orders")
	h.keys("enter", "p")
	focusField(h, "payload")
	h.typeText("x")
	h.keys("ctrl+s")
	h.wantNotView("Type orders to confirm")
	h.wantView("Confirm")
}
