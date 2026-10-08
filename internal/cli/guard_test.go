package cli

import (
	"errors"
	"strings"
	"testing"
)

func TestGuard(t *testing.T) {
	tests := []struct {
		name      string
		readOnly  bool
		tty       bool
		args      []string
		stdin     string
		wantErr   error
		wantCalls int
		wantPromp bool
	}{
		{name: "read_only refuses even with --yes", readOnly: true, args: []string{"--yes"}, wantErr: ErrReadOnly},
		{name: "read_only refuses on a terminal", readOnly: true, tty: true, stdin: "y\n", wantErr: ErrReadOnly},
		{name: "no terminal needs --yes", wantErr: ErrNotConfirmed},
		{name: "--yes confirms", args: []string{"--yes"}, wantCalls: 1},
		{name: "-y confirms", args: []string{"-y"}, wantCalls: 1},
		{name: "terminal prompt accepts y", tty: true, stdin: "y\n", wantCalls: 1, wantPromp: true},
		{name: "terminal prompt accepts YES", tty: true, stdin: "YES\n", wantCalls: 1, wantPromp: true},
		{name: "terminal prompt defaults to no", tty: true, stdin: "\n", wantErr: ErrNotConfirmed, wantPromp: true},
		{name: "terminal prompt EOF is no", tty: true, stdin: "", wantErr: ErrNotConfirmed, wantPromp: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newFakeEnv(t, tt.readOnly)
			e.f.AddTopic("orders", 1)
			args := append([]string{"--config", e.path, "topic", "delete", "orders"}, tt.args...)
			_, stderr, err := runWith(t, []Option{withTerminal(tt.tty)}, strings.NewReader(tt.stdin), args...)
			if tt.wantErr == nil && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			}
			if len(e.f.Calls) != tt.wantCalls {
				t.Errorf("calls = %v, want %d", e.f.Calls, tt.wantCalls)
			}
			if got := strings.Contains(stderr, "Continue? [y/N]"); got != tt.wantPromp {
				t.Errorf("prompted = %v, want %v (stderr %q)", got, tt.wantPromp, stderr)
			}
		})
	}
}

func TestGuardMessageNamesActionAndContext(t *testing.T) {
	e := newFakeEnv(t, true)
	e.f.AddTopic("orders", 1)
	_, _, err := e.run(t, "topic", "delete", "orders", "--yes")
	if err == nil || !strings.Contains(err.Error(), `refusing to delete orders`) || !strings.Contains(err.Error(), `context "fake" is read_only`) {
		t.Fatalf("error = %v", err)
	}
}
