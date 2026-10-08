package cli

import (
	"runtime"
	"strings"
	"testing"
)

func TestBuildString(t *testing.T) {
	got := build{version: "v1.2.3", commit: "abc123", date: "2026-10-07"}.String()
	want := "mqx v1.2.3 (commit abc123, built 2026-10-07, " + runtime.GOOS + "/" + runtime.GOARCH + ")"
	if got != want {
		t.Errorf("build.String() = %q, want %q", got, want)
	}
}

func TestVersionCmd(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantPrefix string
		wantErr    string
	}{
		{name: "prints version", args: []string{"version"}, wantPrefix: "mqx "},
		{name: "rejects extra args", args: []string{"version", "extra"}, wantErr: `unknown command "extra"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stdout, _, err := run(t, tt.args...)
			checkErr(t, err, tt.wantErr)
			if !strings.HasPrefix(stdout, tt.wantPrefix) {
				t.Errorf("stdout = %q, want prefix %q", stdout, tt.wantPrefix)
			}
			if tt.wantErr == "" && !strings.Contains(stdout, runtime.GOOS+"/"+runtime.GOARCH) {
				t.Errorf("stdout = %q, want platform %s/%s", stdout, runtime.GOOS, runtime.GOARCH)
			}
		})
	}
}
