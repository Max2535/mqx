package cli

import (
	"bytes"
	"strings"
	"testing"
)

// run executes a fresh command tree in-process and captures its output.
func run(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	cmd := NewRootCmd()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

// checkErr fails unless err is nil (want == "") or contains want.
func checkErr(t *testing.T, err error, want string) {
	t.Helper()
	if want == "" {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return
	}
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %v, want containing %q", err, want)
	}
}

func TestOptionsPath(t *testing.T) {
	t.Setenv("MQX_CONFIG", "/from/env.yaml")
	tests := []struct {
		name string
		flag string
		want string
	}{
		{name: "flag wins over env", flag: "/from/flag.yaml", want: "/from/flag.yaml"},
		{name: "env when no flag", flag: "", want: "/from/env.yaml"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := (&options{configPath: tt.flag}).path()
			if err != nil {
				t.Fatalf("path() error: %v", err)
			}
			if got != tt.want {
				t.Errorf("path() = %q, want %q", got, tt.want)
			}
		})
	}
}
