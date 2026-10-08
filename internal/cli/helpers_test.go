package cli

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/Max2535/mqx/internal/config"
	"github.com/Max2535/mqx/internal/testutil/fakebroker"
)

// fakeEnv is a fake broker plus a config file whose current context opens it.
type fakeEnv struct {
	f    *fakebroker.Fake
	path string
}

// newFakeEnv registers a fake broker; readOnly marks its context read_only.
func newFakeEnv(t *testing.T, readOnly bool) fakeEnv {
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
	return fakeEnv{f: f, path: path}
}

// run executes mqx against the fake's config.
func (e fakeEnv) run(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	return runWith(t, nil, nil, append([]string{"--config", e.path}, args...)...)
}

// runWith executes a fresh command tree with options and stdin.
func runWith(t *testing.T, opts []Option, in io.Reader, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	cmd := NewRootCmd(append([]Option{withTerminal(false)}, opts...)...)
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	if in != nil {
		cmd.SetIn(in)
	} else {
		cmd.SetIn(strings.NewReader(""))
	}
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

// fields splits output into lines of whitespace-separated fields.
func fields(s string) [][]string {
	var out [][]string
	for _, line := range strings.Split(strings.TrimSpace(s), "\n") {
		if line != "" {
			out = append(out, strings.Fields(line))
		}
	}
	return out
}
