package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Max2535/mqx/internal/config"
)

const sampleConfig = `current-context: local
contexts:
  - name: local
    broker: kafka
    brokers: ["localhost:9092", "localhost:9093"]
  - name: rabbit
    broker: rabbitmq
    url: amqp://guest@localhost:5672/
    read_only: true
`

// configFile writes content to a temp config and returns its path.
// Empty content means the file does not exist.
func configFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if content != "" {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return path
}

func TestCtxList(t *testing.T) {
	tests := []struct {
		name       string
		content    string
		wantLines  [][]string // stdout lines split into fields
		wantErr    string
		wantStderr string
	}{
		{
			name:    "marks current context",
			content: sampleConfig,
			wantLines: [][]string{
				{"CURRENT", "NAME", "BROKER", "MODE", "ENDPOINT"},
				{"*", "local", "kafka", "rw", "localhost:9092,localhost:9093"},
				{"rabbit", "rabbitmq", "ro", "amqp://guest@localhost:5672/"},
			},
		},
		{name: "missing file is a hint, not an error", content: "", wantStderr: "no config at"},
		{
			name:    "invalid config lists redacted then fails",
			content: "contexts:\n  - name: leaky\n    broker: rabbitmq\n    url: amqp://guest:s3cret@localhost:5672/\n",
			wantLines: [][]string{
				{"CURRENT", "NAME", "BROKER", "MODE", "ENDPOINT"},
				{"leaky", "rabbitmq", "rw", "amqp://guest:xxxxx@localhost:5672/"},
			},
			wantErr: "must not embed a password",
		},
		{
			name:    "host-less url is not echoed",
			content: "contexts:\n  - name: sneaky\n    broker: rabbitmq\n    url: guest:s3cret@localhost:5672\n",
			wantLines: [][]string{
				{"CURRENT", "NAME", "BROKER", "MODE", "ENDPOINT"},
				{"sneaky", "rabbitmq", "rw", "<invalid", "url>"},
			},
			wantErr: "must be an absolute URL with a host",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := configFile(t, tt.content)
			stdout, stderr, err := run(t, "--config", path, "ctx", "list")
			checkErr(t, err, tt.wantErr)
			var got [][]string
			for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
				if line != "" {
					got = append(got, strings.Fields(line))
				}
			}
			if !reflect.DeepEqual(got, tt.wantLines) {
				t.Errorf("stdout fields = %q, want %q", got, tt.wantLines)
			}
			if !strings.Contains(stderr, tt.wantStderr) {
				t.Errorf("stderr = %q, want containing %q", stderr, tt.wantStderr)
			}
			if strings.Contains(stdout+stderr, "s3cret") {
				t.Errorf("output leaks secret:\nstdout: %s\nstderr: %s", stdout, stderr)
			}
		})
	}
}

func TestCtxUse(t *testing.T) {
	tests := []struct {
		name        string
		content     string
		args        []string
		wantOut     string
		wantErr     string
		wantCurrent string
		wantUsage   bool
	}{
		{
			name:        "switches context",
			content:     sampleConfig,
			args:        []string{"rabbit"},
			wantOut:     `Switched to context "rabbit".`,
			wantCurrent: "rabbit",
		},
		{
			name:    "unknown name leaves file unchanged",
			content: sampleConfig,
			args:    []string{"nope"},
			wantErr: `context "nope" not found; available: local, rabbit`,
		},
		{
			name:    "invalid config leaves file unchanged",
			content: "contexts:\n  - name: a\n    broker: nats\n",
			args:    []string{"a"},
			wantErr: `unknown broker "nats"`,
		},
		{name: "missing file", content: "", args: []string{"local"}, wantErr: "no config at"},
		{
			name:      "requires a name",
			content:   sampleConfig,
			args:      nil,
			wantErr:   "accepts 1 arg(s), received 0",
			wantUsage: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := configFile(t, tt.content)
			before, _ := os.ReadFile(path) // nil when the file is missing
			stdout, stderr, err := run(t, append([]string{"--config", path, "ctx", "use"}, tt.args...)...)
			checkErr(t, err, tt.wantErr)
			if !strings.Contains(stdout, tt.wantOut) {
				t.Errorf("stdout = %q, want containing %q", stdout, tt.wantOut)
			}
			if gotUsage := strings.Contains(stdout+stderr, "Usage:"); gotUsage != tt.wantUsage {
				t.Errorf("usage printed = %v, want %v", gotUsage, tt.wantUsage)
			}
			after, _ := os.ReadFile(path)
			if tt.wantErr != "" {
				if !bytes.Equal(before, after) {
					t.Errorf("config file changed on failure:\nbefore: %s\nafter: %s", before, after)
				}
				return
			}
			cfg, err := config.Load(path)
			if err != nil {
				t.Fatalf("reload config: %v", err)
			}
			if cfg.CurrentContext != tt.wantCurrent {
				t.Errorf("current-context = %q, want %q", cfg.CurrentContext, tt.wantCurrent)
			}
		})
	}
}
