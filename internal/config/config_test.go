package config

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

// writeConfig writes content to a fresh temp config file and returns its path.
func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoad(t *testing.T) {
	tests := []struct {
		name    string
		content string
		missing bool
		want    *Config
		wantIs  error
		wantErr string
	}{
		{
			name: "valid",
			content: `current-context: a
contexts:
  - name: a
    broker: kafka
    brokers: ["localhost:9092"]
  - name: b
    broker: rabbitmq
    url: amqp://localhost:5672/
    management_url: http://localhost:15672
    username_env: RMQ_USER
    password_env: RMQ_PASS
  - name: prod
    broker: kafka
    brokers: ["prod:9092"]
    read_only: true
`,
			want: &Config{CurrentContext: "a", Contexts: []Context{
				{Name: "a", Broker: "kafka", Brokers: []string{"localhost:9092"}},
				{Name: "b", Broker: "rabbitmq", URL: "amqp://localhost:5672/", ManagementURL: "http://localhost:15672",
					UsernameEnv: "RMQ_USER", PasswordEnv: "RMQ_PASS"},
				{Name: "prod", Broker: "kafka", Brokers: []string{"prod:9092"}, ReadOnly: true},
			}},
		},
		{name: "empty file", content: "", want: &Config{}},
		{name: "missing file", missing: true, wantIs: fs.ErrNotExist},
		{name: "malformed yaml", content: "contexts: [", wantErr: "parse config"},
		{name: "unknown key", content: "contexts:\n  - name: a\n    colour: red\n", wantErr: "field colour not found"},
		{name: "literal password", content: "contexts:\n  - name: a\n    password: hunter2\n", wantErr: "password_env"},
		{name: "literal username", content: "contexts:\n  - name: a\n    username: hunter2\n", wantErr: "username_env"},
		{name: "read_only not a bool", content: "contexts:\n  - name: a\n    read_only: maybe\n", wantErr: "cannot unmarshal"},
		{name: "type error does not echo value", content: "contexts:\n  - name: a\n    read_only: s3cret\n", wantErr: "cannot unmarshal"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "absent.yaml")
			if !tt.missing {
				path = writeConfig(t, tt.content)
			}
			got, err := Load(path)
			switch {
			case tt.wantIs != nil:
				if !errors.Is(err, tt.wantIs) {
					t.Fatalf("Load() error = %v, want errors.Is %v", err, tt.wantIs)
				}
				return
			case tt.wantErr != "":
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Load() error = %v, want containing %q", err, tt.wantErr)
				}
				for _, secret := range []string{"hunter2", "s3cret"} {
					if strings.Contains(err.Error(), secret) {
						t.Errorf("Load() error leaks the literal value: %v", err)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() unexpected error: %v", err)
			}
			if !sameConfig(got, tt.want) {
				t.Errorf("Load() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestSaveRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "dir", "config.yaml")
	cfg := &Config{Contexts: []Context{
		{Name: "a", Broker: "kafka", Brokers: []string{"localhost:9092"}, ReadOnly: true},
		{Name: "b", Broker: "rabbitmq", URL: "amqp://localhost:5672/", ManagementURL: "http://localhost:15672",
			UsernameEnv: "U", PasswordEnv: "P"},
	}}
	// The second pass overwrites an existing file.
	for _, current := range []string{"a", "b"} {
		cfg.CurrentContext = current
		if err := cfg.Save(path); err != nil {
			t.Fatalf("Save() error: %v", err)
		}
		got, err := Load(path)
		if err != nil {
			t.Fatalf("Load() error: %v", err)
		}
		if !sameConfig(got, cfg) {
			t.Fatalf("round trip = %+v, want %+v", got, cfg)
		}
	}
	if runtime.GOOS != "windows" { // Windows has no POSIX permission bits.
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if perm := info.Mode().Perm(); perm != 0o600 {
			t.Errorf("config mode = %o, want 600", perm)
		}
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Errorf("config dir has %d entries, want only config.yaml (temp file left behind?)", len(entries))
	}
}

// sameConfig compares the data of two configs, ignoring the loaded YAML tree.
func sameConfig(a, b *Config) bool {
	return a.CurrentContext == b.CurrentContext && reflect.DeepEqual(a.Contexts, b.Contexts)
}

func TestDefaultPath(t *testing.T) {
	home := t.TempDir()
	tests := []struct {
		name string
		env  string
		want string
	}{
		{name: "MQX_CONFIG wins", env: filepath.Join(home, "custom.yaml"), want: filepath.Join(home, "custom.yaml")},
		{name: "falls back to home", env: "", want: filepath.Join(home, ".config", "mqx", "config.yaml")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("HOME", home)        // Unix
			t.Setenv("USERPROFILE", home) // Windows
			t.Setenv(EnvConfig, tt.env)
			got, err := DefaultPath()
			if err != nil {
				t.Fatalf("DefaultPath() error: %v", err)
			}
			if got != tt.want {
				t.Errorf("DefaultPath() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestUse(t *testing.T) {
	tests := []struct {
		name        string
		cfg         Config
		use         string
		wantCurrent string
		wantErr     string
	}{
		{name: "switch", cfg: Config{CurrentContext: "a", Contexts: []Context{kafkaCtx("a"), kafkaCtx("b")}}, use: "b", wantCurrent: "b"},
		{name: "same context", cfg: Config{CurrentContext: "a", Contexts: []Context{kafkaCtx("a")}}, use: "a", wantCurrent: "a"},
		{
			name:        "unknown name lists available",
			cfg:         Config{CurrentContext: "a", Contexts: []Context{kafkaCtx("a"), kafkaCtx("b")}},
			use:         "zzz",
			wantCurrent: "a",
			wantErr:     `context "zzz" not found; available: a, b`,
		},
		{name: "no contexts", cfg: Config{}, use: "a", wantErr: `context "a" not found; no contexts defined in config`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Use(tt.use)
			if tt.wantErr != "" {
				if err == nil || err.Error() != tt.wantErr {
					t.Fatalf("Use() error = %v, want %q", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("Use() unexpected error: %v", err)
			}
			if tt.cfg.CurrentContext != tt.wantCurrent {
				t.Errorf("CurrentContext = %q, want %q", tt.cfg.CurrentContext, tt.wantCurrent)
			}
		})
	}
}

func TestSaveThroughSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "real.yaml")
	link := filepath.Join(dir, "link.yaml")
	if err := os.WriteFile(target, []byte("contexts: []\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("cannot create symlink: %v", err)
	}
	cfg := &Config{CurrentContext: "a", Contexts: []Context{{Name: "a", Broker: "kafka", Brokers: []string{"h:9092"}}}}
	if err := cfg.Save(link); err != nil {
		t.Fatalf("Save() error: %v", err)
	}
	if fi, err := os.Lstat(link); err != nil || fi.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("link is no longer a symlink: %v, %v", fi, err)
	}
	got, err := Load(target)
	if err != nil || got.CurrentContext != "a" {
		t.Errorf("target not updated: %+v, %v", got, err)
	}
}
