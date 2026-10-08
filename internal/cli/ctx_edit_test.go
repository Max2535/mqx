package cli

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Max2535/mqx/internal/config"
)

func readConfig(t *testing.T, path string) (*config.Config, string) {
	t.Helper()
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return cfg, string(data)
}

func TestCtxAdd(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "config.yaml")
	run := func(args ...string) (string, error) {
		out, _, err := runWith(t, nil, nil, append([]string{"--config", path}, args...)...)
		return out, err
	}

	// The first context creates the file and becomes current.
	out, err := run("ctx", "add", "local", "--broker", "kafka", "--brokers", "localhost:9092")
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	if !strings.Contains(out, `Added context "local"`) || !strings.Contains(out, "It is the current context") {
		t.Errorf("output = %q", out)
	}

	if err := os.WriteFile(path, []byte("# team contexts\n"+mustRead(t, path)), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = run("ctx", "add", "prod", "--broker", "kafka", "--brokers", "k1:9093, k2:9093",
		"--sasl-mechanism", "SCRAM-SHA-512", "--username-env", "PROD_USER", "--password-env", "PROD_PASS",
		"--tls-enabled", "--tls-ca-file", "/etc/ca.pem", "--schema-registry-url", "http://sr:8081", "--read-only")
	if err != nil {
		t.Fatalf("add prod: %v", err)
	}
	cfg, data := readConfig(t, path)
	prod, err := cfg.Find("prod")
	if err != nil {
		t.Fatal(err)
	}
	want := config.Context{Name: "prod", Broker: "kafka", Brokers: []string{"k1:9093", "k2:9093"},
		SASLMechanism: "SCRAM-SHA-512", UsernameEnv: "PROD_USER", PasswordEnv: "PROD_PASS", ReadOnly: true,
		TLS: &config.TLS{Enabled: true, CAFile: "/etc/ca.pem"}, SchemaRegistry: &config.Endpoint{URL: "http://sr:8081"}}
	if prod.Get("brokers") != want.Get("brokers") || *prod.TLS != *want.TLS || *prod.SchemaRegistry != *want.SchemaRegistry ||
		prod.SASLMechanism != want.SASLMechanism || prod.PasswordEnv != want.PasswordEnv || !prod.ReadOnly {
		t.Errorf("prod = %+v", prod)
	}
	if cfg.CurrentContext != "local" {
		t.Errorf("current-context = %q; adding must not switch without --use", cfg.CurrentContext)
	}
	if !strings.HasPrefix(data, "# team contexts\n") {
		t.Errorf("comment lost:\n%s", data)
	}

	if _, err := run("ctx", "add", "r", "--broker", "rabbitmq", "--url", "amqp://h:5672/", "--use"); err != nil {
		t.Fatalf("add --use: %v", err)
	}
	if cfg, _ := readConfig(t, path); cfg.CurrentContext != "r" {
		t.Errorf("--use did not switch: %q", cfg.CurrentContext)
	}
}

func mustRead(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestCtxAddRejects(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "missing broker", args: []string{"x"}, want: `"broker" not set`},
		{name: "unknown broker", args: []string{"x", "--broker", "nats"}, want: "--broker must be one of"},
		{name: "duplicate", args: []string{"fake", "--broker", "kafka", "--brokers", "h:1"}, want: "already exists"},
		{name: "invalid result", args: []string{"x", "--broker", "kafka"}, want: "at least one entry in brokers"},
		{name: "field of another broker", args: []string{"x", "--broker", "kafka", "--brokers", "h:1", "--url", "amqp://h/"},
			want: "--url does not apply to kafka"},
		{name: "secret in env flag", args: []string{"x", "--broker", "kafka", "--brokers", "h:1", "--password-env", "s3cr3t!"},
			want: "not the secret itself"},
		{name: "password in url", args: []string{"x", "--broker", "rabbitmq", "--url", "amqp://u:s3cr3t@h:5672/"},
			want: "must not embed a password"},
		{name: "bad choice", args: []string{"x", "--broker", "kafka", "--brokers", "h:1", "--sasl-mechanism", "GSSAPI"},
			want: "must be one of"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newFakeEnv(t, false)
			before := mustRead(t, e.path)
			_, _, err := e.run(t, append([]string{"ctx", "add"}, tt.args...)...)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want containing %q", err, tt.want)
			}
			if strings.Contains(err.Error(), "s3cr3t") {
				t.Errorf("error echoes the secret: %v", err)
			}
			if after := mustRead(t, e.path); after != before {
				t.Errorf("config changed on error:\n%s", after)
			}
		})
	}
}

func TestCtxSet(t *testing.T) {
	e := newFakeEnv(t, true)
	_, _, err := e.run(t, "ctx", "set", "fake", "--read-only=false", "--username-env", "FAKE_USER", "--name", "dev")
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	cfg, _ := readConfig(t, e.path)
	dev, err := cfg.Find("dev")
	if err != nil || dev.ReadOnly || dev.UsernameEnv != "FAKE_USER" || dev.Options["instance"] == "" {
		t.Fatalf("dev = %+v, %v", dev, err)
	}
	if cfg.CurrentContext != "dev" {
		t.Errorf("current-context did not follow the rename: %q", cfg.CurrentContext)
	}

	// An empty value clears a field.
	if _, _, err := e.run(t, "ctx", "set", "dev", "--username-env", ""); err != nil {
		t.Fatal(err)
	}
	if cfg, _ := readConfig(t, e.path); cfg.Contexts[0].UsernameEnv != "" {
		t.Errorf("username_env not cleared: %+v", cfg.Contexts[0])
	}

	for _, tt := range []struct {
		args []string
		want string
	}{
		{args: []string{"dev"}, want: "nothing to change"},
		{args: []string{"nope", "--read-only"}, want: `context "nope" not found`},
		{args: []string{"dev", "--options", ""}, want: "fake needs options.instance"},
	} {
		if _, _, err := e.run(t, append([]string{"ctx", "set"}, tt.args...)...); err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("set %v: error = %v, want containing %q", tt.args, err, tt.want)
		}
	}
}

func TestCtxDelete(t *testing.T) {
	e := newFakeEnv(t, false)

	_, _, err := e.run(t, "ctx", "delete", "fake")
	if !errors.Is(err, ErrNotConfirmed) {
		t.Fatalf("delete without --yes: %v, want ErrNotConfirmed", err)
	}
	_, _, err = runWith(t, []Option{withTerminal(true)}, strings.NewReader("n\n"), "--config", e.path, "ctx", "delete", "fake")
	if !errors.Is(err, ErrNotConfirmed) {
		t.Fatalf("answering n: %v", err)
	}
	if cfg, _ := readConfig(t, e.path); len(cfg.Contexts) != 1 {
		t.Fatal("deleted without confirmation")
	}

	out, stderr, err := runWith(t, []Option{withTerminal(true)}, strings.NewReader("y\n"), "--config", e.path, "ctx", "rm", "fake")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !strings.Contains(stderr, `About to delete context "fake"`) || !strings.Contains(out, "It was the current context") {
		t.Errorf("stdout = %q, stderr = %q", out, stderr)
	}
	cfg, _ := readConfig(t, e.path)
	if len(cfg.Contexts) != 0 || cfg.CurrentContext != "" {
		t.Errorf("after delete = %+v", cfg)
	}

	if _, _, err := e.run(t, "ctx", "delete", "fake", "--yes"); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("deleting twice: %v", err)
	}
}
