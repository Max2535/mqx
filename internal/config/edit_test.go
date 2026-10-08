package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEdit(t *testing.T) {
	base := func() *Config {
		return &Config{CurrentContext: "a", Contexts: []Context{kafkaCtx("a"), kafkaCtx("b")}}
	}
	tests := []struct {
		name        string
		edit        func(c *Config) error
		wantNames   string
		wantCurrent string
		wantErr     string
	}{
		{name: "add", edit: func(c *Config) error { return c.Add(kafkaCtx("c")) }, wantNames: "a,b,c", wantCurrent: "a"},
		{name: "add duplicate", edit: func(c *Config) error { return c.Add(kafkaCtx("b")) },
			wantNames: "a,b", wantCurrent: "a", wantErr: `context "b" already exists`},
		{name: "add unnamed", edit: func(c *Config) error { return c.Add(Context{Broker: "kafka"}) },
			wantNames: "a,b", wantCurrent: "a", wantErr: "name is required"},
		{name: "replace keeps position", edit: func(c *Config) error {
			ctx := kafkaCtx("a")
			ctx.ReadOnly = true
			return c.Replace("a", ctx)
		}, wantNames: "a,b", wantCurrent: "a"},
		{name: "rename current follows", edit: func(c *Config) error { return c.Replace("a", kafkaCtx("z")) },
			wantNames: "z,b", wantCurrent: "z"},
		{name: "rename onto existing", edit: func(c *Config) error { return c.Replace("a", kafkaCtx("b")) },
			wantNames: "a,b", wantCurrent: "a", wantErr: "already exists"},
		{name: "replace unknown", edit: func(c *Config) error { return c.Replace("x", kafkaCtx("x")) },
			wantNames: "a,b", wantCurrent: "a", wantErr: `context "x" not found`},
		{name: "remove other", edit: func(c *Config) error { return c.Remove("b") }, wantNames: "a", wantCurrent: "a"},
		{name: "remove current clears it", edit: func(c *Config) error { return c.Remove("a") }, wantNames: "b", wantCurrent: ""},
		{name: "remove unknown", edit: func(c *Config) error { return c.Remove("x") },
			wantNames: "a,b", wantCurrent: "a", wantErr: `context "x" not found`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := base()
			err := tt.edit(c)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want containing %q", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			var names []string
			for _, ctx := range c.Contexts {
				names = append(names, ctx.Name)
			}
			if got := strings.Join(names, ","); got != tt.wantNames {
				t.Errorf("contexts = %s, want %s", got, tt.wantNames)
			}
			if c.CurrentContext != tt.wantCurrent {
				t.Errorf("current = %q, want %q", c.CurrentContext, tt.wantCurrent)
			}
		})
	}
}

func TestRemoveDoesNotShiftCallerSlice(t *testing.T) {
	c := &Config{Contexts: []Context{kafkaCtx("a"), kafkaCtx("b"), kafkaCtx("c")}}
	held := c.Contexts
	if err := c.Remove("a"); err != nil {
		t.Fatal(err)
	}
	if held[0].Name != "a" || held[2].Name != "c" {
		t.Errorf("caller's slice changed: %v", held)
	}
}

// commented has no blank lines: yaml.v3 does not keep them.
const commented = `# mqx contexts, maintained by hand.
current-context: local # the default
contexts:
  # Local stack from docker-compose.
  - name: local
    broker: kafka
    brokers: ["localhost:9092"] # single node
  # Production: never mutate.
  - name: prod
    broker: kafka
    brokers: ["k1:9093", "k2:9093"]
    read_only: true # keep this on
`

func TestSaveKeepsComments(t *testing.T) {
	tests := []struct {
		name string
		edit func(c *Config) error
		want string
	}{
		{name: "unchanged", edit: func(*Config) error { return nil }, want: commented},
		{
			name: "use",
			edit: func(c *Config) error { return c.Use("prod") },
			want: strings.Replace(commented, "current-context: local #", "current-context: prod #", 1),
		},
		{
			name: "edit field and add key",
			edit: func(c *Config) error {
				ctx, _ := c.Find("prod")
				ctx.Brokers = []string{"k3:9093"}
				ctx.UsernameEnv = "PROD_USER"
				return c.Replace("prod", ctx)
			},
			want: strings.Replace(commented, `    brokers: ["k1:9093", "k2:9093"]
    read_only: true # keep this on
`, `    brokers: ["k3:9093"]
    read_only: true # keep this on
    username_env: PROD_USER
`, 1),
		},
		{
			name: "rename keeps comments",
			edit: func(c *Config) error {
				ctx, _ := c.Find("prod")
				ctx.Name = "production"
				return c.Replace("prod", ctx)
			},
			want: strings.Replace(commented, "- name: prod", "- name: production", 1),
		},
		{
			name: "add",
			edit: func(c *Config) error {
				return c.Add(Context{Name: "rabbit", Broker: "rabbitmq", URL: "amqp://localhost:5672/"})
			},
			want: commented + `  - name: rabbit
    broker: rabbitmq
    url: amqp://localhost:5672/
`,
		},
		{
			name: "remove first",
			edit: func(c *Config) error { return c.Remove("local") },
			want: `# mqx contexts, maintained by hand.
current-context: "" # the default
contexts:
  # Production: never mutate.
  - name: prod
    broker: kafka
    brokers: ["k1:9093", "k2:9093"]
    read_only: true # keep this on
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeConfig(t, commented)
			c, err := Load(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := tt.edit(c); err != nil {
				t.Fatal(err)
			}
			if err := c.Save(path); err != nil {
				t.Fatalf("Save() error: %v", err)
			}
			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tt.want {
				t.Errorf("saved file:\n%s\nwant:\n%s", got, tt.want)
			}
			if _, err := os.Stat(path + ".bak"); err == nil {
				t.Error("a mergeable file must not leave a backup")
			}
			reloaded, err := Load(path)
			if err != nil {
				t.Fatalf("reload: %v", err)
			}
			if !sameConfig(reloaded, c) {
				t.Errorf("reloaded = %+v, want %+v", reloaded, c)
			}
		})
	}
}

func TestSaveWithAnchorsBacksUp(t *testing.T) {
	const anchored = `# shared seeds
contexts:
  - name: a
    broker: kafka
    brokers: &seeds ["h:9092"]
  - name: b
    broker: kafka
    brokers: *seeds
`
	path := writeConfig(t, anchored)
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Use("b"); err != nil {
		t.Fatal(err)
	}
	if err := c.Save(path); err != nil {
		t.Fatalf("Save() error: %v", err)
	}
	bak, err := os.ReadFile(path + ".bak")
	if err != nil || string(bak) != anchored {
		t.Fatalf("backup = %q, %v; want the original file", bak, err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.CurrentContext != "b" || len(got.Contexts) != 2 || got.Contexts[1].Brokers[0] != "h:9092" {
		t.Errorf("saved config = %+v", got)
	}
	if entries, _ := os.ReadDir(filepath.Dir(path)); len(entries) != 2 {
		t.Errorf("dir has %d entries, want config and its backup", len(entries))
	}
}

func TestSaveKeepsCommentOnlyFile(t *testing.T) {
	path := writeConfig(t, "# my contexts\n# one per cluster\n")
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Add(kafkaCtx("a")); err != nil {
		t.Fatal(err)
	}
	if err := c.Save(path); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	if !strings.HasPrefix(string(data), "# my contexts\n# one per cluster\n") {
		t.Errorf("comment lost:\n%s", data)
	}
	if got, err := Load(path); err != nil || len(got.Contexts) != 1 {
		t.Errorf("reload = %+v, %v", got, err)
	}
}
