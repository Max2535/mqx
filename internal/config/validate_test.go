package config

import (
	"strings"
	"testing"
)

func kafkaCtx(name string) Context {
	return Context{Name: name, Broker: "kafka", Brokers: []string{"localhost:9092"}}
}

func rabbitCtx(name string) Context {
	return Context{Name: name, Broker: "rabbitmq", URL: "amqp://localhost:5672/"}
}

func withURL(c Context, u string) Context {
	c.URL = u
	return c
}

func withMgmtURL(c Context, u string) Context {
	c.ManagementURL = u
	return c
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name   string
		cfg    Config
		want   []string // substrings that must all appear; nil means valid
		secret string   // must never appear in the error
	}{
		{name: "valid", cfg: Config{CurrentContext: "a", Contexts: []Context{kafkaCtx("a"), rabbitCtx("b")}}},
		{name: "empty config", cfg: Config{}},
		{name: "no current context selected", cfg: Config{Contexts: []Context{kafkaCtx("a")}}},
		{name: "url with username only", cfg: Config{Contexts: []Context{withURL(rabbitCtx("a"), "amqp://guest@localhost:5672/")}}},
		{
			name: "missing name",
			cfg:  Config{Contexts: []Context{{Broker: "kafka", Brokers: []string{"h:9092"}}}},
			want: []string{"context #1: name is required"},
		},
		{
			name: "duplicate name",
			cfg:  Config{Contexts: []Context{kafkaCtx("a"), rabbitCtx("a")}},
			want: []string{`context "a": duplicate name`},
		},
		{
			name: "missing broker",
			cfg:  Config{Contexts: []Context{{Name: "a"}}},
			want: []string{`context "a": broker is required (supported: kafka, rabbitmq)`},
		},
		{
			name: "unknown broker",
			cfg:  Config{Contexts: []Context{{Name: "a", Broker: "nats"}}},
			want: []string{`context "a": unknown broker "nats" (supported: kafka, rabbitmq)`},
		},
		{
			name: "kafka without brokers",
			cfg:  Config{Contexts: []Context{{Name: "a", Broker: "kafka"}}},
			want: []string{`context "a": kafka needs at least one entry in brokers`},
		},
		{
			name: "rabbitmq without url",
			cfg:  Config{Contexts: []Context{{Name: "a", Broker: "rabbitmq"}}},
			want: []string{`context "a": rabbitmq needs url`},
		},
		{
			name:   "password in url",
			cfg:    Config{Contexts: []Context{withURL(rabbitCtx("a"), "amqp://guest:s3cret@localhost:5672/")}},
			want:   []string{`context "a": url must not embed a password; remove it and set password_env`},
			secret: "s3cret",
		},
		{
			name:   "password in management_url",
			cfg:    Config{Contexts: []Context{withMgmtURL(rabbitCtx("a"), "http://admin:s3cret@localhost:15672")}},
			want:   []string{`context "a": management_url must not embed a password`},
			secret: "s3cret",
		},
		{
			name:   "host-less url hiding a password",
			cfg:    Config{Contexts: []Context{withURL(rabbitCtx("a"), "guest:s3cret@localhost:5672")}},
			want:   []string{`context "a": url must be an absolute URL with a host`},
			secret: "s3cret",
		},
		{
			name:   "host-less management_url hiding a password",
			cfg:    Config{Contexts: []Context{withMgmtURL(rabbitCtx("a"), "guest:s3cret@localhost:15672")}},
			want:   []string{`context "a": management_url must be an absolute URL with a host`},
			secret: "s3cret",
		},
		{
			name:   "unparseable url does not echo it",
			cfg:    Config{Contexts: []Context{withURL(rabbitCtx("a"), "amqp://u:s3cret%zz@localhost/")}},
			want:   []string{`context "a": url is not a valid URL`},
			secret: "s3cret",
		},
		{
			name: "unknown current context",
			cfg:  Config{CurrentContext: "gone", Contexts: []Context{kafkaCtx("a")}},
			want: []string{`current-context "gone" does not exist`},
		},
		{
			name: "reports every problem at once",
			cfg: Config{CurrentContext: "gone", Contexts: []Context{
				{Name: "a", Broker: "nats"},
				kafkaCtx("a"),
			}},
			want: []string{`unknown broker "nats"`, `context "a": duplicate name`, `current-context "gone" does not exist`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.want == nil {
				if err != nil {
					t.Fatalf("Validate() unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want errors %q", tt.want)
			}
			for _, w := range tt.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("Validate() error = %q, want it to contain %q", err, w)
				}
			}
			if tt.secret != "" && strings.Contains(err.Error(), tt.secret) {
				t.Errorf("Validate() error leaks secret: %q", err)
			}
		})
	}
}
