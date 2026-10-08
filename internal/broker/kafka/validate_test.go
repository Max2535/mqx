package kafka

import (
	"strings"
	"testing"

	"github.com/Max2535/mqx/internal/config"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name   string
		ctx    config.Context
		want   []string
		secret string
	}{
		{name: "valid", ctx: config.Context{Brokers: []string{"localhost:9092"}}},
		{name: "scram", ctx: config.Context{Brokers: []string{"h:9092"}, SASLMechanism: "scram-sha-512"}},
		{name: "no brokers", ctx: config.Context{}, want: []string{"kafka needs at least one entry in brokers"}},
		{
			name:   "broker with credentials",
			ctx:    config.Context{Brokers: []string{"localhost:9092", "user:s3cret@host:9092"}},
			want:   []string{"brokers[1] must be host:port; credentials go in username_env / password_env"},
			secret: "s3cret",
		},
		{name: "broker without port", ctx: config.Context{Brokers: []string{"localhost"}}, want: []string{"brokers[0] must be host:port"}},
		{name: "bad sasl", ctx: config.Context{Brokers: []string{"h:9092"}, SASLMechanism: "GSSAPI"}, want: []string{`sasl_mechanism "GSSAPI" is not supported`}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validate(tt.ctx)
			if len(errs) != len(tt.want) {
				t.Fatalf("validate() = %v, want %d errors", errs, len(tt.want))
			}
			for i, w := range tt.want {
				if !strings.Contains(errs[i].Error(), w) {
					t.Errorf("error %d = %q, want containing %q", i, errs[i], w)
				}
				if tt.secret != "" && strings.Contains(errs[i].Error(), tt.secret) {
					t.Errorf("error leaks secret: %q", errs[i])
				}
			}
		})
	}
}
