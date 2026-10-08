package config

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestResolve(t *testing.T) {
	t.Setenv("MQX_TEST_USER", "alice")
	t.Setenv("MQX_TEST_PASS", "s3cret")
	t.Setenv("MQX_TEST_EMPTY", "")
	t.Setenv("MQX_TEST_MISSING", "") // registers restore, then unset for real
	os.Unsetenv("MQX_TEST_MISSING")

	tests := []struct {
		name    string
		ctx     Context
		want    Credentials
		wantErr []string
	}{
		{name: "no credentials referenced", ctx: kafkaCtx("a")},
		{
			name: "both set",
			ctx:  Context{Name: "a", UsernameEnv: "MQX_TEST_USER", PasswordEnv: "MQX_TEST_PASS"},
			want: Credentials{Username: "alice", Password: "s3cret"},
		},
		{
			name: "set but empty is allowed",
			ctx:  Context{Name: "a", UsernameEnv: "MQX_TEST_USER", PasswordEnv: "MQX_TEST_EMPTY"},
			want: Credentials{Username: "alice"},
		},
		{
			name:    "unset password env",
			ctx:     Context{Name: "a", UsernameEnv: "MQX_TEST_USER", PasswordEnv: "MQX_TEST_MISSING"},
			wantErr: []string{`context "a": env var MQX_TEST_MISSING (password_env) is not set`},
		},
		{
			name: "both unset reports both",
			ctx:  Context{Name: "a", UsernameEnv: "MQX_TEST_MISSING", PasswordEnv: "MQX_TEST_MISSING"},
			wantErr: []string{
				"env var MQX_TEST_MISSING (username_env) is not set",
				"env var MQX_TEST_MISSING (password_env) is not set",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.ctx.Resolve()
			if tt.wantErr != nil {
				if err == nil {
					t.Fatalf("Resolve() = %v, want error", got)
				}
				for _, w := range tt.wantErr {
					if !strings.Contains(err.Error(), w) {
						t.Errorf("Resolve() error = %q, want it to contain %q", err, w)
					}
				}
				if got != (Credentials{}) {
					t.Errorf("Resolve() returned partial credentials on error")
				}
				return
			}
			if err != nil {
				t.Fatalf("Resolve() unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("Resolve() = {%q, %q}, want {%q, %q}", got.Username, got.Password, tt.want.Username, tt.want.Password)
			}
		})
	}
}

func TestCredentialsRedacted(t *testing.T) {
	c := Credentials{Username: "alice", Password: "s3cret"}
	for _, format := range []string{"%v", "%+v", "%#v", "%s"} {
		for _, arg := range []any{c, &c} {
			out := fmt.Sprintf(format, arg)
			if strings.Contains(out, "alice") || strings.Contains(out, "s3cret") {
				t.Errorf("fmt %s of %T leaks credentials: %q", format, arg, out)
			}
		}
	}
}
