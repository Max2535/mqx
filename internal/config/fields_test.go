package config

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestContextSetGet(t *testing.T) {
	tests := []struct {
		key, value string
		want       Context
		get        string // expected Get after Set; defaults to value
		wantErr    string
	}{
		{key: "brokers", value: " a:1 , b:2,", want: Context{Brokers: []string{"a:1", "b:2"}}, get: "a:1,b:2"},
		{key: "brokers", value: "", want: Context{}},
		{key: "url", value: "amqp://h:5672/", want: Context{URL: "amqp://h:5672/"}},
		{key: "username_env", value: "APP_USER", want: Context{UsernameEnv: "APP_USER"}},
		{key: "password_env", value: "hunter2!", wantErr: "not the secret itself"},
		{key: "sasl_mechanism", value: "SCRAM-SHA-512", want: Context{SASLMechanism: "SCRAM-SHA-512"}},
		{key: "sasl_mechanism", value: "GSSAPI", wantErr: "must be one of PLAIN"},
		{key: "read_only", value: "true", want: Context{ReadOnly: true}},
		{key: "read_only", value: "yes", wantErr: "true or false"},
		{key: "tls.ca_file", value: "/ca.pem", want: Context{TLS: &TLS{CAFile: "/ca.pem"}}},
		{key: "tls.enabled", value: "false", want: Context{}},
		{key: "schema_registry.url", value: "http://sr:8081", want: Context{SchemaRegistry: &Endpoint{URL: "http://sr:8081"}}},
		{key: "connect.password_env", value: "CONNECT_PW", want: Context{Connect: &Endpoint{PasswordEnv: "CONNECT_PW"}}},
		{key: "options", value: "b=2, a=1", want: Context{Options: map[string]string{"a": "1", "b": "2"}}, get: "a=1,b=2"},
		{key: "options", value: "novalue", wantErr: "not key=value"},
		{key: "colour", value: "red", wantErr: "unknown context field"},
	}
	for _, tt := range tests {
		t.Run(tt.key+"="+tt.value, func(t *testing.T) {
			var c Context
			err := c.Set(tt.key, tt.value)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Set() error = %v, want containing %q", err, tt.wantErr)
				}
				if strings.Contains(err.Error(), "hunter2") {
					t.Errorf("error echoes the value: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Set() error: %v", err)
			}
			if !reflect.DeepEqual(c, tt.want) {
				t.Errorf("context = %+v, want %+v", c, tt.want)
			}
			want := tt.value
			if tt.get != "" {
				want = tt.get
			}
			if got := c.Get(tt.key); got != want {
				t.Errorf("Get() = %q, want %q", got, want)
			}
		})
	}
}

func TestSetClearsEmptyBlocks(t *testing.T) {
	c := Context{TLS: &TLS{CAFile: "/ca"}, KSQLDB: &Endpoint{URL: "http://k"}}
	for _, kv := range [][2]string{{"tls.ca_file", ""}, {"ksqldb.url", ""}} {
		if err := c.Set(kv[0], kv[1]); err != nil {
			t.Fatal(err)
		}
	}
	if c.TLS != nil || c.KSQLDB != nil {
		t.Errorf("empty blocks kept: %+v", c)
	}
}

func TestCloneIsDeep(t *testing.T) {
	orig := Context{Brokers: []string{"a"}, TLS: &TLS{CAFile: "x"}, Connect: &Endpoint{URL: "u"},
		Options: map[string]string{"k": "v"}}
	c := orig.Clone()
	c.Brokers[0] = "b"
	c.Options["k"] = "w"
	_ = c.Set("tls.ca_file", "y")
	_ = c.Set("connect.url", "z")
	c.TLS.CertFile = "mutated"
	c.Connect.UsernameEnv = "MUTATED"
	if orig.Brokers[0] != "a" || orig.Options["k"] != "v" || orig.TLS.CAFile != "x" || orig.TLS.CertFile != "" ||
		orig.Connect.URL != "u" || orig.Connect.UsernameEnv != "" {
		t.Errorf("clone shares state with the original: %+v", orig)
	}
}

func TestFieldsFor(t *testing.T) {
	var keys []string
	for _, f := range FieldsFor([]string{"url", "tls.enabled"}) {
		keys = append(keys, f.Key)
	}
	if got := strings.Join(keys, ","); got != "url,username_env,password_env,tls.enabled,read_only" {
		t.Errorf("FieldsFor = %s", got)
	}
	if len(FieldsFor(nil)) != len(Fields()) {
		t.Error("nil list must return every field")
	}
}

func TestUpdate(t *testing.T) {
	const orig = "current-context: a # keep\ncontexts:\n  - name: a\n    broker: kafka\n    brokers: [\"h:9092\"]\n"
	t.Run("valid edit is saved", func(t *testing.T) {
		path := writeConfig(t, orig)
		c, err := Load(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Update(path, fakeBrokers{}, func(c *Config) error { return c.Add(kafkaCtx("b")) }); err != nil {
			t.Fatalf("Update() error: %v", err)
		}
		got, err := Load(path)
		if err != nil || len(got.Contexts) != 2 {
			t.Fatalf("saved = %+v, %v", got, err)
		}
	})
	for _, tt := range []struct {
		name string
		edit func(c *Config) error
		want string
	}{
		{name: "invalid result", edit: func(c *Config) error {
			return c.Replace("a", Context{Name: "a", Broker: "kafka"})
		}, want: "at least one entry"},
		{name: "edit error", edit: func(c *Config) error { return c.Remove("zzz") }, want: "not found"},
		{name: "password in url", edit: func(c *Config) error {
			return c.Add(Context{Name: "r", Broker: "rabbitmq", URL: "amqp://u:s3cret@h:5672/"})
		}, want: "must not embed a password"},
	} {
		t.Run(tt.name+" changes nothing", func(t *testing.T) {
			path := writeConfig(t, orig)
			c, err := Load(path)
			if err != nil {
				t.Fatal(err)
			}
			err = c.Update(path, fakeBrokers{}, tt.edit)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Update() error = %v, want containing %q", err, tt.want)
			}
			if len(c.Contexts) != 1 || len(c.Contexts[0].Brokers) != 1 || c.CurrentContext != "a" {
				t.Errorf("config not rolled back: %+v", c)
			}
			if data, _ := os.ReadFile(path); string(data) != orig {
				t.Errorf("file changed:\n%s", data)
			}
		})
	}
}
