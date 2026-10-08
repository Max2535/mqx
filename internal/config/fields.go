package config

import (
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// FieldKind says how an editor shows a field and how its text is parsed.
type FieldKind int

// Field kinds.
const (
	FieldString FieldKind = iota // free text
	FieldList                    // comma-separated values
	FieldBool                    // "true" or "false"
	FieldEnv                     // the name of an environment variable, never its value
	FieldMap                     // comma-separated key=value pairs
)

// Field describes one editable context setting. Key is a dotted path into the
// YAML, e.g. "tls.ca_file"; editors (the TUI form, `mqx ctx add` flags) are built from it.
type Field struct {
	Key     string
	Help    string
	Kind    FieldKind
	Choices []string // allowed values for a string field; "" means unset
}

// CommonFields apply to every broker; adapters list the rest in their driver.
var CommonFields = []string{"username_env", "password_env", "read_only"}

var fields = []Field{
	{Key: "brokers", Help: "seed brokers, comma-separated host:port", Kind: FieldList},
	{Key: "url", Help: "broker URL without a password, e.g. amqp://host:5672/vhost"},
	{Key: "management_url", Help: "management API URL, e.g. http://host:15672"},
	{Key: "username_env", Help: "env var that holds the username", Kind: FieldEnv},
	{Key: "password_env", Help: "env var that holds the password", Kind: FieldEnv},
	{Key: "sasl_mechanism", Help: "SASL mechanism", Choices: []string{"", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}},
	{Key: "tls.enabled", Help: "connect with TLS", Kind: FieldBool},
	{Key: "tls.ca_file", Help: "CA bundle path"},
	{Key: "tls.cert_file", Help: "client certificate path (with tls.key_file)"},
	{Key: "tls.key_file", Help: "client key path (with tls.cert_file)"},
	{Key: "tls.insecure_skip_verify", Help: "skip server certificate verification", Kind: FieldBool},
	{Key: "schema_registry.url", Help: "Schema Registry URL"},
	{Key: "schema_registry.username_env", Help: "env var with the Schema Registry username", Kind: FieldEnv},
	{Key: "schema_registry.password_env", Help: "env var with the Schema Registry password", Kind: FieldEnv},
	{Key: "connect.url", Help: "Kafka Connect URL"},
	{Key: "connect.username_env", Help: "env var with the Kafka Connect username", Kind: FieldEnv},
	{Key: "connect.password_env", Help: "env var with the Kafka Connect password", Kind: FieldEnv},
	{Key: "ksqldb.url", Help: "ksqlDB URL"},
	{Key: "ksqldb.username_env", Help: "env var with the ksqlDB username", Kind: FieldEnv},
	{Key: "ksqldb.password_env", Help: "env var with the ksqlDB password", Kind: FieldEnv},
	{Key: "options", Help: "broker-specific settings, key=value,key=value", Kind: FieldMap},
	{Key: "read_only", Help: "refuse every mutating command", Kind: FieldBool},
}

// Fields returns every editable field in display order.
func Fields() []Field { return slices.Clone(fields) }

// LookupField returns the field with the given key.
func LookupField(key string) (Field, bool) {
	i := slices.IndexFunc(fields, func(f Field) bool { return f.Key == key })
	if i < 0 {
		return Field{}, false
	}
	return fields[i], true
}

// FieldsFor returns the common fields plus the listed broker-specific ones, in
// display order. A nil list means the broker did not say, so every field is returned.
func FieldsFor(brokerFields []string) []Field {
	if brokerFields == nil {
		return Fields()
	}
	var out []Field
	for _, f := range fields {
		if slices.Contains(CommonFields, f.Key) || slices.Contains(brokerFields, f.Key) {
			out = append(out, f)
		}
	}
	return out
}

var envName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Get returns the field's value as editor text.
func (c Context) Get(key string) string {
	switch key {
	case "brokers":
		return strings.Join(c.Brokers, ",")
	case "url":
		return c.URL
	case "management_url":
		return c.ManagementURL
	case "username_env":
		return c.UsernameEnv
	case "password_env":
		return c.PasswordEnv
	case "sasl_mechanism":
		return c.SASLMechanism
	case "read_only":
		return strconv.FormatBool(c.ReadOnly)
	case "options":
		keys := slices.Sorted(maps.Keys(c.Options))
		pairs := make([]string, len(keys))
		for i, k := range keys {
			pairs[i] = k + "=" + c.Options[k]
		}
		return strings.Join(pairs, ",")
	}
	if sub, ok := strings.CutPrefix(key, "tls."); ok {
		t := TLS{}
		if c.TLS != nil {
			t = *c.TLS
		}
		switch sub {
		case "enabled":
			return strconv.FormatBool(t.Enabled)
		case "ca_file":
			return t.CAFile
		case "cert_file":
			return t.CertFile
		case "key_file":
			return t.KeyFile
		case "insecure_skip_verify":
			return strconv.FormatBool(t.InsecureSkipVerify)
		}
	}
	if ep, sub, ok := c.endpoint(key); ok {
		if *ep == nil {
			return ""
		}
		switch sub {
		case "url":
			return (*ep).URL
		case "username_env":
			return (*ep).UsernameEnv
		case "password_env":
			return (*ep).PasswordEnv
		}
	}
	return ""
}

// Set parses value into the field. An empty value clears it; a TLS block or
// service endpoint left with nothing set is removed. Env fields take a
// variable name only, so a pasted secret is refused instead of saved.
func (c *Context) Set(key, value string) error {
	f, ok := LookupField(key)
	if !ok {
		return fmt.Errorf("unknown context field %q", key)
	}
	value = strings.TrimSpace(value)
	if err := checkValue(f, value); err != nil {
		return fmt.Errorf("%s: %w", key, err)
	}
	b := value == "true"
	switch key {
	case "brokers":
		c.Brokers = splitList(value)
	case "url":
		c.URL = value
	case "management_url":
		c.ManagementURL = value
	case "username_env":
		c.UsernameEnv = value
	case "password_env":
		c.PasswordEnv = value
	case "sasl_mechanism":
		c.SASLMechanism = value
	case "read_only":
		c.ReadOnly = b
	case "options":
		m, err := parseMap(value)
		if err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
		c.Options = m
	default:
		if sub, ok := strings.CutPrefix(key, "tls."); ok {
			c.setTLS(sub, value, b)
			return nil
		}
		ep, sub, _ := c.endpoint(key)
		setEndpoint(ep, sub, value)
	}
	return nil
}

func checkValue(f Field, value string) error {
	if value == "" {
		return nil
	}
	switch {
	case f.Kind == FieldBool && value != "true" && value != "false":
		return errors.New("must be true or false")
	case f.Kind == FieldEnv && !envName.MatchString(value):
		return errors.New("must be the name of an environment variable (letters, digits, _), not the secret itself")
	case len(f.Choices) > 0 && !slices.Contains(f.Choices, value):
		return fmt.Errorf("must be one of %s", strings.Join(f.Choices[1:], ", "))
	}
	return nil
}

func (c *Context) setTLS(sub, value string, b bool) {
	t := TLS{}
	if c.TLS != nil {
		t = *c.TLS
	}
	switch sub {
	case "enabled":
		t.Enabled = b
	case "ca_file":
		t.CAFile = value
	case "cert_file":
		t.CertFile = value
	case "key_file":
		t.KeyFile = value
	case "insecure_skip_verify":
		t.InsecureSkipVerify = b
	}
	c.TLS = &t
	if t == (TLS{}) {
		c.TLS = nil
	}
}

// endpoint maps "schema_registry.url" to the Schema Registry endpoint and "url".
func (c *Context) endpoint(key string) (**Endpoint, string, bool) {
	name, sub, ok := strings.Cut(key, ".")
	if !ok {
		return nil, "", false
	}
	switch name {
	case "schema_registry":
		return &c.SchemaRegistry, sub, true
	case "connect":
		return &c.Connect, sub, true
	case "ksqldb":
		return &c.KSQLDB, sub, true
	}
	return nil, "", false
}

func setEndpoint(ep **Endpoint, sub, value string) {
	e := Endpoint{}
	if *ep != nil {
		e = **ep
	}
	switch sub {
	case "url":
		e.URL = value
	case "username_env":
		e.UsernameEnv = value
	case "password_env":
		e.PasswordEnv = value
	}
	*ep = &e
	if e == (Endpoint{}) {
		*ep = nil
	}
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func parseMap(s string) (map[string]string, error) {
	pairs := splitList(s)
	if len(pairs) == 0 {
		return nil, nil
	}
	m := make(map[string]string, len(pairs))
	for _, p := range pairs {
		k, v, ok := strings.Cut(p, "=")
		if k = strings.TrimSpace(k); !ok || k == "" {
			return nil, fmt.Errorf("%q is not key=value", p)
		}
		m[k] = strings.TrimSpace(v)
	}
	return m, nil
}

// Clone returns a deep copy, so editing it never changes c.
func (c Context) Clone() Context {
	out := c
	out.Brokers = slices.Clone(c.Brokers)
	out.Options = maps.Clone(c.Options)
	if c.TLS != nil {
		t := *c.TLS
		out.TLS = &t
	}
	for _, p := range []struct{ dst, src **Endpoint }{
		{&out.SchemaRegistry, &c.SchemaRegistry}, {&out.Connect, &c.Connect}, {&out.KSQLDB, &c.KSQLDB},
	} {
		if *p.src != nil {
			e := **p.src
			*p.dst = &e
		}
	}
	return out
}
