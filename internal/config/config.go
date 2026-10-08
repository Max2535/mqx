// Package config loads, validates and saves mqx's named broker contexts.
//
// Credentials are never stored in the file: a context names the environment
// variables that hold them (username_env, password_env) and Resolve reads them.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// EnvConfig overrides the default config file path.
const EnvConfig = "MQX_CONFIG"

const literalSecretHint = "credentials must not be stored in the config; " +
	"reference environment variables with username_env / password_env"

// Config is the on-disk config file.
type Config struct {
	CurrentContext string    `yaml:"current-context"`
	Contexts       []Context `yaml:"contexts"`

	doc *yaml.Node // the tree Load read, so Save can keep its comments
}

// Context is one named broker connection.
type Context struct {
	Name          string   `yaml:"name"`
	Broker        string   `yaml:"broker"`
	Brokers       []string `yaml:"brokers,omitempty"`        // kafka seed brokers
	URL           string   `yaml:"url,omitempty"`            // rabbitmq AMQP URL, no password
	ManagementURL string   `yaml:"management_url,omitempty"` // rabbitmq management API
	UsernameEnv   string   `yaml:"username_env,omitempty"`
	PasswordEnv   string   `yaml:"password_env,omitempty"`
	ReadOnly      bool     `yaml:"read_only,omitempty"` // refuse mutating commands

	SASLMechanism string `yaml:"sasl_mechanism,omitempty"` // kafka: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
	TLS           *TLS   `yaml:"tls,omitempty"`

	// Kafka ecosystem services, each with its own credentials.
	SchemaRegistry *Endpoint `yaml:"schema_registry,omitempty"`
	Connect        *Endpoint `yaml:"connect,omitempty"`
	KSQLDB         *Endpoint `yaml:"ksqldb,omitempty"`

	// Options holds broker-specific settings, so a new broker needs no new fields here.
	Options map[string]string `yaml:"options,omitempty"`
}

// TLS configures transport security. Files are paths, never inline PEM.
type TLS struct {
	Enabled            bool   `yaml:"enabled"`
	CAFile             string `yaml:"ca_file,omitempty"`
	CertFile           string `yaml:"cert_file,omitempty"`
	KeyFile            string `yaml:"key_file,omitempty"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify,omitempty"`
}

// Endpoint is an HTTP service next to the broker (Schema Registry, Connect, ksqlDB).
type Endpoint struct {
	URL         string `yaml:"url"`
	UsernameEnv string `yaml:"username_env,omitempty"`
	PasswordEnv string `yaml:"password_env,omitempty"`
}

// DefaultPath returns $MQX_CONFIG, or ~/.config/mqx/config.yaml on every OS.
func DefaultPath() (string, error) {
	if p := os.Getenv(EnvConfig); p != "" {
		return p, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("locate home directory (set %s or pass --config): %w", EnvConfig, err)
	}
	return filepath.Join(home, ".config", "mqx", "config.yaml"), nil
}

// Load reads the config at path. An empty file is an empty config.
// A missing file returns an error matching errors.Is(err, fs.ErrNotExist).
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("load config %q: %w", path, err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true) // unknown keys, including literal password/username, are errors
	var c Config
	if err := dec.Decode(&c); err != nil && !errors.Is(err, io.EOF) {
		var typeErr *yaml.TypeError
		if errors.As(err, &typeErr) {
			err = &yaml.TypeError{Errors: redactQuoted(typeErr.Errors)}
		}
		if isLiteralSecret(err) {
			return nil, fmt.Errorf("parse config %q: %w (%s)", path, err, literalSecretHint)
		}
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err == nil && len(doc.Content) == 1 && doc.Content[0].Kind == yaml.MappingNode {
		c.doc = &doc
	}
	return &c, nil
}

var quotedValue = regexp.MustCompile("`[^`]*`")

// redactQuoted hides the backtick-quoted offending values yaml.v3 puts in type errors.
func redactQuoted(msgs []string) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = quotedValue.ReplaceAllString(m, "<value>")
	}
	return out
}

// isLiteralSecret reports whether a decode error comes from a literal credential key.
// yaml.v3 reports unknown keys as "field <key> not found in type ...", without the value.
func isLiteralSecret(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "field password not found") ||
		strings.Contains(msg, "field username not found")
}

// Save writes c to path atomically (temp file + rename) with mode 0600,
// creating the parent directory with mode 0700 if needed.
//
// A Config from Load is written back into the file's own YAML tree, which keeps
// comments, key order and flow style (blank lines are not kept). A file using
// anchors or aliases cannot be updated that way; it is rewritten from scratch
// after copying it to path + ".bak".
func (c *Config) Save(path string) error {
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		path = resolved // write through a symlinked config instead of replacing the link
	}
	data, keptComments, err := c.encode()
	if err != nil {
		return fmt.Errorf("encode config: %w", err)
	}
	if !keptComments {
		if err := backup(path); err != nil {
			return err
		}
	}
	return writeAtomic(path, data)
}

// encode renders c, merged into its loaded tree when possible. keptComments is
// false only when a loaded tree had to be discarded.
func (c *Config) encode() (data []byte, keptComments bool, err error) {
	var fresh yaml.Node
	if err := fresh.Encode(c); err != nil {
		return nil, false, err
	}
	doc := &yaml.Node{Kind: yaml.DocumentNode, Content: []*yaml.Node{&fresh}}
	keptComments = c.doc == nil
	if c.doc != nil && mergeable(c.doc) {
		mergeNode(c.doc.Content[0], &fresh)
		doc, keptComments = c.doc, true
	}
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(doc); err != nil {
		return nil, false, err
	}
	if err := enc.Close(); err != nil {
		return nil, false, err
	}
	return buf.Bytes(), keptComments, nil
}

// backup copies an existing file at path to path + ".bak" with mode 0600.
func backup(path string) error {
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("back up config %q: %w", path, err)
	}
	if err := writeAtomic(path+".bak", data); err != nil {
		return fmt.Errorf("back up config %q: %w", path, err)
	}
	return nil
}

func writeAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create config dir %q: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".config-*.yaml") // CreateTemp uses mode 0600
	if err != nil {
		return fmt.Errorf("save config %q: %w", path, err)
	}
	defer os.Remove(tmp.Name()) // no-op once renamed
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("save config %q: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("save config %q: %w", path, err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("save config %q: %w", path, err)
	}
	return nil
}

// Use makes name the current context. It does not save; call Save afterwards.
func (c *Config) Use(name string) error {
	if _, err := c.Find(name); err != nil {
		return err
	}
	c.CurrentContext = name
	return nil
}

func (c *Config) notFound(name string) error {
	names := make([]string, 0, len(c.Contexts))
	for _, ctx := range c.Contexts {
		names = append(names, ctx.Name)
	}
	if len(names) == 0 {
		return fmt.Errorf("context %q not found; no contexts defined in config", name)
	}
	return fmt.Errorf("context %q not found; available: %s", name, strings.Join(names, ", "))
}
