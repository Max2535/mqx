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
	"os"
	"path/filepath"
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
	ReadOnly      bool     `yaml:"read_only,omitempty"` // refuse mutating commands (enforced from M3)
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
		if isLiteralSecret(err) {
			return nil, fmt.Errorf("parse config %q: %w (%s)", path, err, literalSecretHint)
		}
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}
	return &c, nil
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
func (c *Config) Save(path string) error {
	data, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("encode config: %w", err)
	}
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
