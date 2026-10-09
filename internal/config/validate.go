package config

import (
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
)

var (
	errInvalidURL     = errors.New("is not a valid URL; check for unescaped characters")
	errNoHost         = errors.New("must be an absolute URL with a host, e.g. amqp://host:5672/")
	errEmbeddedSecret = errors.New("must not embed a password; remove it and set password_env")
)

// BrokerValidator checks broker-specific context fields. The broker registry
// implements it, so this package needs no knowledge of individual brokers.
type BrokerValidator interface {
	// Supported lists the known broker types, for error messages.
	Supported() []string
	// ValidateContext returns known=false for an unregistered broker type.
	ValidateContext(c Context) (errs []error, known bool)
}

// Validate reports every problem in c at once, joined with errors.Join, or nil.
// It never reads environment variables, so it works without credentials set.
func (c *Config) Validate(brokers BrokerValidator) error {
	var errs []error
	seen := make(map[string]bool, len(c.Contexts))
	for i, ctx := range c.Contexts {
		label := fmt.Sprintf("context %q", ctx.Name)
		switch {
		case ctx.Name == "":
			label = fmt.Sprintf("context #%d", i+1)
			errs = append(errs, fmt.Errorf("%s: name is required", label))
		case seen[ctx.Name]:
			errs = append(errs, fmt.Errorf("%s: duplicate name; context names must be unique", label))
		}
		seen[ctx.Name] = true
		errs = append(errs, ctx.validate(label, brokers)...)
	}
	if e := c.Assistant.Effort; e != "" && !slices.Contains(AssistantEfforts, e) {
		errs = append(errs, fmt.Errorf("assistant: effort %q is not one of %s", e, strings.Join(AssistantEfforts, ", ")))
	}
	if c.CurrentContext != "" && !seen[c.CurrentContext] {
		errs = append(errs, fmt.Errorf("current-context %q does not exist; run `mqx ctx use <name>` with a name from `mqx ctx list`",
			c.CurrentContext))
	}
	return errors.Join(errs...)
}

func (c Context) validate(label string, brokers BrokerValidator) []error {
	var errs []error
	supported := strings.Join(brokers.Supported(), ", ")
	if c.Broker == "" {
		errs = append(errs, fmt.Errorf("%s: broker is required (supported: %s)", label, supported))
	} else {
		specific, known := brokers.ValidateContext(c)
		if !known {
			errs = append(errs, fmt.Errorf("%s: unknown broker %q (supported: %s)", label, c.Broker, supported))
		}
		for _, err := range specific {
			errs = append(errs, fmt.Errorf("%s: %w", label, err))
		}
	}
	urls := []struct{ key, value string }{{"url", c.URL}, {"management_url", c.ManagementURL}}
	for _, ep := range []struct {
		key string
		e   *Endpoint
	}{{"schema_registry", c.SchemaRegistry}, {"connect", c.Connect}, {"ksqldb", c.KSQLDB}} {
		if ep.e == nil {
			continue
		}
		if ep.e.URL == "" {
			errs = append(errs, fmt.Errorf("%s: %s.url is required", label, ep.key))
			continue
		}
		urls = append(urls, struct{ key, value string }{ep.key + ".url", ep.e.URL})
	}
	for _, f := range urls {
		if err := checkNoPassword(f.value); err != nil {
			errs = append(errs, fmt.Errorf("%s: %s %w", label, f.key, err))
		}
	}
	if c.TLS != nil && (c.TLS.CertFile == "") != (c.TLS.KeyFile == "") {
		errs = append(errs, fmt.Errorf("%s: tls.cert_file and tls.key_file must be set together", label))
	}
	return errs
}

// checkNoPassword rejects URLs carrying a userinfo password. url.Parse's error is
// deliberately not wrapped: it quotes the raw URL, which may contain the secret.
func checkNoPassword(raw string) error {
	if raw == "" {
		return nil
	}
	u, err := url.Parse(raw)
	if err != nil {
		return errInvalidURL
	}
	// Without "//" the userinfo is not parsed (u.User is nil), so a password could slip past.
	if u.Host == "" {
		return errNoHost
	}
	if u.User != nil {
		if _, ok := u.User.Password(); ok {
			return errEmbeddedSecret
		}
	}
	return nil
}
