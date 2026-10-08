package config

import (
	"errors"
	"fmt"
	"net/url"
)

// supportedBrokers lists broker types for validation and error messages.
// ponytail: hard-coded until M1's broker registry becomes the source of truth.
const supportedBrokers = "kafka, rabbitmq"

var (
	errInvalidURL     = errors.New("is not a valid URL; check for unescaped characters")
	errNoHost         = errors.New("must be an absolute URL with a host, e.g. amqp://host:5672/")
	errEmbeddedSecret = errors.New("must not embed a password; remove it and set password_env")
)

// Validate reports every problem in c at once, joined with errors.Join, or nil.
// It never reads environment variables, so it works without credentials set.
func (c *Config) Validate() error {
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
		errs = append(errs, ctx.validate(label)...)
	}
	if c.CurrentContext != "" && !seen[c.CurrentContext] {
		errs = append(errs, fmt.Errorf("current-context %q does not exist; run `mqx ctx use <name>` with a name from `mqx ctx list`",
			c.CurrentContext))
	}
	return errors.Join(errs...)
}

func (c Context) validate(label string) []error {
	var errs []error
	switch c.Broker {
	case "":
		errs = append(errs, fmt.Errorf("%s: broker is required (supported: %s)", label, supportedBrokers))
	case "kafka":
		if len(c.Brokers) == 0 {
			errs = append(errs, fmt.Errorf("%s: kafka needs at least one entry in brokers", label))
		}
	case "rabbitmq":
		if c.URL == "" {
			errs = append(errs, fmt.Errorf("%s: rabbitmq needs url", label))
		}
	default:
		errs = append(errs, fmt.Errorf("%s: unknown broker %q (supported: %s)", label, c.Broker, supportedBrokers))
	}
	for _, f := range []struct{ key, value string }{{"url", c.URL}, {"management_url", c.ManagementURL}} {
		if err := checkNoPassword(f.value); err != nil {
			errs = append(errs, fmt.Errorf("%s: %s %w", label, f.key, err))
		}
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
