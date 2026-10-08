package config

import (
	"errors"
	"fmt"
	"os"
)

// Credentials are read from the environment variables a Context references.
// String and GoString redact them so they never leak through %v or logs.
type Credentials struct {
	Username string
	Password string
}

// String implements fmt.Stringer without revealing the values.
func (Credentials) String() string { return "Credentials{redacted}" }

// GoString implements fmt.GoStringer without revealing the values.
func (c Credentials) GoString() string { return c.String() }

// Resolve reads the referenced env vars. Unreferenced fields stay empty.
// A referenced but unset variable is an error; all such errors are joined.
func (c Context) Resolve() (Credentials, error) {
	return resolve(fmt.Sprintf("context %q", c.Name), c.UsernameEnv, c.PasswordEnv)
}

// Resolve reads the endpoint's credentials; label names it in errors, e.g. `context "a" schema_registry`.
func (e Endpoint) Resolve(label string) (Credentials, error) {
	return resolve(label, e.UsernameEnv, e.PasswordEnv)
}

func resolve(label, userEnv, passEnv string) (Credentials, error) {
	var creds Credentials
	var errs []error
	for _, f := range []struct {
		key, env string
		dst      *string
	}{
		{key: "username_env", env: userEnv, dst: &creds.Username},
		{key: "password_env", env: passEnv, dst: &creds.Password},
	} {
		if f.env == "" {
			continue
		}
		v, ok := os.LookupEnv(f.env)
		if !ok {
			errs = append(errs, fmt.Errorf("%s: env var %s (%s) is not set", label, f.env, f.key))
			continue
		}
		*f.dst = v
	}
	if err := errors.Join(errs...); err != nil {
		return Credentials{}, err
	}
	return creds, nil
}

// Find returns the context called name.
func (c *Config) Find(name string) (Context, error) {
	for _, ctx := range c.Contexts {
		if ctx.Name == name {
			return ctx, nil
		}
	}
	return Context{}, c.notFound(name)
}
