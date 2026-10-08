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
	var creds Credentials
	var errs []error
	for _, f := range []struct {
		key, env string
		dst      *string
	}{
		{key: "username_env", env: c.UsernameEnv, dst: &creds.Username},
		{key: "password_env", env: c.PasswordEnv, dst: &creds.Password},
	} {
		if f.env == "" {
			continue
		}
		v, ok := os.LookupEnv(f.env)
		if !ok {
			errs = append(errs, fmt.Errorf("context %q: env var %s (%s) is not set", c.Name, f.env, f.key))
			continue
		}
		*f.dst = v
	}
	if err := errors.Join(errs...); err != nil {
		return Credentials{}, err
	}
	return creds, nil
}
