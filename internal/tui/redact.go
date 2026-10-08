package tui

import (
	"net/url"
	"regexp"
	"strings"

	"github.com/Max2535/mqx/internal/config"
)

var uriPassword = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://[^:/@\s"]*):[^@/\s"]*@`)

// redactURIs masks the password of every URI in s.
func redactURIs(s string) string { return uriPassword.ReplaceAllString(s, "$1:******@") }

// endpoint describes where a context points, without credentials.
func endpoint(c config.Context) string {
	switch {
	case len(c.Brokers) > 0:
		return strings.Join(c.Brokers, ",")
	case c.URL != "":
		return redactURL(c.URL)
	case c.ManagementURL != "":
		return redactURL(c.ManagementURL)
	}
	return "-"
}

func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return redactURIs(raw)
	}
	u.User = nil
	return u.String()
}
