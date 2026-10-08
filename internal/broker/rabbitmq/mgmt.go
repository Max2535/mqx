package rabbitmq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/Max2535/mqx/internal/broker"
)

var errNotFound = broker.ErrNotFound

// mgmtClient calls the RabbitMQ Management HTTP API with basic auth.
type mgmtClient struct {
	base     *url.URL
	username string
	password string
	hc       *http.Client
}

func newMgmtClient(s settings) *mgmtClient {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if s.tls != nil && s.mgmtURL.Scheme == "https" {
		tr.TLSClientConfig = s.tls.Clone()
	}
	return &mgmtClient{base: s.mgmtURL, username: s.username, password: s.password, hc: &http.Client{Transport: tr}}
}

// apiPath joins segments under /api, percent-encoding each one so that the
// "/" vhost becomes %2F and names may contain any character.
func apiPath(segments ...string) string {
	var b strings.Builder
	b.WriteString("/api")
	for _, s := range segments {
		b.WriteByte('/')
		b.WriteString(url.PathEscape(s))
	}
	return b.String()
}

// withQuery appends a query string to a path built by apiPath.
func withQuery(path string, q url.Values) string {
	if len(q) == 0 {
		return path
	}
	return path + "?" + q.Encode()
}

// apiError is a non-2xx management API response.
type apiError struct {
	Method string
	Path   string
	Status int
	Reason string
}

func (e *apiError) Error() string {
	msg := fmt.Sprintf("management API %s %s: %d %s", e.Method, e.Path, e.Status, http.StatusText(e.Status))
	if e.Reason != "" {
		msg += ": " + e.Reason
	}
	switch e.Status {
	case http.StatusUnauthorized:
		msg += " (check username_env/password_env; the user needs the management or monitoring tag)"
	case http.StatusForbidden:
		msg += " (the user lacks the permission or tag this needs, e.g. administrator or policymaker)"
	}
	return msg
}

// Unwrap lets errors.Is(err, broker.ErrNotFound) match 404s.
func (e *apiError) Unwrap() error {
	if e.Status == http.StatusNotFound {
		return errNotFound
	}
	return nil
}

func statusOf(err error) int {
	var ae *apiError
	if errors.As(err, &ae) {
		return ae.Status
	}
	return 0
}

func (m *mgmtClient) get(ctx context.Context, path string, out any) error {
	return m.do(ctx, http.MethodGet, path, nil, out, nil)
}

func (m *mgmtClient) put(ctx context.Context, path string, body any) error {
	return m.do(ctx, http.MethodPut, path, body, nil, nil)
}

func (m *mgmtClient) post(ctx context.Context, path string, body any) error {
	return m.do(ctx, http.MethodPost, path, body, nil, nil)
}

func (m *mgmtClient) delete(ctx context.Context, path string, hdr http.Header) error {
	return m.do(ctx, http.MethodDelete, path, nil, nil, hdr)
}

func (m *mgmtClient) do(ctx context.Context, method, path string, body, out any, hdr http.Header) error {
	var rd io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode request body: %w", err)
		}
		rd = bytes.NewReader(data)
	}
	full := m.base.Scheme + "://" + m.base.Host + m.base.EscapedPath() + path
	req, err := http.NewRequestWithContext(ctx, method, full, rd)
	if err != nil {
		return fmt.Errorf("management API %s %s: %w", method, path, err)
	}
	req.SetBasicAuth(m.username, m.password)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, vs := range hdr {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	resp, err := m.hc.Do(req)
	if err != nil {
		return fmt.Errorf("management API %s %s (is the management plugin enabled and management_url right?): %w",
			method, path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return fmt.Errorf("management API %s %s: read response: %w", method, path, err)
	}
	if resp.StatusCode >= 300 {
		return &apiError{Method: method, Path: path, Status: resp.StatusCode, Reason: reasonOf(data)}
	}
	if out == nil || len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("management API %s %s: decode response: %w", method, path, err)
	}
	return nil
}

// reasonOf extracts RabbitMQ's {"error": ..., "reason": ...} body, redacting URIs.
func reasonOf(data []byte) string {
	var e struct {
		Error  string `json:"error"`
		Reason string `json:"reason"`
	}
	if json.Unmarshal(data, &e) != nil {
		return ""
	}
	r := e.Reason
	if r == "" {
		r = e.Error
	}
	return redactURIs(strings.TrimSpace(r))
}

var uriPassword = regexp.MustCompile(`(://[^:/@\s"']*):[^@\s"']*@`)

// redactURIs replaces the password of every user:password@ URI in s.
func redactURIs(s string) string {
	return uriPassword.ReplaceAllString(s, "$1:xxxxx@")
}
