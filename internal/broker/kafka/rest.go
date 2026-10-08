package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/config"
)

// maxErrorBody bounds how much of an error response is quoted in messages.
const maxErrorBody = 512

// restClient talks JSON to an HTTP service next to Kafka (Connect, ksqlDB).
type restClient struct {
	service string // for messages, e.g. "kafka connect"
	base    string
	creds   config.Credentials
	http    *http.Client
}

func newRESTClient(service, baseURL string, creds config.Credentials) *restClient {
	return &restClient{service: service, base: strings.TrimRight(baseURL, "/"), creds: creds, http: &http.Client{}}
}

// statusError is a non-2xx response.
type statusError struct {
	service, method, path string
	status                int
	message               string
}

func (e *statusError) Error() string {
	return fmt.Sprintf("%s %s %s: HTTP %d: %s", e.service, e.method, e.path, e.status, e.message)
}

// Is maps 404 onto broker.ErrNotFound.
func (e *statusError) Is(target error) bool {
	return target == broker.ErrNotFound && e.status == http.StatusNotFound
}

// do sends body (JSON-encoded unless nil) and decodes a 2xx response into out (unless nil).
func (c *restClient) do(ctx context.Context, method, path string, body, out any) error {
	var rd io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode %s request: %w", c.service, err)
		}
		rd = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return fmt.Errorf("%s request: %w", c.service, err)
	}
	c.prepare(req, body != nil)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%s at %s is unreachable; check the url in the context: %w", c.service, c.base, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read %s response: %w", c.service, err)
	}
	if resp.StatusCode >= 300 {
		return c.statusErr(method, path, resp.StatusCode, data)
	}
	if out == nil || len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("decode %s response of %s %s: %w", c.service, method, path, err)
	}
	return nil
}

func (c *restClient) prepare(req *http.Request, hasBody bool) {
	req.Header.Set("Accept", "application/json")
	if hasBody {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.creds.Username != "" || c.creds.Password != "" {
		req.SetBasicAuth(c.creds.Username, c.creds.Password)
	}
}

func (c *restClient) statusErr(method, path string, status int, body []byte) error {
	msg := strings.TrimSpace(string(body))
	var parsed struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(body, &parsed) == nil && parsed.Message != "" {
		msg = parsed.Message
	}
	if len(msg) > maxErrorBody {
		msg = msg[:maxErrorBody] + "..."
	}
	if msg == "" {
		msg = http.StatusText(status)
	}
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		msg += "; check username_env / password_env of the endpoint"
	}
	return &statusError{service: c.service, method: method, path: path, status: status, message: msg}
}

// isNotFound reports whether err is a 404 from a REST service.
func isNotFound(err error) bool { return errors.Is(err, broker.ErrNotFound) }
