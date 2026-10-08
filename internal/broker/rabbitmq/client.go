package rabbitmq

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Max2535/mqx/internal/config"
)

// Context options (config `options:`) understood by the adapter.
const (
	// OptPeekMaxScan caps how many messages one peek of a classic or quorum
	// queue holds unacknowledged while looking for matches.
	OptPeekMaxScan = "peek_max_scan"
	// OptStreamIdle is how long a stream peek waits for the next message
	// before deciding it reached the end (a Go duration, e.g. 2s).
	OptStreamIdle = "stream_idle_timeout"

	defaultPeekMaxScan = 10000
	defaultStreamIdle  = time.Second
	defaultDialTimeout = 30 * time.Second
)

// settings is a validated context with credentials resolved.
type settings struct {
	scheme   string // amqp or amqps
	host     string // host:port of the AMQP listener
	vhost    string
	username string
	password string // never logged; see redact
	tls      *tls.Config
	mgmtURL  *url.URL

	peekMaxScan int
	streamIdle  time.Duration
}

func validate(c config.Context) []error {
	if c.URL == "" {
		return []error{errors.New("rabbitmq needs url, e.g. amqp://localhost:5672/")}
	}
	var errs []error
	if u, err := url.Parse(c.URL); err == nil {
		if u.Scheme != "amqp" && u.Scheme != "amqps" {
			errs = append(errs, fmt.Errorf("url scheme %q is not supported; use amqp:// or amqps://", u.Scheme))
		}
		if c.TLS != nil && c.TLS.Enabled && u.Scheme == "amqp" {
			errs = append(errs, errors.New("tls.enabled needs an amqps:// url"))
		}
	} // a parse error is reported by the generic validation without echoing the URL
	if c.ManagementURL != "" {
		if u, err := url.Parse(c.ManagementURL); err == nil && u.Scheme != "http" && u.Scheme != "https" {
			errs = append(errs, fmt.Errorf("management_url scheme %q is not supported; use http:// or https://", u.Scheme))
		}
	}
	if v, ok := c.Options[OptPeekMaxScan]; ok {
		if n, err := strconv.Atoi(v); err != nil || n < 1 {
			errs = append(errs, fmt.Errorf("options.%s must be a positive integer", OptPeekMaxScan))
		}
	}
	if v, ok := c.Options[OptStreamIdle]; ok {
		if d, err := time.ParseDuration(v); err != nil || d <= 0 {
			errs = append(errs, fmt.Errorf("options.%s must be a positive duration such as 2s", OptStreamIdle))
		}
	}
	return errs
}

// newSettings validates c and merges in the resolved credentials. When no
// username is configured (neither username_env nor a user in the URL) the
// RabbitMQ default guest is used, as amqp091 does.
func newSettings(c config.Context, creds config.Credentials) (settings, error) {
	if errs := validate(c); len(errs) > 0 {
		return settings{}, errors.Join(errs...)
	}
	uri, err := amqp.ParseURI(c.URL)
	if err != nil {
		// amqp091 errors do not quote the URL, but stay safe.
		return settings{}, errors.New("url is not a valid AMQP URI; expected amqp://host:5672/vhost")
	}
	s := settings{
		scheme:      uri.Scheme,
		host:        net.JoinHostPort(uri.Host, strconv.Itoa(uri.Port)),
		vhost:       uri.Vhost,
		username:    uri.Username,
		password:    uri.Password,
		peekMaxScan: defaultPeekMaxScan,
		streamIdle:  defaultStreamIdle,
	}
	if s.vhost == "" {
		s.vhost = "/"
	}
	if creds.Username != "" {
		s.username = creds.Username
	}
	if creds.Password != "" {
		s.password = creds.Password
	}
	if s.username == "" {
		s.username, s.password = "guest", "guest"
	}
	if v, ok := c.Options[OptPeekMaxScan]; ok {
		s.peekMaxScan, _ = strconv.Atoi(v)
	}
	if v, ok := c.Options[OptStreamIdle]; ok {
		s.streamIdle, _ = time.ParseDuration(v)
	}
	if s.tls, err = tlsConfig(c.TLS); err != nil {
		return settings{}, err
	}
	if s.mgmtURL, err = managementURL(c.ManagementURL, uri); err != nil {
		return settings{}, err
	}
	return s, nil
}

// managementURL returns the configured URL or http://<amqp host>:15672
// (https://<host>:15671 for amqps), RabbitMQ's default listener ports.
func managementURL(raw string, uri amqp.URI) (*url.URL, error) {
	if raw == "" {
		scheme, port := "http", "15672"
		if uri.Scheme == "amqps" {
			scheme, port = "https", "15671"
		}
		return &url.URL{Scheme: scheme, Host: net.JoinHostPort(uri.Host, port)}, nil
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, errors.New("management_url is not a valid URL")
	}
	u.User = nil // a username in the URL is ignored; credentials come from username_env/password_env
	u.Path = strings.TrimSuffix(u.Path, "/")
	u.RawPath = ""
	return u, nil
}

func tlsConfig(t *config.TLS) (*tls.Config, error) {
	if t == nil {
		return nil, nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: t.InsecureSkipVerify} //nolint:gosec // opt-in by config
	if t.CAFile != "" {
		pem, err := os.ReadFile(t.CAFile)
		if err != nil {
			return nil, fmt.Errorf("tls.ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("tls.ca_file %s: no PEM certificates found", t.CAFile)
		}
		cfg.RootCAs = pool
	}
	if t.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("tls.cert_file/key_file: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// connection returns the shared AMQP connection, dialling it on first use or
// after it was closed by the server.
func (r *RabbitMQ) connection(ctx context.Context) (*amqp.Connection, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.conn != nil && !r.conn.IsClosed() {
		return r.conn, nil
	}
	conn, err := r.dial(ctx)
	if err != nil {
		return nil, err
	}
	r.conn = conn
	return conn, nil
}

func (r *RabbitMQ) dial(ctx context.Context) (*amqp.Connection, error) {
	s := r.settings
	cfg := amqp.Config{
		SASL:       []amqp.Authentication{&amqp.PlainAuth{Username: s.username, Password: s.password}},
		Vhost:      s.vhost,
		Properties: amqp.Table{"connection_name": "mqx", "product": "mqx"},
		Dial: func(network, addr string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			deadline, ok := ctx.Deadline()
			if !ok {
				deadline = time.Now().Add(defaultDialTimeout)
			}
			// Bounds the TLS and AMQP handshakes; amqp091 clears it once open.
			if err := conn.SetDeadline(deadline); err != nil {
				conn.Close()
				return nil, err
			}
			return conn, nil
		},
	}
	if s.tls != nil {
		cfg.TLSClientConfig = s.tls.Clone()
	}
	// The URL carries no credentials: they travel only in the SASL config.
	u := url.URL{Scheme: s.scheme, Host: s.host, Path: "/"}
	conn, err := amqp.DialConfig(u.String(), cfg)
	if err != nil {
		return nil, fmt.Errorf("connect to AMQP %s://%s vhost %q: %w", s.scheme, s.host, s.vhost, r.amqpHint(err))
	}
	return conn, nil
}

// amqpHint adds an actionable hint to common AMQP connection failures.
func (r *RabbitMQ) amqpHint(err error) error {
	var aerr *amqp.Error
	if errors.As(err, &aerr) {
		switch aerr.Code {
		case amqp.AccessRefused:
			return fmt.Errorf("%w (check username_env/password_env and the user's access to the vhost)", err)
		case amqp.NotAllowed:
			return fmt.Errorf("%w (the vhost may not exist or the user has no permissions on it)", err)
		}
	}
	return r.redactErr(err)
}

// redactErr hides the password if an error message ever contains it.
func (r *RabbitMQ) redactErr(err error) error {
	if err == nil || len(r.settings.password) < 3 || !strings.Contains(err.Error(), r.settings.password) {
		return err
	}
	return redactedError{err: err, msg: strings.ReplaceAll(err.Error(), r.settings.password, "xxxxx")}
}

type redactedError struct {
	err error
	msg string
}

func (e redactedError) Error() string { return e.msg }
func (e redactedError) Unwrap() error { return e.err }

// channel opens a fresh AMQP channel; callers close it.
func (r *RabbitMQ) channel(ctx context.Context) (*amqp.Channel, error) {
	conn, err := r.connection(ctx)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open AMQP channel: %w", err)
	}
	return ch, nil
}

// amqpErr maps channel exceptions to broker errors with hints.
func amqpErr(op, name string, err error) error {
	var aerr *amqp.Error
	if errors.As(err, &aerr) {
		switch aerr.Code {
		case amqp.NotFound:
			return fmt.Errorf("%s %q: %s: %w", op, name, aerr.Reason, errNotFound)
		case amqp.AccessRefused:
			return fmt.Errorf("%s %q: %s (check the user's permissions)", op, name, aerr.Reason)
		}
	}
	return fmt.Errorf("%s %q: %w", op, name, err)
}
