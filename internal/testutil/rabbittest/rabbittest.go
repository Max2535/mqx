// Package rabbittest starts a RabbitMQ broker in a container for integration
// tests: the management, shovel and federation plugins are enabled and
// management statistics refresh every 500ms.
//
// Start it once per test package from TestMain and terminate it afterwards:
//
//	srv, err := rabbittest.Start(ctx)
//	...
//	defer srv.Terminate(ctx)
package rabbittest

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/testcontainers/testcontainers-go"
	tcrabbit "github.com/testcontainers/testcontainers-go/modules/rabbitmq"

	"github.com/Max2535/mqx/internal/config"
)

// Image is the broker image the tests run against.
const Image = "rabbitmq:4-management"

// Environment variables holding the test credentials, referenced by Context.
const (
	EnvUser     = "MQX_RABBITTEST_USER"
	EnvPassword = "MQX_RABBITTEST_PASSWORD"
)

const (
	user     = "mqx"
	password = "mqx-test-secret"
	plugins  = "[rabbitmq_management,rabbitmq_prometheus,rabbitmq_shovel,rabbitmq_shovel_management," +
		"rabbitmq_federation,rabbitmq_federation_management].\n"
	extraConf = "collect_statistics_interval = 500\nloopback_users = none\n"
)

// Server is a running broker.
type Server struct {
	container *tcrabbit.RabbitMQContainer
	// AMQPHost is host:port of the AMQP listener; ManagementURL the HTTP API.
	AMQPHost      string
	ManagementURL string
	Username      string
	Password      string
}

// Start runs the container on random host ports and sets EnvUser/EnvPassword
// in the process environment.
func Start(ctx context.Context) (*Server, error) {
	c, err := tcrabbit.Run(ctx, Image,
		tcrabbit.WithAdminUsername(user),
		tcrabbit.WithAdminPassword(password),
		testcontainers.WithFiles(
			testcontainers.ContainerFile{
				Reader: strings.NewReader(plugins), ContainerFilePath: "/etc/rabbitmq/enabled_plugins", FileMode: 0o644,
			},
			testcontainers.ContainerFile{
				Reader: strings.NewReader(extraConf), ContainerFilePath: "/etc/rabbitmq/conf.d/90-mqx-test.conf",
				FileMode: 0o644,
			},
		),
	)
	if err != nil {
		if c != nil {
			_ = testcontainers.TerminateContainer(c)
		}
		return nil, fmt.Errorf("start %s: %w", Image, err)
	}
	host, err := c.PortEndpoint(ctx, tcrabbit.DefaultAMQPPort, "")
	if err != nil {
		_ = testcontainers.TerminateContainer(c)
		return nil, err
	}
	mgmt, err := c.HttpURL(ctx)
	if err != nil {
		_ = testcontainers.TerminateContainer(c)
		return nil, err
	}
	if err := os.Setenv(EnvUser, user); err != nil {
		return nil, err
	}
	if err := os.Setenv(EnvPassword, password); err != nil {
		return nil, err
	}
	return &Server{container: c, AMQPHost: host, ManagementURL: mgmt, Username: user, Password: password}, nil
}

// Terminate stops and removes the container.
func (s *Server) Terminate(context.Context) error {
	return testcontainers.TerminateContainer(s.container)
}

// Context returns a config context for vhost using the env-var credentials.
func (s *Server) Context(name, vhost string) config.Context {
	return config.Context{
		Name:          name,
		Broker:        "rabbitmq",
		URL:           s.AMQPURL(vhost, false),
		ManagementURL: s.ManagementURL,
		UsernameEnv:   EnvUser,
		PasswordEnv:   EnvPassword,
	}
}

// AMQPURL is the AMQP URL of vhost, with the credentials when withCreds is set
// (for raw amqp091 clients in tests only).
func (s *Server) AMQPURL(vhost string, withCreds bool) string {
	u := url.URL{Scheme: "amqp", Host: s.AMQPHost, Path: "/" + vhost, RawPath: "/" + url.PathEscape(vhost)}
	if withCreds {
		u.User = url.UserPassword(s.Username, s.Password)
	}
	return u.String()
}
