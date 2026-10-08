package kafka

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"

	"github.com/Max2535/mqx/internal/broker"
)

// Kafka Connect REST payloads.
type (
	connectStatus struct {
		Name      string `json:"name"`
		Type      string `json:"type"`
		Connector struct {
			State    string `json:"state"`
			WorkerID string `json:"worker_id"`
			Trace    string `json:"trace"`
		} `json:"connector"`
		Tasks []struct {
			ID       int    `json:"id"`
			State    string `json:"state"`
			WorkerID string `json:"worker_id"`
			Trace    string `json:"trace"`
		} `json:"tasks"`
	}
	connectInfo struct {
		Name   string            `json:"name"`
		Type   string            `json:"type"`
		Config map[string]string `json:"config"`
	}
	connectExpanded struct {
		Status *connectStatus `json:"status"`
		Info   *connectInfo   `json:"info"`
	}
)

func (k *Kafka) connectClient() (*restClient, error) {
	if k.connect == nil {
		return nil, fmt.Errorf("kafka connect is not configured; add connect.url to the context: %w", broker.ErrUnsupported)
	}
	return k.connect, nil
}

func connectorPath(name string, suffix string) string {
	return "/connectors/" + url.PathEscape(name) + suffix
}

// Connectors implements broker.ConnectManager.
func (k *Kafka) Connectors(ctx context.Context) ([]broker.Connector, error) {
	c, err := k.connectClient()
	if err != nil {
		return nil, err
	}
	var expanded map[string]connectExpanded
	if err := c.do(ctx, http.MethodGet, "/connectors?expand=status&expand=info", nil, &expanded); err != nil {
		return nil, err
	}
	out := make([]broker.Connector, 0, len(expanded))
	for name, e := range expanded {
		out = append(out, toConnector(name, e.Info, e.Status))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Connector implements broker.ConnectManager.
func (k *Kafka) Connector(ctx context.Context, name string) (*broker.Connector, error) {
	c, err := k.connectClient()
	if err != nil {
		return nil, err
	}
	var info connectInfo
	if err := c.do(ctx, http.MethodGet, connectorPath(name, ""), nil, &info); err != nil {
		return nil, connectorErr(name, err)
	}
	var status connectStatus
	if err := c.do(ctx, http.MethodGet, connectorPath(name, "/status"), nil, &status); err != nil {
		return nil, connectorErr(name, err)
	}
	conn := toConnector(name, &info, &status)
	return &conn, nil
}

func toConnector(name string, info *connectInfo, status *connectStatus) broker.Connector {
	c := broker.Connector{Name: name}
	if info != nil {
		c.Type, c.Config = info.Type, info.Config
	}
	if status != nil {
		if c.Type == "" {
			c.Type = status.Type
		}
		c.State, c.Worker = status.Connector.State, status.Connector.WorkerID
		for _, t := range status.Tasks {
			c.Tasks = append(c.Tasks, broker.ConnectorTask{ID: t.ID, State: t.State, Worker: t.WorkerID, Trace: t.Trace})
		}
		sort.Slice(c.Tasks, func(i, j int) bool { return c.Tasks[i].ID < c.Tasks[j].ID })
	}
	return c
}

// ConnectorPlugins implements broker.ConnectManager.
func (k *Kafka) ConnectorPlugins(ctx context.Context) ([]broker.ConnectorPlugin, error) {
	c, err := k.connectClient()
	if err != nil {
		return nil, err
	}
	var plugins []broker.ConnectorPlugin
	if err := c.do(ctx, http.MethodGet, "/connector-plugins", nil, &plugins); err != nil {
		return nil, err
	}
	sort.Slice(plugins, func(i, j int) bool { return plugins[i].Class < plugins[j].Class })
	return plugins, nil
}

// PutConnector implements broker.ConnectManager: it creates the connector or
// replaces its config.
func (k *Kafka) PutConnector(ctx context.Context, name string, cfg map[string]string) error {
	c, err := k.connectClient()
	if err != nil {
		return err
	}
	if cfg["connector.class"] == "" {
		return errors.New("connector config needs connector.class; run `mqx connect plugins` to list installed classes")
	}
	return c.do(ctx, http.MethodPut, connectorPath(name, "/config"), cfg, nil)
}

// DeleteConnector implements broker.ConnectManager.
func (k *Kafka) DeleteConnector(ctx context.Context, name string) error {
	c, err := k.connectClient()
	if err != nil {
		return err
	}
	return connectorErr(name, c.do(ctx, http.MethodDelete, connectorPath(name, ""), nil, nil))
}

// ConnectorAction implements broker.ConnectManager. Restart includes tasks.
func (k *Kafka) ConnectorAction(ctx context.Context, name string, action broker.ConnectorAction) error {
	c, err := k.connectClient()
	if err != nil {
		return err
	}
	switch action {
	case broker.ConnectorPause:
		err = c.do(ctx, http.MethodPut, connectorPath(name, "/pause"), nil, nil)
	case broker.ConnectorResume:
		err = c.do(ctx, http.MethodPut, connectorPath(name, "/resume"), nil, nil)
	case broker.ConnectorRestart:
		err = c.do(ctx, http.MethodPost, connectorPath(name, "/restart?includeTasks=true"), nil, nil)
	default:
		return fmt.Errorf("connector action %q: use pause, resume or restart: %w", action, broker.ErrUnsupported)
	}
	return connectorErr(name, err)
}

func connectorErr(name string, err error) error {
	if err != nil && isNotFound(err) {
		return fmt.Errorf("connector %q: %w; run `mqx connect list` to list connectors", name, err)
	}
	return err
}
