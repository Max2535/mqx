package broker

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/Max2535/mqx/internal/config"
)

type stubBroker struct{ name string }

func (s stubBroker) Name() string                                 { return s.name }
func (stubBroker) Ping(context.Context) error                     { return nil }
func (stubBroker) ListTopics(context.Context) ([]Topic, error)    { return nil, nil }
func (stubBroker) Publish(context.Context, string, Message) error { return nil }
func (stubBroker) Close() error                                   { return nil }
func (stubBroker) Peek(context.Context, string, PeekOptions) (<-chan Message, error) {
	return nil, nil
}

type lagStub struct{ stubBroker }

func (lagStub) Lag(context.Context, string) ([]PartitionLag, error) { return nil, nil }

type hiddenLagStub struct{ lagStub }

func (hiddenLagStub) HasCapability(name string) bool { return name != CapLagReporter }

func TestRegistry(t *testing.T) {
	Register("stub-test", Driver{
		Open: func(_ context.Context, c config.Context, creds config.Credentials) (Broker, error) {
			if creds.Password != "pw" {
				return nil, errors.New("credentials not resolved")
			}
			return stubBroker{name: c.Broker}, nil
		},
		Validate: func(c config.Context) []error {
			if c.URL == "" {
				return []error{errors.New("needs url")}
			}
			return nil
		},
	})
	t.Cleanup(func() {
		mu.Lock()
		delete(drivers, "stub-test")
		mu.Unlock()
	})

	if !slices.Contains(Types(), "stub-test") {
		t.Fatalf("Types() = %v, missing stub-test", Types())
	}

	t.Setenv("STUB_PW", "pw")
	b, err := Open(context.Background(), config.Context{Name: "s", Broker: "stub-test", PasswordEnv: "STUB_PW"})
	if err != nil || b.Name() != "stub-test" {
		t.Fatalf("Open() = %v, %v", b, err)
	}

	_, err = Open(context.Background(), config.Context{Name: "s", Broker: "nope"})
	if err == nil || !strings.Contains(err.Error(), `unknown broker "nope"`) {
		t.Errorf("Open(unknown) error = %v", err)
	}

	v := Validator()
	if errs, known := v.ValidateContext(config.Context{Broker: "stub-test"}); !known || len(errs) != 1 {
		t.Errorf("ValidateContext = %v, %v; want 1 error, known", errs, known)
	}
	if _, known := v.ValidateContext(config.Context{Broker: "nope"}); known {
		t.Errorf("unknown broker reported as known")
	}
}

func TestRegisterPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("Register with nil Open did not panic")
		}
	}()
	Register("nil-open", Driver{})
}

func TestCapabilities(t *testing.T) {
	if got := Capabilities(stubBroker{}); len(got) != 0 {
		t.Errorf("core-only broker capabilities = %v, want none", got)
	}
	if got := Capabilities(lagStub{}); !slices.Equal(got, []string{CapLagReporter}) {
		t.Errorf("lag broker capabilities = %v", got)
	}
	if Has(hiddenLagStub{}, CapLagReporter) {
		t.Error("CapabilityChecker did not hide lag")
	}
	if _, err := As[LagReporter](hiddenLagStub{}, CapLagReporter); !errors.Is(err, ErrUnsupported) {
		t.Errorf("As on hidden capability error = %v, want ErrUnsupported", err)
	}
	if _, err := As[LagReporter](lagStub{}, CapLagReporter); err != nil {
		t.Errorf("As on supported capability error = %v", err)
	}
}
