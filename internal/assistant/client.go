package assistant

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/anthropics/anthropic-sdk-go"
)

// NewSender returns a Sender that calls the Anthropic API. The SDK finds the
// credentials: ANTHROPIC_API_KEY, ANTHROPIC_AUTH_TOKEN or an `ant auth login`
// profile.
func NewSender() Sender {
	client := anthropic.NewClient()
	return func(ctx context.Context, p anthropic.BetaMessageNewParams) (*anthropic.BetaMessage, error) {
		msg, err := client.Beta.Messages.New(ctx, p)
		return msg, explain(err)
	}
}

// explain turns API errors into something the user can act on.
func explain(err error) error {
	var apiErr *anthropic.Error
	if !errors.As(err, &apiErr) {
		return err
	}
	switch apiErr.StatusCode {
	case http.StatusUnauthorized:
		return fmt.Errorf("the API rejected the credentials: set ANTHROPIC_API_KEY or run `ant auth login`: %w", err)
	case http.StatusForbidden:
		return fmt.Errorf("the API key may not use this model; set assistant.model in the config: %w", err)
	case http.StatusNotFound:
		return fmt.Errorf("unknown model; check assistant.model in the config: %w", err)
	case http.StatusTooManyRequests:
		return fmt.Errorf("rate limited by the API; wait a moment and ask again: %w", err)
	}
	return err
}
