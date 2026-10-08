package cli

import (
	// Register the real adapters so config validation knows kafka and rabbitmq.
	_ "github.com/Max2535/mqx/internal/broker/kafka"
	_ "github.com/Max2535/mqx/internal/broker/rabbitmq"
)
