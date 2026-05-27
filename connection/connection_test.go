package connection

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/helpers"
)

func TestDefaultOptions(t *testing.T) {
	opts := DefaultOptions()

	if opts.AmqpConfig.Heartbeat != 60*time.Second {
		t.Errorf("expected heartbeat 60s, got %v", opts.AmqpConfig.Heartbeat)
	}
	if opts.ReconnectInterval != 5*time.Second {
		t.Errorf("expected reconnect interval 5s, got %v", opts.ReconnectInterval)
	}
	if opts.MaxReconnectAttempts != 10 {
		t.Errorf("expected max reconnect attempts 10, got %d", opts.MaxReconnectAttempts)
	}
}

func TestNewRabbitMqConnectionClass(t *testing.T) {
	opts := helpers.ConnectionOptions{
		AmqpConfig:           amqp.Config{Heartbeat: 30 * time.Second},
		ReconnectInterval:    2 * time.Second,
		MaxReconnectAttempts: 5,
	}

	conn := NewRabbitMqConnectionClass("amqp://guest:guest@localhost:5672/", opts)

	if conn.rabbitConnString != "amqp://guest:guest@localhost:5672/" {
		t.Errorf("unexpected conn string: %s", conn.rabbitConnString)
	}
	if conn.options.MaxReconnectAttempts != 5 {
		t.Errorf("expected 5, got %d", conn.options.MaxReconnectAttempts)
	}
	if conn.Connection != nil {
		t.Error("expected nil connection before Connect()")
	}
	if conn.isShuttingDown {
		t.Error("expected isShuttingDown to be false")
	}
	if conn.reconnectAttempts != 0 {
		t.Errorf("expected 0 reconnect attempts, got %d", conn.reconnectAttempts)
	}
}

func TestConnectFailsWithBadURL(t *testing.T) {
	opts := DefaultOptions()
	conn := NewRabbitMqConnectionClass("amqp://invalid:invalid@localhost:9999/", opts)

	err := conn.Connect()
	if err == nil {
		t.Error("expected error connecting to invalid URL")
		conn.Shutdown()
	}
}

func TestShutdownWithNilConnection(t *testing.T) {
	opts := DefaultOptions()
	conn := NewRabbitMqConnectionClass("amqp://guest:guest@localhost:5672/", opts)

	err := conn.Shutdown()
	if err != nil {
		t.Errorf("expected nil error on shutdown with nil connection, got %v", err)
	}
	if !conn.isShuttingDown {
		t.Error("expected isShuttingDown to be true after Shutdown()")
	}
}

func TestShutdownSetsFlag(t *testing.T) {
	opts := DefaultOptions()
	conn := NewRabbitMqConnectionClass("amqp://guest:guest@localhost:5672/", opts)

	conn.Shutdown()

	if !conn.isShuttingDown {
		t.Error("expected isShuttingDown = true")
	}
}
