package integration_test

import (
	"testing"
	"time"

	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/connection"
	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/helpers"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestConnectionConnect(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := connection.NewRabbitMqConnectionClass(connStr, connection.DefaultOptions())
	err := conn.Connect()
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Shutdown()

	if conn.Connection == nil {
		t.Fatal("expected non-nil connection")
	}
	if conn.Connection.IsClosed() {
		t.Fatal("expected open connection")
	}
}

func TestConnectionShutdown(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := connection.NewRabbitMqConnectionClass(connStr, connection.DefaultOptions())
	if err := conn.Connect(); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	err := conn.Shutdown()
	if err != nil {
		t.Fatalf("shutdown error: %v", err)
	}

	if !conn.Connection.IsClosed() {
		t.Fatal("expected connection to be closed after shutdown")
	}
}

func TestConnectionOpenChannel(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := connection.NewRabbitMqConnectionClass(connStr, connection.DefaultOptions())
	if err := conn.Connect(); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Shutdown()

	ch, err := conn.Connection.Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()
}

func TestConnectionCustomOptions(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	opts := helpers.ConnectionOptions{
		AmqpConfig:           amqp.Config{Heartbeat: 10 * time.Second},
		ReconnectInterval:    2 * time.Second,
		MaxReconnectAttempts: 3,
	}

	conn := connection.NewRabbitMqConnectionClass(connStr, opts)
	if err := conn.Connect(); err != nil {
		t.Fatalf("failed to connect with custom opts: %v", err)
	}
	defer conn.Shutdown()

	if conn.Connection.IsClosed() {
		t.Fatal("expected open connection")
	}
}
