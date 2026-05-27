package integration_test

import (
	"testing"

	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/connection"
	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/exchange"
	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/helpers"
)

func TestCreateTopicExchange(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := connection.NewRabbitMqConnectionClass(connStr, connection.DefaultOptions())
	if err := conn.Connect(); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer conn.Shutdown()

	ex := exchange.NewRabbitExchange("test.topic.exchange", exchange.Topic, helpers.RabbitExchangeOptions{
		Durable: true,
	})

	if err := ex.CreateExchange(conn); err != nil {
		t.Fatalf("create exchange failed: %v", err)
	}
}

func TestCreateDirectExchange(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := connection.NewRabbitMqConnectionClass(connStr, connection.DefaultOptions())
	if err := conn.Connect(); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer conn.Shutdown()

	ex := exchange.NewRabbitExchange("test.direct.exchange", exchange.Direct, helpers.RabbitExchangeOptions{
		Durable: true,
	})

	if err := ex.CreateExchange(conn); err != nil {
		t.Fatalf("create direct exchange failed: %v", err)
	}
}

func TestCreateFanoutExchange(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := connection.NewRabbitMqConnectionClass(connStr, connection.DefaultOptions())
	if err := conn.Connect(); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer conn.Shutdown()

	ex := exchange.NewRabbitExchange("test.fanout.exchange", exchange.Fanout, helpers.RabbitExchangeOptions{
		Durable: true,
	})

	if err := ex.CreateExchange(conn); err != nil {
		t.Fatalf("create fanout exchange failed: %v", err)
	}
}

func TestCreateQueueAndBind(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := connection.NewRabbitMqConnectionClass(connStr, connection.DefaultOptions())
	if err := conn.Connect(); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer conn.Shutdown()

	ex := exchange.NewRabbitExchange("test.exchange.q", exchange.Topic, helpers.RabbitExchangeOptions{
		Durable: true,
	})
	if err := ex.CreateExchange(conn); err != nil {
		t.Fatalf("create exchange failed: %v", err)
	}

	q, err := ex.CreateQueue(conn, helpers.RabbitQueueConfig{
		Name:       "test.queue.bind",
		BindingKey: "test.#",
		Durable:    true,
	})
	if err != nil {
		t.Fatalf("create queue failed: %v", err)
	}
	if q.Name != "test.queue.bind" {
		t.Errorf("expected queue name 'test.queue.bind', got '%s'", q.Name)
	}
}

func TestCreateQuorumQueue(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := connection.NewRabbitMqConnectionClass(connStr, connection.DefaultOptions())
	if err := conn.Connect(); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer conn.Shutdown()

	ex := exchange.NewRabbitExchange("test.exchange.quorum", exchange.Topic, helpers.RabbitExchangeOptions{
		Durable: true,
	})
	if err := ex.CreateExchange(conn); err != nil {
		t.Fatalf("create exchange failed: %v", err)
	}

	q, err := ex.CreateQueue(conn, helpers.RabbitQueueConfig{
		Name:       "test.quorum.queue",
		BindingKey: "quorum.#",
		QueueType:  helpers.QuorumQueue,
		Durable:    true,
	})
	if err != nil {
		t.Fatalf("create quorum queue failed: %v", err)
	}
	if q.Name != "test.quorum.queue" {
		t.Errorf("expected 'test.quorum.queue', got '%s'", q.Name)
	}
}

func TestCreateClassicQueue(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := connection.NewRabbitMqConnectionClass(connStr, connection.DefaultOptions())
	if err := conn.Connect(); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer conn.Shutdown()

	ex := exchange.NewRabbitExchange("test.exchange.classic", exchange.Direct, helpers.RabbitExchangeOptions{
		Durable: true,
	})
	if err := ex.CreateExchange(conn); err != nil {
		t.Fatalf("create exchange failed: %v", err)
	}

	q, err := ex.CreateQueue(conn, helpers.RabbitQueueConfig{
		Name:       "test.classic.queue",
		BindingKey: "classic.key",
		QueueType:  helpers.ClassicQueue,
		Durable:    true,
	})
	if err != nil {
		t.Fatalf("create classic queue failed: %v", err)
	}
	if q.Name != "test.classic.queue" {
		t.Errorf("expected 'test.classic.queue', got '%s'", q.Name)
	}
}

func TestCreateExchangeIdempotent(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := connection.NewRabbitMqConnectionClass(connStr, connection.DefaultOptions())
	if err := conn.Connect(); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer conn.Shutdown()

	ex := exchange.NewRabbitExchange("test.idempotent.ex", exchange.Topic, helpers.RabbitExchangeOptions{
		Durable: true,
	})

	// Declare twice — should not error
	if err := ex.CreateExchange(conn); err != nil {
		t.Fatalf("first declare failed: %v", err)
	}
	if err := ex.CreateExchange(conn); err != nil {
		t.Fatalf("second declare (idempotent) failed: %v", err)
	}
}

func TestCreateQueueIdempotent(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := connection.NewRabbitMqConnectionClass(connStr, connection.DefaultOptions())
	if err := conn.Connect(); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer conn.Shutdown()

	ex := exchange.NewRabbitExchange("test.idem.queue.ex", exchange.Topic, helpers.RabbitExchangeOptions{Durable: true})
	ex.CreateExchange(conn)

	cfg := helpers.RabbitQueueConfig{
		Name:       "test.idem.queue",
		BindingKey: "idem.#",
		Durable:    true,
	}

	// Declare twice
	if _, err := ex.CreateQueue(conn, cfg); err != nil {
		t.Fatalf("first queue declare failed: %v", err)
	}
	if _, err := ex.CreateQueue(conn, cfg); err != nil {
		t.Fatalf("second queue declare (idempotent) failed: %v", err)
	}
}
