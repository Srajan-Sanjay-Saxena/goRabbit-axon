package integration_test

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/connection"
	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/exchange"
	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/helpers"
	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/producer"
)

func TestProducerPublishWithConfirm(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := connection.NewRabbitMqConnectionClass(connStr, connection.DefaultOptions())
	if err := conn.Connect(); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer conn.Shutdown()

	setupExchangeAndQueue(t, conn, "prod.test.ex", "prod.test.q", "prod.test.#")

	pub := producer.NewProducer("prod.test.ex", "prod.test.event")
	if err := pub.GetChannel(conn); err != nil {
		t.Fatalf("get channel failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := pub.Publish(ctx, []byte(`{"event": "test"}`), conn, helpers.RabbitMqPublisherConfig{
		Persistent: true,
	})
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}
}

func TestProducerPublishPersistent(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := connection.NewRabbitMqConnectionClass(connStr, connection.DefaultOptions())
	if err := conn.Connect(); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer conn.Shutdown()

	setupExchangeAndQueue(t, conn, "persist.ex", "persist.q", "persist.#")

	pub := producer.NewProducer("persist.ex", "persist.msg")
	if err := pub.GetChannel(conn); err != nil {
		t.Fatalf("get channel failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := pub.Publish(ctx, []byte(`{"persistent": true}`), conn, helpers.RabbitMqPublisherConfig{
		Persistent: true,
	})
	if err != nil {
		t.Fatalf("persistent publish failed: %v", err)
	}
}

func TestProducerPublishWithTTL(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := connection.NewRabbitMqConnectionClass(connStr, connection.DefaultOptions())
	if err := conn.Connect(); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer conn.Shutdown()

	setupExchangeAndQueue(t, conn, "ttl.ex", "ttl.q", "ttl.#")

	pub := producer.NewProducer("ttl.ex", "ttl.msg")
	if err := pub.GetChannel(conn); err != nil {
		t.Fatalf("get channel failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := pub.Publish(ctx, []byte(`{"otp": "1234"}`), conn, helpers.RabbitMqPublisherConfig{
		Persistent: true,
		Expiration: "5000",
	})
	if err != nil {
		t.Fatalf("publish with TTL failed: %v", err)
	}
}

func TestProducerPublishWithHeaders(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := connection.NewRabbitMqConnectionClass(connStr, connection.DefaultOptions())
	if err := conn.Connect(); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer conn.Shutdown()

	setupExchangeAndQueue(t, conn, "headers.ex", "headers.q", "headers.#")

	pub := producer.NewProducer("headers.ex", "headers.msg")
	if err := pub.GetChannel(conn); err != nil {
		t.Fatalf("get channel failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := pub.Publish(ctx, []byte(`{"data": "with headers"}`), conn, helpers.RabbitMqPublisherConfig{
		Persistent: true,
		Headers: amqp.Table{
			"x-source":    "integration-test",
			"x-operation": "insert",
		},
	})
	if err != nil {
		t.Fatalf("publish with headers failed: %v", err)
	}
}

func TestProducerPublishUnroutableReturnsError(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := connection.NewRabbitMqConnectionClass(connStr, connection.DefaultOptions())
	if err := conn.Connect(); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer conn.Shutdown()

	// Create exchange but NO queue bound to this routing key
	ex := exchange.NewRabbitExchange("unroutable.ex", exchange.Topic, helpers.RabbitExchangeOptions{Durable: true})
	if err := ex.CreateExchange(conn); err != nil {
		t.Fatalf("create exchange failed: %v", err)
	}

	pub := producer.NewProducer("unroutable.ex", "no.queue.bound.here")
	if err := pub.GetChannel(conn); err != nil {
		t.Fatalf("get channel failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := pub.Publish(ctx, []byte(`{"lost": true}`), conn, helpers.RabbitMqPublisherConfig{
		Persistent: true,
	})
	if err == nil {
		t.Fatal("expected error for unroutable message (mandatory=true)")
	}
}

func TestProducerPublishContextCancelled(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := connection.NewRabbitMqConnectionClass(connStr, connection.DefaultOptions())
	if err := conn.Connect(); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer conn.Shutdown()

	setupExchangeAndQueue(t, conn, "ctx.cancel.ex", "ctx.cancel.q", "ctx.cancel.#")

	pub := producer.NewProducer("ctx.cancel.ex", "ctx.cancel.key")
	pub.GetChannel(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(1 * time.Millisecond) // ensure context is expired

	err := pub.Publish(ctx, []byte(`{"cancelled": true}`), conn, helpers.RabbitMqPublisherConfig{})
	if err == nil {
		t.Fatal("expected error on expired context")
	}
}

func TestProducerMultiplePublishes(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	conn := connection.NewRabbitMqConnectionClass(connStr, connection.DefaultOptions())
	if err := conn.Connect(); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer conn.Shutdown()

	setupExchangeAndQueue(t, conn, "multi.ex", "multi.q", "multi.#")

	pub := producer.NewProducer("multi.ex", "multi.msg")
	if err := pub.GetChannel(conn); err != nil {
		t.Fatalf("get channel failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		err := pub.Publish(ctx, []byte(`{"seq": `+string(rune('0'+i))+`}`), conn, helpers.RabbitMqPublisherConfig{
			Persistent: true,
		})
		if err != nil {
			t.Fatalf("publish %d failed: %v", i, err)
		}
	}
}
