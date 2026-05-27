package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/connection"
	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/exchange"
	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/helpers"
	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/producer"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
)

func startRabbitMQ(t *testing.T) (string, func()) {
	t.Helper()
	ctx := context.Background()

	container, err := rabbitmq.Run(ctx, "rabbitmq:3.13-management")
	if err != nil {
		t.Fatalf("failed to start rabbitmq container: %v", err)
	}

	connStr, err := container.AmqpURL(ctx)
	if err != nil {
		testcontainers.CleanupContainer(t, container)
		t.Fatalf("failed to get amqp url: %v", err)
	}

	cleanup := func() {
		testcontainers.CleanupContainer(t, container)
	}

	return connStr, cleanup
}

func setupExchangeAndQueue(t *testing.T, conn *connection.RabbitMqConnectionClass, exName, qName, bindingKey string) {
	t.Helper()
	ex := exchange.NewRabbitExchange(exName, exchange.Topic, helpers.RabbitExchangeOptions{Durable: true})
	if err := ex.CreateExchange(conn); err != nil {
		t.Fatalf("create exchange failed: %v", err)
	}
	if _, err := ex.CreateQueue(conn, helpers.RabbitQueueConfig{
		Name:       qName,
		BindingKey: bindingKey,
		Durable:    true,
	}); err != nil {
		t.Fatalf("create queue failed: %v", err)
	}
}

func publishMessages(t *testing.T, conn *connection.RabbitMqConnectionClass, exName, routingKey string, count int) {
	t.Helper()
	pub := producer.NewProducer(exName, routingKey)
	if err := pub.GetChannel(conn); err != nil {
		t.Fatalf("producer get channel failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < count; i++ {
		body := []byte(fmt.Sprintf(`{"msg": %d}`, i))
		if err := pub.Publish(ctx, body, conn, helpers.RabbitMqPublisherConfig{Persistent: true}); err != nil {
			t.Fatalf("publish %d failed: %v", i, err)
		}
	}
}
