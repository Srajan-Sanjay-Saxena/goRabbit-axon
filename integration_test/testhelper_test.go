package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Srajan-Sanjay-Saxena/goRabbit-axon/breaker"
	singleConn "github.com/Srajan-Sanjay-Saxena/goRabbit-axon/connection/singleConnection"
	"github.com/Srajan-Sanjay-Saxena/goRabbit-axon/exchange"
	"github.com/Srajan-Sanjay-Saxena/goRabbit-axon/helpers"
	"github.com/Srajan-Sanjay-Saxena/goRabbit-axon/producer"
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

func setupConn(t *testing.T, connStr string) *singleConn.RabbitMqSingleConnectionHandler {
	t.Helper()
	conn := singleConn.NewRabbitMqSingleConnectionHandler(connStr, singleConn.DefaultOptions(), nil)
	conn.AddBreaker(breaker.CircuitBreakerOptions{})
	if err := conn.Connect(context.Background()); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	return conn
}

func setupExchangeAndQueue(t *testing.T, conn helpers.IRabbitConnection, exName, qName, bindingKey string) {
	t.Helper()
	ctx := context.Background()
	ex := exchange.NewRabbitExchange(exName, exchange.Topic, exchange.RabbitExchangeOptions{Durable: true})
	if err := ex.CreateExchange(ctx, conn); err != nil {
		t.Fatalf("create exchange failed: %v", err)
	}
	if _, err := ex.CreateQueue(ctx, conn, exchange.RabbitQueueConfig{
		Name:       qName,
		BindingKey: bindingKey,
		Durable:    true,
	}); err != nil {
		t.Fatalf("create queue failed: %v", err)
	}
}

func publishMessages(t *testing.T, conn helpers.IRabbitConnection, exName, routingKey string, count int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pub := producer.NewProducer(exName, routingKey)
	if err := pub.GetChannel(ctx, conn); err != nil {
		t.Fatalf("producer get channel failed: %v", err)
	}

	for i := 0; i < count; i++ {
		body := []byte(fmt.Sprintf(`{"msg": %d}`, i))
		if err := pub.Publish(ctx, body, producer.RabbitMqPublisherConfig{Persistent: true}); err != nil {
			t.Fatalf("publish %d failed: %v", i, err)
		}
	}
}
