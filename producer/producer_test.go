package producer

import (
	"context"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/helpers"
)

func TestNewProducer(t *testing.T) {
	pub := NewProducer("test.exchange", "test.routing.key")

	if pub.exchangeName != "test.exchange" {
		t.Errorf("expected 'test.exchange', got '%s'", pub.exchangeName)
	}
	if pub.routingKey != "test.routing.key" {
		t.Errorf("expected 'test.routing.key', got '%s'", pub.routingKey)
	}
	if pub.channel != nil {
		t.Error("expected nil channel before GetChannel()")
	}
}

func TestBuildConfigPersistent(t *testing.T) {
	pub := NewProducer("ex", "rk")

	msg := pub.BuildConfig(helpers.RabbitMqPublisherConfig{
		Persistent: true,
	})

	if msg.DeliveryMode != amqp.Persistent {
		t.Errorf("expected Persistent delivery mode, got %d", msg.DeliveryMode)
	}
}

func TestBuildConfigTransient(t *testing.T) {
	pub := NewProducer("ex", "rk")

	msg := pub.BuildConfig(helpers.RabbitMqPublisherConfig{
		Persistent: false,
	})

	if msg.DeliveryMode != amqp.Transient {
		t.Errorf("expected Transient delivery mode, got %d", msg.DeliveryMode)
	}
}

func TestBuildConfigDefaultContentType(t *testing.T) {
	pub := NewProducer("ex", "rk")

	msg := pub.BuildConfig(helpers.RabbitMqPublisherConfig{})

	if msg.ContentType != "application/json" {
		t.Errorf("expected 'application/json', got '%s'", msg.ContentType)
	}
}

func TestBuildConfigCustomContentType(t *testing.T) {
	pub := NewProducer("ex", "rk")
	ct := "text/plain"

	msg := pub.BuildConfig(helpers.RabbitMqPublisherConfig{
		ContentType: &ct,
	})

	if msg.ContentType != "text/plain" {
		t.Errorf("expected 'text/plain', got '%s'", msg.ContentType)
	}
}

func TestBuildConfigPriority(t *testing.T) {
	pub := NewProducer("ex", "rk")

	msg := pub.BuildConfig(helpers.RabbitMqPublisherConfig{
		Priority: 9,
	})

	if msg.Priority != 9 {
		t.Errorf("expected priority 9, got %d", msg.Priority)
	}
}

func TestBuildConfigExpiration(t *testing.T) {
	pub := NewProducer("ex", "rk")

	msg := pub.BuildConfig(helpers.RabbitMqPublisherConfig{
		Expiration: "60000",
	})

	if msg.Expiration != "60000" {
		t.Errorf("expected '60000', got '%s'", msg.Expiration)
	}
}

func TestBuildConfigHeaders(t *testing.T) {
	pub := NewProducer("ex", "rk")

	headers := amqp.Table{
		"x-source":  "cdc",
		"x-version": "1.0",
	}

	msg := pub.BuildConfig(helpers.RabbitMqPublisherConfig{
		Headers: headers,
	})

	if msg.Headers["x-source"] != "cdc" {
		t.Error("expected x-source header")
	}
	if msg.Headers["x-version"] != "1.0" {
		t.Error("expected x-version header")
	}
}

func TestPublishFailsWithoutChannel(t *testing.T) {
	pub := NewProducer("ex", "rk")

	err := pub.Publish(context.Background(), []byte("test"), nil, helpers.RabbitMqPublisherConfig{})
	if err == nil {
		t.Error("expected error when publishing without channel")
	}
	if err.Error() != "channel not initialized, call GetChannel first" {
		t.Errorf("unexpected error: %s", err.Error())
	}
}

func TestBuildConfigFullOptions(t *testing.T) {
	pub := NewProducer("ex", "rk")
	ct := "application/xml"

	msg := pub.BuildConfig(helpers.RabbitMqPublisherConfig{
		Persistent:  true,
		Priority:    5,
		Expiration:  "30000",
		ContentType: &ct,
		Headers:     amqp.Table{"x-retry": int32(3)},
	})

	if msg.DeliveryMode != amqp.Persistent {
		t.Error("expected persistent")
	}
	if msg.Priority != 5 {
		t.Error("expected priority 5")
	}
	if msg.Expiration != "30000" {
		t.Error("expected expiration 30000")
	}
	if msg.ContentType != "application/xml" {
		t.Error("expected application/xml")
	}
	if msg.Headers["x-retry"] != int32(3) {
		t.Error("expected x-retry header")
	}
}
