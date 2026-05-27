package helpers

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestQueueTypeConstants(t *testing.T) {
	if ClassicQueue != "classic" {
		t.Errorf("expected ClassicQueue = 'classic', got '%s'", ClassicQueue)
	}
	if QuorumQueue != "quorum" {
		t.Errorf("expected QuorumQueue = 'quorum', got '%s'", QuorumQueue)
	}
}

func TestConnectionOptionsDefaults(t *testing.T) {
	opts := ConnectionOptions{
		ReconnectInterval:    5 * time.Second,
		MaxReconnectAttempts: 10,
	}

	if opts.ReconnectInterval != 5*time.Second {
		t.Errorf("expected 5s, got %v", opts.ReconnectInterval)
	}
	if opts.MaxReconnectAttempts != 10 {
		t.Errorf("expected 10, got %d", opts.MaxReconnectAttempts)
	}
}

func TestRabbitQueueConfigWithQuorum(t *testing.T) {
	cfg := RabbitQueueConfig{
		Name:       "test.queue",
		BindingKey: "test.#",
		QueueType:  QuorumQueue,
		Durable:    true,
	}

	if cfg.Name != "test.queue" {
		t.Errorf("expected 'test.queue', got '%s'", cfg.Name)
	}
	if cfg.QueueType != QuorumQueue {
		t.Errorf("expected QuorumQueue, got '%s'", cfg.QueueType)
	}
	if !cfg.Durable {
		t.Error("expected Durable to be true")
	}
}

func TestRabbitMqPublisherConfigPersistent(t *testing.T) {
	cfg := RabbitMqPublisherConfig{
		Persistent: true,
		Priority:   5,
		Expiration: "60000",
	}

	if !cfg.Persistent {
		t.Error("expected Persistent to be true")
	}
	if cfg.Priority != 5 {
		t.Errorf("expected priority 5, got %d", cfg.Priority)
	}
	if cfg.Expiration != "60000" {
		t.Errorf("expected '60000', got '%s'", cfg.Expiration)
	}
}

func TestRabbitMqPublisherConfigContentType(t *testing.T) {
	ct := "text/plain"
	cfg := RabbitMqPublisherConfig{
		ContentType: &ct,
	}

	if cfg.ContentType == nil {
		t.Fatal("expected ContentType to be set")
	}
	if *cfg.ContentType != "text/plain" {
		t.Errorf("expected 'text/plain', got '%s'", *cfg.ContentType)
	}
}

func TestRabbitMqPublisherConfigNilContentType(t *testing.T) {
	cfg := RabbitMqPublisherConfig{
		Persistent: true,
	}

	if cfg.ContentType != nil {
		t.Error("expected ContentType to be nil")
	}
}

func TestRabbitExchangeOptions(t *testing.T) {
	opts := RabbitExchangeOptions{
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
	}

	if !opts.Durable {
		t.Error("expected Durable true")
	}
	if opts.AutoDelete {
		t.Error("expected AutoDelete false")
	}
}

func TestRabbitQueueConfigWithArgs(t *testing.T) {
	cfg := RabbitQueueConfig{
		Name:      "dlq.test",
		Durable:   true,
		QueueType: ClassicQueue,
		Args: amqp.Table{
			"x-dead-letter-exchange": "dlx.exchange",
			"x-message-ttl":          int32(60000),
		},
	}

	if cfg.Args["x-dead-letter-exchange"] != "dlx.exchange" {
		t.Error("expected dead letter exchange arg")
	}
	if cfg.Args["x-message-ttl"] != int32(60000) {
		t.Error("expected message ttl arg")
	}
}
