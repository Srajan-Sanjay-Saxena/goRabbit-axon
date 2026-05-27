package helpers

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type ConnectionOptions struct {
	AmqpConfig           amqp.Config
	ReconnectInterval    time.Duration
	MaxReconnectAttempts int
}

type RabbitExchangeOptions struct {
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
}

type QueueType string

const (
	ClassicQueue QueueType = "classic"
	QuorumQueue  QueueType = "quorum"
)

type RabbitQueueConfig struct {
	Name       string
	BindingKey string
	QueueType  QueueType
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

type RabbitMqPublisherConfig struct {
	Persistent  bool
	Priority    uint8
	Expiration  string
	ContentType *string
	Headers     amqp.Table
}
