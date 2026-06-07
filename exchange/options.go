package exchange

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

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