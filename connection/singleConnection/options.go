package singleConn

import (
	"time"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ConnectionOptions struct {
	AmqpConfig           amqp.Config
	ReconnectInterval    time.Duration
	MaxReconnectAttempts int
}
