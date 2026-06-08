package helpers

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Srajan-Sanjay-Saxena/goRabbit-axon/channel"
)

type IRabbitConnection interface {
	GetChannel(ctx context.Context, onClose channel.OnChannelClose) (*amqp.Channel, error)
	Shutdown() error
}
