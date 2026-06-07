package channel

import (
	"context"

	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/logger"
	amqp "github.com/rabbitmq/amqp091-go"
)

type OnChannelClose func(deadCh *amqp.Channel, conn *amqp.Connection)

type ChannelHandler struct {
	logger  *logger.Logger
	onClose OnChannelClose
}

func NewChannelHandler(log *logger.Logger, onClose OnChannelClose) *ChannelHandler {
	if log == nil {
		log = logger.New(logger.Production)
	}
	return &ChannelHandler{
		logger:  log,
		onClose: onClose,
	}
}

func (ch *ChannelHandler) GetChannel(ctx context.Context, conn *amqp.Connection) (*amqp.Channel, error) {
	if conn == nil || conn.IsClosed() {
		return nil, amqp.ErrClosed
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	go ch.HandleChannelClose(ctx, channel, conn)

	return channel, nil
}

func (ch *ChannelHandler) HandleChannelClose(ctx context.Context, channel *amqp.Channel, conn *amqp.Connection) {
	closeCh := channel.NotifyClose(make(chan *amqp.Error, 1))
	select {
	case err := <-closeCh:
		if err != nil {
			ch.logger.Warn("channel closed unexpectedly", "error", err)
		}
		if ch.onClose != nil {
			ch.onClose(channel, conn)
		}
	case <-ctx.Done():
		return
	}
}
