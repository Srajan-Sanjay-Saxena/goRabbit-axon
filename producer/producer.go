package producer

import (
	"context"
	"errors"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/connection"
	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/helpers"
)

type RabbitMqProducer struct {
	exchangeName  string
	routingKey    string
	channel       *amqp.Channel
	confirmCh     chan amqp.Confirmation
	returnCh      chan amqp.Return
	blockedCh     chan amqp.Blocking
	mode          helpers.ChannelMode
	fireAndForget bool
}

func NewProducer(exchangeName, routingKey string) *RabbitMqProducer {
	return &RabbitMqProducer{
		exchangeName: exchangeName,
		routingKey:   routingKey,
	}
}

func (rProd *RabbitMqProducer) GetChannel(rabbit *connection.RabbitMqConnectionClass, opts ...helpers.ProducerChannelOptions) error {
	ch, err := rabbit.Connection.Channel()
	if err != nil {
		return err
	}

	mode := helpers.Confirmed
	if len(opts) > 0 {
		mode = opts[0].Mode
	}
	rProd.mode = mode

	if mode == helpers.Unsafe {
		rProd.fireAndForget = opts[0].UnsafeOptions.FireAndForget
	}

	if mode == helpers.Confirmed || (mode == helpers.Unsafe && !rProd.fireAndForget) {
		if err := ch.Confirm(false); err != nil {
			ch.Close()
			return err
		}
		rProd.confirmCh = ch.NotifyPublish(make(chan amqp.Confirmation, 1))
		rProd.returnCh = ch.NotifyReturn(make(chan amqp.Return, 1))
		rProd.blockedCh = rabbit.Connection.NotifyBlocked(make(chan amqp.Blocking, 1))
	}

	rProd.channel = ch
	return nil
}

func (rProd *RabbitMqProducer) Publish(ctx context.Context, body []byte, rabbit *connection.RabbitMqConnectionClass, cfg helpers.RabbitMqPublisherConfig) error {
	if rProd.channel == nil {
		return errors.New("channel not initialized, call GetChannel first")
	}

	msg := rProd.BuildConfig(cfg)
	msg.Body = body

	err := rProd.channel.PublishWithContext(ctx, rProd.exchangeName, rProd.routingKey, rProd.mode == helpers.Confirmed, false, msg)
	if err != nil {
		return err
	}

	if rProd.mode == helpers.Unsafe && rProd.fireAndForget {
		return nil
	}

	select {
	case confirm := <-rProd.confirmCh:
		if !confirm.Ack {
			return errors.New("broker nacked the message")
		}
		return nil
	case ret := <-rProd.returnCh:
		return errors.New("message returned: " + ret.ReplyText)
	case <-rProd.blockedCh:
		<-rProd.confirmCh
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (rProd *RabbitMqProducer) BuildConfig(cfg helpers.RabbitMqPublisherConfig) amqp.Publishing {
	deliveryMode := amqp.Transient
	if cfg.Persistent {
		deliveryMode = amqp.Persistent
	}

	contentType := "application/json"
	if cfg.ContentType != nil {
		contentType = *cfg.ContentType
	}

	return amqp.Publishing{
		DeliveryMode: deliveryMode,
		Priority:     cfg.Priority,
		Expiration:   cfg.Expiration,
		ContentType:  contentType,
		Headers:      cfg.Headers,
	}
}
