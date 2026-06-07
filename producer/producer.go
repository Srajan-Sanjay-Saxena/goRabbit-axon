package producer

import (
	"context"
	"errors"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/helpers"
)

type RabbitMqProducer struct {
	exchangeName  string
	routingKey    string
	channel       *amqp.Channel
	confirmCh     chan amqp.Confirmation
	returnCh      chan amqp.Return
	mode          ChannelMode
	fireAndForget bool
}

func NewProducer(exchangeName, routingKey string) *RabbitMqProducer {
	return &RabbitMqProducer{
		exchangeName: exchangeName,
		routingKey:   routingKey,
	}
}

func (rProd *RabbitMqProducer) GetChannel(conn helpers.IRabbitConnection, opts ...ProducerChannelOptions) error {
	ch, err := conn.GetChannel()
	if err != nil {
		return err
	}

	mode := Confirmed
	if len(opts) > 0 {
		mode = opts[0].Mode
	}
	rProd.mode = mode

	if mode == Unsafe && len(opts) > 0 {
		rProd.fireAndForget = opts[0].UnsafeOptions.FireAndForget
	}

	if mode == Confirmed || (mode == Unsafe && !rProd.fireAndForget) {
		if err := ch.Confirm(false); err != nil {
			ch.Close()
			return err
		}
		rProd.confirmCh = ch.NotifyPublish(make(chan amqp.Confirmation, 1))
		rProd.returnCh = ch.NotifyReturn(make(chan amqp.Return, 1))
	}

	rProd.channel = ch
	return nil
}

func (rProd *RabbitMqProducer) Publish(ctx context.Context, body []byte, cfg RabbitMqPublisherConfig) error {
	if rProd.channel == nil {
		return errors.New("channel not initialized, call GetChannel first")
	}

	msg := rProd.buildMessage(cfg)
	msg.Body = body

	err := rProd.channel.PublishWithContext(ctx, rProd.exchangeName, rProd.routingKey, rProd.mode == Confirmed, false, msg)
	if err != nil {
		return err
	}

	if rProd.mode == Unsafe && rProd.fireAndForget {
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
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (rProd *RabbitMqProducer) buildMessage(cfg RabbitMqPublisherConfig) amqp.Publishing {
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
