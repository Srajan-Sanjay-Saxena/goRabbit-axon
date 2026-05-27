package producer

import (
	"context"
	"errors"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/connection"
	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/helpers"
)

type RabbitMqProducer struct {
	exchangeName string
	routingKey   string
	channel      *amqp.Channel
}

func NewProducer(exchangeName, routingKey string) *RabbitMqProducer {
	return &RabbitMqProducer{
		exchangeName: exchangeName,
		routingKey:   routingKey,
	}
}

func (rProd *RabbitMqProducer) GetChannel(rabbit *connection.RabbitMqConnectionClass) error {
	ch, err := rabbit.Connection.Channel()
	if err != nil {
		return err
	}

	if err := ch.Confirm(false); err != nil {
		ch.Close()
		return err
	}

	rProd.channel = ch
	return nil
}

func (rProd *RabbitMqProducer) Publish(ctx context.Context, body []byte, rabbit *connection.RabbitMqConnectionClass, cfg helpers.RabbitMqPublisherConfig) error {
	if rProd.channel == nil {
		return errors.New("channel not initialized, call GetChannel first")
	}

	confirmCh := rProd.channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	returnCh := rProd.channel.NotifyReturn(make(chan amqp.Return, 1))
	blockedCh := rabbit.Connection.NotifyBlocked(make(chan amqp.Blocking, 1))

	msg := rProd.BuildConfig(cfg)
	msg.Body = body

	err := rProd.channel.PublishWithContext(ctx, rProd.exchangeName, rProd.routingKey, true, false, msg)
	if err != nil {
		return err
	}

	select {
	case confirm := <-confirmCh:
		if !confirm.Ack {
			return errors.New("broker nacked the message")
		}
		return nil
	case ret := <-returnCh:
		return errors.New("message returned: " + ret.ReplyText)
	case <-blockedCh:
		// Connection blocked by broker (resource alarm).
		// Waits here until broker sends unblocked or confirm arrives.
		// For production, consider a backpressure queue (10,000 buffer size)
		// to handle short broker hiccups without blocking callers.
		<-confirmCh
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
