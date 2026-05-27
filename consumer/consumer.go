package consumer

import (
	"context"
	"errors"
	"log"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/connection"
)

type MessageHandler func(ctx context.Context, msg amqp.Delivery) error

type RabbitMqConsumer struct {
	queueName   string
	prefetch    int
	autoAck     bool
	channel     *amqp.Channel
	handler     MessageHandler
	consumerTag string
	wg          sync.WaitGroup
}

func NewConsumer(queueName string, prefetch int, handler MessageHandler) *RabbitMqConsumer {
	return &RabbitMqConsumer{
		queueName: queueName,
		prefetch:  prefetch,
		handler:   handler,
	}
}

func (c *RabbitMqConsumer) GetChannel(rabbit *connection.RabbitMqConnectionClass) error {
	ch, err := rabbit.Connection.Channel()
	if err != nil {
		return err
	}

	if err := ch.Qos(c.prefetch, 0, false); err != nil {
		ch.Close()
		return err
	}

	c.channel = ch
	return nil
}

func (c *RabbitMqConsumer) Consume(ctx context.Context) error {
	if c.channel == nil {
		return errors.New("channel not initialized, call GetChannel first")
	}

	msgs, err := c.channel.Consume(c.queueName, c.consumerTag, c.autoAck, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case msg, ok := <-msgs:
				if !ok {
					log.Println("[RabbitMQ] Consumer channel closed")
					return
				}
				c.wg.Add(1)
				go func(m amqp.Delivery) {
					defer c.wg.Done()
					if err := c.handler(ctx, m); err != nil {
						log.Printf("[RabbitMQ] Handler error: %v", err)
						m.Nack(false, true)
					} else {
						m.Ack(false)
					}
				}(msg)
			case <-ctx.Done():
				log.Println("[RabbitMQ] Consumer stopping, waiting for in-flight messages...")
				return
			}
		}
	}()

	return nil
}

func (c *RabbitMqConsumer) Stop() error {
	if c.channel == nil {
		return nil
	}
	// Stop receiving new messages
	if err := c.channel.Cancel(c.consumerTag, false); err != nil {
		return err
	}
	// Wait for all in-flight messages to finish processing
	c.wg.Wait()
	return c.channel.Close()
}
