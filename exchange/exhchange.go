package exchange

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/connection"
	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/helpers"
)

type ExchangeTopic int

const (
	Topic ExchangeTopic = iota
	Direct
	Fanout
	Headers
)

func (et ExchangeTopic) String() string {
	switch et {
	case Topic:
		return "topic"
	case Direct:
		return "direct"
	case Fanout:
		return "fanout"
	case Headers:
		return "headers"
	default:
		return "unknown"
	}
}

type RabbitExchangeClass struct {
	ExchangeName    string
	exchangeType    ExchangeTopic
	exchangeOptions helpers.RabbitExchangeOptions
}

func NewRabbitExchange(exchangeName string, exchangeType ExchangeTopic, exchangeOptions helpers.RabbitExchangeOptions) *RabbitExchangeClass {
	return &RabbitExchangeClass{
		ExchangeName:    exchangeName,
		exchangeType:    exchangeType,
		exchangeOptions: exchangeOptions,
	}
}

func (rbEx *RabbitExchangeClass) CreateExchange(rabbit *connection.RabbitMqConnectionClass) error {
	ch, err := rabbit.Connection.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	return ch.ExchangeDeclare(rbEx.ExchangeName, rbEx.exchangeType.String(), rbEx.exchangeOptions.Durable, rbEx.exchangeOptions.AutoDelete, rbEx.exchangeOptions.Internal, rbEx.exchangeOptions.NoWait, nil)
}

func (rbEx *RabbitExchangeClass) CreateQueue(rabbit *connection.RabbitMqConnectionClass, cfg helpers.RabbitQueueConfig) (amqp.Queue, error) {
	ch, err := rabbit.Connection.Channel()
	if err != nil {
		return amqp.Queue{}, err
	}
	defer ch.Close()

	args := cfg.Args
	if args == nil {
		args = amqp.Table{}
	}
	if cfg.QueueType != "" {
		args["x-queue-type"] = string(cfg.QueueType)
	}

	q, err := ch.QueueDeclare(cfg.Name, cfg.Durable, cfg.AutoDelete, cfg.Exclusive, cfg.NoWait, args)
	if err != nil {
		return amqp.Queue{}, err
	}

	if err := ch.QueueBind(q.Name, cfg.BindingKey, rbEx.ExchangeName, cfg.NoWait, nil); err != nil {
		return amqp.Queue{}, err
	}

	return q, nil
}
