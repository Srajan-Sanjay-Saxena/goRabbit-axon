package helpers

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/breaker"
)

type IRabbitConnection interface {
	GetChannel() (*amqp.Channel, error)
	Shutdown() error
	Connect() error
	AddBreaker(opts breaker.CircuitBreakerOptions)
	OnReconnect(cb func() error)
}
