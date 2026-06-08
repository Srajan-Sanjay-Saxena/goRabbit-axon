package helpers

import (
	"context"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/channel"
)

func TestIRabbitConnectionInterface(t *testing.T) {
	var _ IRabbitConnection = (*mockConn)(nil)
}

type mockConn struct{}

func (m *mockConn) GetChannel(ctx context.Context, onClose channel.OnChannelClose) (*amqp.Channel, error) {
	return nil, nil
}
func (m *mockConn) Shutdown() error { return nil }
