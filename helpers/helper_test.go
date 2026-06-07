package helpers

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Verify IRabbitConnection interface compiles correctly
func TestIRabbitConnectionInterface(t *testing.T) {
	var _ IRabbitConnection = (*mockConn)(nil)
}

type mockConn struct{}

func (m *mockConn) GetChannel() (*amqp.Channel, error) { return nil, nil }
func (m *mockConn) Shutdown() error                    { return nil }
