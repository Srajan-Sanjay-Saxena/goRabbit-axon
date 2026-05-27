package consumer

import (
	"context"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestNewConsumerFields(t *testing.T) {
	cons := NewConsumer("test.queue", 15, func(ctx context.Context, msg amqp.Delivery) error {
		return nil
	})

	if cons.queueName != "test.queue" {
		t.Errorf("expected 'test.queue', got '%s'", cons.queueName)
	}
	if cons.prefetch != 15 {
		t.Errorf("expected prefetch 15, got %d", cons.prefetch)
	}
	if cons.channel != nil {
		t.Error("expected nil channel before GetChannel()")
	}
	if cons.autoAck != false {
		t.Error("expected autoAck false by default")
	}
	if cons.handler == nil {
		t.Error("expected handler to be set")
	}
}

func TestConsumeFailsWithoutChannel(t *testing.T) {
	cons := NewConsumer("test.queue", 10, func(ctx context.Context, msg amqp.Delivery) error {
		return nil
	})

	err := cons.Consume(context.Background())
	if err == nil {
		t.Error("expected error when consuming without channel")
	}
	if err.Error() != "channel not initialized, call GetChannel first" {
		t.Errorf("unexpected error: %s", err.Error())
	}
}

func TestStopWithNilChannel(t *testing.T) {
	cons := NewConsumer("test.queue", 10, func(ctx context.Context, msg amqp.Delivery) error {
		return nil
	})

	err := cons.Stop()
	if err != nil {
		t.Errorf("expected nil error on Stop with nil channel, got %v", err)
	}
}

func TestNewConsumerWithDifferentPrefetch(t *testing.T) {
	tests := []struct {
		prefetch int
	}{
		{1},
		{10},
		{50},
		{100},
	}

	for _, tt := range tests {
		cons := NewConsumer("q", tt.prefetch, func(ctx context.Context, msg amqp.Delivery) error {
			return nil
		})
		if cons.prefetch != tt.prefetch {
			t.Errorf("expected prefetch %d, got %d", tt.prefetch, cons.prefetch)
		}
	}
}

func TestNewConsumerHandlerIsSet(t *testing.T) {
	cons := NewConsumer("q", 10, func(ctx context.Context, msg amqp.Delivery) error {
		return nil
	})

	if cons.handler == nil {
		t.Fatal("handler should not be nil")
	}
}

func TestNewConsumerConsumerTagEmpty(t *testing.T) {
	cons := NewConsumer("q", 10, func(ctx context.Context, msg amqp.Delivery) error {
		return nil
	})

	if cons.consumerTag != "" {
		t.Errorf("expected empty consumer tag, got '%s'", cons.consumerTag)
	}
}
