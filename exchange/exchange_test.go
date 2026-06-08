package exchange

import (
	"testing"
)

func TestExchangeTopicString(t *testing.T) {
	tests := []struct {
		input    ExchangeTopic
		expected string
	}{
		{Topic, "topic"},
		{Direct, "direct"},
		{Fanout, "fanout"},
		{Headers, "headers"},
		{ExchangeTopic(99), "unknown"},
	}

	for _, tt := range tests {
		result := tt.input.String()
		if result != tt.expected {
			t.Errorf("ExchangeTopic(%d).String() = %s, want %s", tt.input, result, tt.expected)
		}
	}
}

func TestNewRabbitExchange(t *testing.T) {
	opts := RabbitExchangeOptions{
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
	}

	ex := NewRabbitExchange("test.exchange", Topic, opts)

	if ex.ExchangeName != "test.exchange" {
		t.Errorf("expected 'test.exchange', got '%s'", ex.ExchangeName)
	}
	if ex.exchangeType != Topic {
		t.Errorf("expected Topic, got %v", ex.exchangeType)
	}
	if !ex.exchangeOptions.Durable {
		t.Error("expected Durable true")
	}
}

func TestNewRabbitExchangeDirectType(t *testing.T) {
	ex := NewRabbitExchange("direct.ex", Direct, RabbitExchangeOptions{Durable: true})

	if ex.exchangeType != Direct {
		t.Errorf("expected Direct, got %v", ex.exchangeType)
	}
	if ex.exchangeType.String() != "direct" {
		t.Errorf("expected 'direct', got '%s'", ex.exchangeType.String())
	}
}

func TestNewRabbitExchangeFanoutType(t *testing.T) {
	ex := NewRabbitExchange("fanout.ex", Fanout, RabbitExchangeOptions{})

	if ex.exchangeType != Fanout {
		t.Errorf("expected Fanout, got %v", ex.exchangeType)
	}
	if ex.exchangeType.String() != "fanout" {
		t.Errorf("expected 'fanout', got '%s'", ex.exchangeType.String())
	}
}

func TestNewRabbitExchangeHeadersType(t *testing.T) {
	ex := NewRabbitExchange("headers.ex", Headers, RabbitExchangeOptions{Internal: true})

	if ex.exchangeType != Headers {
		t.Errorf("expected Headers, got %v", ex.exchangeType)
	}
	if !ex.exchangeOptions.Internal {
		t.Error("expected Internal true")
	}
}
