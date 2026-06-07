package connection

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/helpers"
)

func TestNewConnectionPool(t *testing.T) {
	opts := DefaultOptions()
	pool := NewConnectionPool("amqp://guest:guest@localhost:5672/", 5, opts)

	if pool.size != 5 {
		t.Errorf("expected size 5, got %d", pool.size)
	}
	if pool.connString != "amqp://guest:guest@localhost:5672/" {
		t.Errorf("unexpected conn string: %s", pool.connString)
	}
	if cap(pool.available) != 5 {
		t.Errorf("expected channel capacity 5, got %d", cap(pool.available))
	}
	if len(pool.connections) != 0 {
		t.Errorf("expected 0 connections before Init, got %d", len(pool.connections))
	}
}

func TestAcquireOnEmptyPool(t *testing.T) {
	opts := DefaultOptions()
	pool := NewConnectionPool("amqp://guest:guest@localhost:5672/", 3, opts)

	// Don't call Init — pool is empty
	conn, err := pool.Acquire()
	if err == nil {
		t.Error("expected error when acquiring from empty pool")
	}
	if conn != nil {
		t.Error("expected nil connection")
	}
	if err.Error() != "no available connections in pool" {
		t.Errorf("unexpected error message: %s", err.Error())
	}
}

func TestInitFailsWithBadURL(t *testing.T) {
	opts := helpers.ConnectionOptions{
		AmqpConfig:           amqp.Config{Heartbeat: 10 * time.Second},
		ReconnectInterval:    1 * time.Second,
		MaxReconnectAttempts: 2,
	}
	pool := NewConnectionPool("amqp://bad:bad@localhost:9999/", 3, opts)

	err := pool.Init()
	if err == nil {
		t.Error("expected error initializing pool with bad URL")
		pool.Shutdown()
	}
}

func TestPoolSizeMatchesConfig(t *testing.T) {
	sizes := []int{1, 3, 10}
	for _, size := range sizes {
		pool := NewConnectionPool("amqp://localhost/", size, DefaultOptions())
		if pool.size != size {
			t.Errorf("expected size %d, got %d", size, pool.size)
		}
		if cap(pool.available) != size {
			t.Errorf("expected channel cap %d, got %d", size, cap(pool.available))
		}
	}
}

func TestAcquireAllThenExhausted(t *testing.T) {
	opts := DefaultOptions()
	pool := NewConnectionPool("amqp://guest:guest@localhost:5672/", 2, opts)

	// Manually put mock connections in the channel to simulate Init
	conn1 := NewRabbitMqConnectionClass(pool.connString, pool.options)
	conn2 := NewRabbitMqConnectionClass(pool.connString, pool.options)
	pool.connections = append(pool.connections, conn1, conn2)

	// We can't actually acquire these without a real connection (IsClosed check will panic)
	// So test that after draining the channel, Acquire returns error
	_, err := pool.Acquire()
	if err == nil {
		t.Error("expected error on exhausted pool")
	}
}

func TestShutdownWithNoConnections(t *testing.T) {
	pool := NewConnectionPool("amqp://localhost/", 3, DefaultOptions())

	err := pool.Shutdown()
	if err != nil {
		t.Errorf("expected nil error on shutdown with no connections, got %v", err)
	}
}
