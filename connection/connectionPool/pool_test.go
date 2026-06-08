package connPool

import (
	"context"
	"testing"

	singleConn "github.com/Srajan-Sanjay-Saxena/goRabbit-axon/connection/singleConnection"
)

func TestNewConnectionPool(t *testing.T) {
	pool := NewConnectionPool("amqp://guest:guest@localhost:5672/", PoolOptions{ConnSize: 5, ChanPerConn: 3}, singleConn.DefaultOptions(), nil)

	if pool.poolOpts.ConnSize != 5 {
		t.Errorf("expected ConnSize 5, got %d", pool.poolOpts.ConnSize)
	}
	if pool.poolOpts.ChanPerConn != 3 {
		t.Errorf("expected ChanPerConn 3, got %d", pool.poolOpts.ChanPerConn)
	}
	if pool.connString != "amqp://guest:guest@localhost:5672/" {
		t.Errorf("unexpected conn string: %s", pool.connString)
	}
	if len(pool.connections) != 0 {
		t.Errorf("expected 0 connections before Connect, got %d", len(pool.connections))
	}
	if pool.log == nil {
		t.Error("expected logger to be initialized")
	}
}

func TestNewConnectionPoolDefaults(t *testing.T) {
	pool := NewConnectionPool("amqp://localhost/", PoolOptions{}, singleConn.DefaultOptions(), nil)

	if pool.poolOpts.ConnSize != 3 {
		t.Errorf("expected default ConnSize 3, got %d", pool.poolOpts.ConnSize)
	}
	if pool.poolOpts.ChanPerConn != 5 {
		t.Errorf("expected default ChanPerConn 5, got %d", pool.poolOpts.ChanPerConn)
	}
}

func TestGetChannelOnEmptyPool(t *testing.T) {
	pool := NewConnectionPool("amqp://localhost/", PoolOptions{ConnSize: 3, ChanPerConn: 5}, singleConn.DefaultOptions(), nil)

	_, err := pool.GetChannel(context.Background(), nil)
	if err == nil {
		t.Error("expected error when getting channel from empty pool")
	}
	if err.Error() != "pool not initialized" {
		t.Errorf("unexpected error message: %s", err.Error())
	}
}

func TestConnectFailsWithBadURL(t *testing.T) {
	pool := NewConnectionPool("amqp://bad:bad@localhost:9999/", PoolOptions{ConnSize: 2, ChanPerConn: 2}, singleConn.DefaultOptions(), nil)

	err := pool.Connect(context.Background())
	if err == nil {
		t.Error("expected error initializing pool with bad URL")
		pool.Shutdown()
	}
}

func TestShutdownWithNoConnections(t *testing.T) {
	pool := NewConnectionPool("amqp://localhost/", PoolOptions{ConnSize: 3, ChanPerConn: 5}, singleConn.DefaultOptions(), nil)

	err := pool.Shutdown()
	if err != nil {
		t.Errorf("expected nil error on shutdown with no connections, got %v", err)
	}
}
