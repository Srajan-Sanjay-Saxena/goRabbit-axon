package integration_test

import (
	"testing"

	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/connection"
)

func TestPoolInit(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	pool := connection.NewConnectionPool(connStr, 3, connection.DefaultOptions())
	if err := pool.Init(); err != nil {
		t.Fatalf("pool init failed: %v", err)
	}
	defer pool.Shutdown()
}

func TestPoolAcquireAndRelease(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	pool := connection.NewConnectionPool(connStr, 3, connection.DefaultOptions())
	if err := pool.Init(); err != nil {
		t.Fatalf("pool init failed: %v", err)
	}
	defer pool.Shutdown()

	conn, err := pool.Acquire()
	if err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	if conn == nil {
		t.Fatal("expected non-nil connection")
	}
	if conn.Connection.IsClosed() {
		t.Fatal("acquired connection should be open")
	}

	pool.Release(conn)
}

func TestPoolAcquireAll(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	poolSize := 3
	pool := connection.NewConnectionPool(connStr, poolSize, connection.DefaultOptions())
	if err := pool.Init(); err != nil {
		t.Fatalf("pool init failed: %v", err)
	}
	defer pool.Shutdown()

	conns := make([]*connection.RabbitMqConnectionClass, 0, poolSize)
	for i := 0; i < poolSize; i++ {
		conn, err := pool.Acquire()
		if err != nil {
			t.Fatalf("acquire %d failed: %v", i, err)
		}
		conns = append(conns, conn)
	}

	// Pool should be exhausted now
	_, err := pool.Acquire()
	if err == nil {
		t.Fatal("expected error when pool is exhausted")
	}

	// Release all back
	for _, conn := range conns {
		pool.Release(conn)
	}

	// Should be able to acquire again
	conn, err := pool.Acquire()
	if err != nil {
		t.Fatalf("acquire after release failed: %v", err)
	}
	pool.Release(conn)
}

func TestPoolShutdownClosesAll(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	pool := connection.NewConnectionPool(connStr, 3, connection.DefaultOptions())
	if err := pool.Init(); err != nil {
		t.Fatalf("pool init failed: %v", err)
	}

	// Acquire one to have it checked out
	conn, err := pool.Acquire()
	if err != nil {
		t.Fatalf("acquire failed: %v", err)
	}

	// Shutdown should close even checked-out connections
	pool.Shutdown()

	if !conn.Connection.IsClosed() {
		t.Fatal("expected checked-out connection to be closed after pool shutdown")
	}
}

func TestPoolMultipleAcquireReleaseCycles(t *testing.T) {
	connStr, cleanup := startRabbitMQ(t)
	defer cleanup()

	pool := connection.NewConnectionPool(connStr, 2, connection.DefaultOptions())
	if err := pool.Init(); err != nil {
		t.Fatalf("pool init failed: %v", err)
	}
	defer pool.Shutdown()

	for i := 0; i < 10; i++ {
		conn, err := pool.Acquire()
		if err != nil {
			t.Fatalf("cycle %d acquire failed: %v", i, err)
		}

		// Use the connection
		ch, err := conn.Connection.Channel()
		if err != nil {
			t.Fatalf("cycle %d channel failed: %v", i, err)
		}
		ch.Close()

		pool.Release(conn)
	}
}
