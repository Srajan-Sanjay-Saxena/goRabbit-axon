package connection

import (
	"errors"
	"log"
	"sync"
	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/backoff"
	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/helpers"
	"time"
)

type ConnectionPool struct {
	connections []*RabbitMqConnectionClass
	available   chan *RabbitMqConnectionClass
	connString  string
	options     helpers.ConnectionOptions
	size        int
	mu          sync.Mutex
}

func NewConnectionPool(connString string, size int, opts helpers.ConnectionOptions) *ConnectionPool {
	return &ConnectionPool{
		connString: connString,
		options:    opts,
		size:       size,
		available:  make(chan *RabbitMqConnectionClass, size),
	}
}

func (p *ConnectionPool) Init() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i := 0; i < p.size; i++ {
		conn := NewRabbitMqConnectionClass(p.connString, p.options)
		if err := conn.Connect(); err != nil {
			return err
		}
		p.connections = append(p.connections, conn)
		p.available <- conn
	}
	return nil
}

func (p *ConnectionPool) Acquire() (*RabbitMqConnectionClass, error) {
	select {
	case conn := <-p.available:
		if conn.Connection.IsClosed() {
			if err := conn.Connect(); err != nil {
				return nil, err
			}
		}
		return conn, nil
	default:
		return nil, errors.New("no available connections in pool")
	}
}

func (p *ConnectionPool) Release(conn *RabbitMqConnectionClass) {
	if conn.Connection != nil && !conn.Connection.IsClosed() {
		p.available <- conn
		return
	}

	go p.reconnectAndRelease(conn)
}

func (p *ConnectionPool) reconnectAndRelease(conn *RabbitMqConnectionClass) {
	bo := backoff.NewBackoffDelay(1*time.Second, 1*time.Second, 16*time.Second, 30*time.Second)
	bo.StartTimer()

	for i := 0; i < p.options.MaxReconnectAttempts; i++ {
		if err := conn.Connect(); err == nil {
			p.available <- conn
			return
		}
		bo.Wait()
	}

	log.Println("[Pool] Failed to reconnect, replacing with new connection")
	newConn := NewRabbitMqConnectionClass(p.connString, p.options)
	if err := newConn.Connect(); err != nil {
		log.Printf("[Pool] Failed to create replacement connection: %v", err)
		return
	}

	p.mu.Lock()
	for i, c := range p.connections {
		if c == conn {
			p.connections[i] = newConn
			break
		}
	}
	p.mu.Unlock()

	p.available <- newConn
}

func (p *ConnectionPool) Shutdown() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var lastErr error
	for _, conn := range p.connections {
		if err := conn.Shutdown(); err != nil {
			lastErr = err
		}
	}
	close(p.available)
	return lastErr
}
