package connPool

import (
	"context"
	"errors"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/breaker"
	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/channel"
	singleConn "github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/connection/singleConnection"
	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/logger"
)

type PoolOptions struct {
	ConnSize    int
	ChanPerConn int
}

type RabbitMqConnectionPoolHandler struct {
	connections    []*singleConn.RabbitMqSingleConnectionHandler
	chanPool       map[*singleConn.RabbitMqSingleConnectionHandler]chan *amqp.Channel
	channelHandler *channel.ChannelHandler
	connString     string
	options        singleConn.ConnectionOptions
	poolOpts       PoolOptions
	connIdx        int
	log            *logger.Logger
	mu             sync.Mutex
}

func NewConnectionPool(connString string, poolOpts PoolOptions, connOpts singleConn.ConnectionOptions, log *logger.Logger) *RabbitMqConnectionPoolHandler {
	if log == nil {
		log = logger.New(logger.Production)
	}
	if poolOpts.ConnSize == 0 {
		poolOpts.ConnSize = 3
	}
	if poolOpts.ChanPerConn == 0 {
		poolOpts.ChanPerConn = 5
	}

	p := &RabbitMqConnectionPoolHandler{
		connString: connString,
		options:    connOpts,
		poolOpts:   poolOpts,
		log:        log,
		chanPool:   make(map[*singleConn.RabbitMqSingleConnectionHandler]chan *amqp.Channel),
	}

	p.channelHandler = channel.NewChannelHandler(log, p.replaceDeadChannel)

	return p
}

func (p *RabbitMqConnectionPoolHandler) Connect(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i := 0; i < p.poolOpts.ConnSize; i++ {
		conn := singleConn.NewRabbitMqSingleConnectionHandler(p.connString, p.options, p.log, p.replaceDeadChannel)
		conn.AddBreaker(breaker.CircuitBreakerOptions{})
		if err := conn.Connect(ctx); err != nil {
			return err
		}
		p.connections = append(p.connections, conn)

		// Pre-warm channel buffer
		buf := make(chan *amqp.Channel, p.poolOpts.ChanPerConn)
		for j := 0; j < p.poolOpts.ChanPerConn; j++ {
			ch, err := p.channelHandler.GetChannel(ctx, conn.Connection)
			if err != nil {
				return err
			}
			buf <- ch
		}
		p.chanPool[conn] = buf
	}

	p.log.Info("connection pool initialized", "connections", p.poolOpts.ConnSize, "channelsPerConn", p.poolOpts.ChanPerConn)
	return nil
}

func (p *RabbitMqConnectionPoolHandler) GetChannel() (*amqp.Channel, error) {
	p.mu.Lock()
	startIdx := p.connIdx
	p.connIdx = (p.connIdx + 1) % len(p.connections)
	p.mu.Unlock()

	if len(p.connections) == 0 {
		return nil, errors.New("pool not initialized")
	}

	for i := 0; i < len(p.connections); i++ {
		idx := (startIdx + i) % len(p.connections)
		conn := p.connections[idx]

		if conn.Connection == nil || conn.Connection.IsClosed() {
			continue
		}

		buf := p.chanPool[conn]
		select {
		case ch := <-buf:
			return ch, nil
		default:
			continue
		}
	}

	return nil, errors.New("all channels acquired — pool exhausted")
}

func (p *RabbitMqConnectionPoolHandler) ReleaseChannel(ch *amqp.Channel) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Find which connection owns this channel and put it back
	for _, conn := range p.connections {
		buf := p.chanPool[conn]
		// Non-blocking push — if buffer is full, channel is orphaned (shouldn't happen)
		select {
		case buf <- ch:
			return
		default:
			continue
		}
	}

	// If we get here, no buffer accepted it — close the orphan
	p.log.Warn("orphaned channel returned, closing")
	ch.Close()
}

func (p *RabbitMqConnectionPoolHandler) replaceDeadChannel(deadCh *amqp.Channel, conn *amqp.Connection) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Find the connection handler that owns this amqp.Connection
	for connHandler, buf := range p.chanPool {
		if connHandler.Connection != conn {
			continue
		}

		if conn.IsClosed() {
			p.log.Warn("connection closed, skipping channel replacement")
			return
		}

		newCh, err := conn.Channel()
		if err != nil {
			p.log.Error("failed to replace dead channel", "error", err)
			return
		}

		// Watch the new channel
		go p.channelHandler.HandleChannelClose(context.Background(), newCh, conn)

		// Put replacement into buffer (non-blocking)
		select {
		case buf <- newCh:
			p.log.Info("replaced dead channel")
		default:
			// Buffer full — shouldn't happen since one died, but close to be safe
			newCh.Close()
		}
		return
	}
}

func (p *RabbitMqConnectionPoolHandler) Shutdown() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Drain and close all channels
	for _, buf := range p.chanPool {
		close(buf)
		for ch := range buf {
			if ch != nil {
				ch.Close()
			}
		}
	}

	// Close all connections
	var lastErr error
	for _, conn := range p.connections {
		if err := conn.Shutdown(); err != nil {
			lastErr = err
		}
	}

	p.log.Info("connection pool shut down")
	return lastErr
}
