package connection

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"sync"
	"github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/helpers"
	"time"
)

type RabbitProduceClass struct{}

type RabbitConsumeClass struct{}

type RabbitMqConnectionClass struct {
	Connection           *amqp.Connection
	rabbitConnString     string
	options              helpers.ConnectionOptions
	reconnectAttempts    int
	isShuttingDown       bool
	onReconnectCallbacks []func() error
	mu                   sync.Mutex
}

func DefaultOptions() helpers.ConnectionOptions {
	return helpers.ConnectionOptions{
		AmqpConfig:           amqp.Config{Heartbeat: 60 * time.Second},
		ReconnectInterval:    5 * time.Second,
		MaxReconnectAttempts: 10,
	}
}

func NewRabbitMqConnectionClass(connString string, opts helpers.ConnectionOptions) *RabbitMqConnectionClass {
	return &RabbitMqConnectionClass{
		rabbitConnString: connString,
		options:          opts,
	}
}

func (rabbit *RabbitMqConnectionClass) Connect() error {
	rabbit.mu.Lock()
	defer rabbit.mu.Unlock()

	conn, err := amqp.DialConfig(rabbit.rabbitConnString, rabbit.options.AmqpConfig)
	if err != nil {
		return err
	}

	rabbit.Connection = conn
	rabbit.reconnectAttempts = 0

	go rabbit.handleDisconnect()

	return nil
}

func (rabbit *RabbitMqConnectionClass) handleDisconnect() {
	closeCh := rabbit.Connection.NotifyClose(make(chan *amqp.Error, 1))

	err := <-closeCh
	if err != nil {
		log.Printf("[RabbitMQ] Connection error: %v", err)
	}

	log.Println("[RabbitMQ] Connection closed")

	rabbit.mu.Lock()
	shuttingDown := rabbit.isShuttingDown
	rabbit.mu.Unlock()

	if !shuttingDown {
		rabbit.reconnect()
	}
}

func (rabbit *RabbitMqConnectionClass) reconnect() {
	for rabbit.reconnectAttempts < rabbit.options.MaxReconnectAttempts {
		rabbit.reconnectAttempts++
		log.Printf("[RabbitMQ] Reconnect attempt %d/%d", rabbit.reconnectAttempts, rabbit.options.MaxReconnectAttempts)

		time.Sleep(rabbit.options.ReconnectInterval)

		if err := rabbit.Connect(); err != nil {
			log.Printf("[RabbitMQ] Reconnect failed: %v", err)
			continue
		}

		log.Println("[RabbitMQ] Reconnected successfully")
		for _, cb := range rabbit.onReconnectCallbacks {
			if err := cb(); err != nil {
				log.Printf("[RabbitMQ] Reconnect callback error: %v", err)
			}
		}
		return
	}

	log.Println("[RabbitMQ] Max reconnect attempts reached")
}

func (rabbit *RabbitMqConnectionClass) Shutdown() error {
	rabbit.mu.Lock()
	rabbit.isShuttingDown = true
	rabbit.mu.Unlock()

	if rabbit.Connection != nil {
		return rabbit.Connection.Close()
	}
	return nil
}

