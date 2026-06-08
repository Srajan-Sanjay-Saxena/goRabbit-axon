# goRabbit-axon

A production-ready RabbitMQ client library for Go with connection pooling, fixed channel pools, circuit breaker reconnection, quorum queue support, publisher confirms, and graceful shutdown — built to be imported as an internal SDK by microservices.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                     ConnectionPool                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │   Conn #1    │  │   Conn #2    │  │   Conn #N    │           │
│  │ (circuit     │  │ (circuit     │  │ (circuit     │           │
│  │  breaker +   │  │  breaker +   │  │  breaker +   │           │
│  │  reconnect)  │  │  reconnect)  │  │  reconnect)  │           │
│  ├──────────────┤  ├──────────────┤  ├──────────────┤           │
│  │ [ch][ch][ch] │  │ [ch][ch][ch] │  │ [ch][ch][ch] │           │
│  │  buffered    │  │  buffered    │  │  buffered    │           │
│  │  chan pool   │  │  chan pool   │  │  chan pool   │           │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘           │
│         └──────────────────┼──────────────────┘                   │
│                            │ round-robin                          │
│                    GetChannel(ctx, onClose)                       │
└────────────────────────────┼─────────────────────────────────────┘
                             │
               ┌─────────────┼─────────────┐
               │             │             │
          ┌────▼────┐  ┌────▼────┐  ┌────▼────┐
          │Producer │  │Consumer │  │Exchange │
          │(confirms│  │(prefetch│  │(declare │
          │+onClose │  │+onClose │  │ nil cb) │
          │→nil ch) │  │→wg.Wait)│  │         │
          └─────────┘  └─────────┘  └─────────┘
```

---

## Packages

| Package | File | Responsibility |
|---------|------|----------------|
| `logger` | `logger.go`, `modes.go` | Structured slog logger (Production/Development) |
| `breaker` | `circuitBreaker.go`, `options.go` | Circuit breaker with backoff, probe, state management |
| `channel` | `channel.go` | Channel lifecycle, NotifyClose watcher, OnChannelClose callback |
| `connection/singleConnection` | `singleConnectionHandler.go`, `options.go` | Single connection + circuit breaker reconnect |
| `connection/connectionPool` | `poolConnectionHandler.go` | Fixed channel pool per connection, round-robin, self-healing |
| `exchange` | `exhchange.go`, `options.go` | Exchange/queue declaration and binding |
| `producer` | `producer.go`, `options.go` | Publishing with confirms, channel modes, onClose auto-nil |
| `consumer` | `consumer.go` | Consuming with prefetch, graceful shutdown, onClose wg.Wait |
| `helpers` | `helper.go` | `IRabbitConnection` interface |

---

## Installation

```bash
go get github.com/Srajan-Sanjay-Saxena/goRabbit-axon
```

---

## Usage

### 1. Single Connection

```go
import (
    "context"
    singleConn "github.com/Srajan-Sanjay-Saxena/goRabbit-axon/connection/singleConnection"
    "github.com/Srajan-Sanjay-Saxena/goRabbit-axon/breaker"
)

ctx := context.Background()

conn := singleConn.NewRabbitMqSingleConnectionHandler(
    "amqp://guest:guest@localhost:5672/",
    singleConn.DefaultOptions(),
    nil, // nil = production logger
)
conn.AddBreaker(breaker.CircuitBreakerOptions{})

if err := conn.Connect(ctx); err != nil {
    log.Fatal(err)
}
defer conn.Shutdown()
```

### 2. Connection Pool (Recommended)

```go
import (
    "context"
    connPool "github.com/Srajan-Sanjay-Saxena/goRabbit-axon/connection/connectionPool"
    singleConn "github.com/Srajan-Sanjay-Saxena/goRabbit-axon/connection/singleConnection"
)

ctx := context.Background()

pool := connPool.NewConnectionPool(
    "amqp://guest:guest@localhost:5672/",
    connPool.PoolOptions{ConnSize: 3, ChanPerConn: 5},
    singleConn.DefaultOptions(),
    nil,
)

if err := pool.Connect(ctx); err != nil {
    log.Fatal(err)
}
defer pool.Shutdown()
```

### 3. Declare Exchange and Queue

```go
import (
    "github.com/Srajan-Sanjay-Saxena/goRabbit-axon/exchange"
)

ex := exchange.NewRabbitExchange("orders.exchange", exchange.Topic, exchange.RabbitExchangeOptions{
    Durable: true,
})
ex.CreateExchange(ctx, pool) // pool or conn — both implement IRabbitConnection

ex.CreateQueue(ctx, pool, exchange.RabbitQueueConfig{
    Name:       "orders.created",
    BindingKey: "order.created.#",
    QueueType:  exchange.QuorumQueue,
    Durable:    true,
})
```

### 4. Publish Messages

```go
import (
    "github.com/Srajan-Sanjay-Saxena/goRabbit-axon/producer"
)

pub := producer.NewProducer("orders.exchange", "order.created.us")
pub.GetChannel(ctx, pool) // acquires channel, registers onClose callback

err := pub.Publish(ctx, []byte(`{"order_id": "123"}`), producer.RabbitMqPublisherConfig{
    Persistent: true,
})

// If channel dies: pub.channel auto-nils via onClose
// Check with: pub.IsChannelValid()
```

### 5. Consume Messages

```go
import (
    "github.com/Srajan-Sanjay-Saxena/goRabbit-axon/consumer"
)

handler := func(ctx context.Context, msg amqp.Delivery) error {
    fmt.Println("Received:", string(msg.Body))
    return nil
}

cons := consumer.NewConsumer("orders.created", 10, handler)
cons.GetChannel(ctx, pool) // acquires channel, registers onClose (wg.Wait + nil)
cons.Consume(ctx)

// On shutdown:
cons.Stop() // Cancel → WaitGroup → Close
```

---

## Full Application Example

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "os/signal"
    "syscall"

    amqp "github.com/rabbitmq/amqp091-go"
    "github.com/Srajan-Sanjay-Saxena/goRabbit-axon/breaker"
    connPool "github.com/Srajan-Sanjay-Saxena/goRabbit-axon/connection/connectionPool"
    singleConn "github.com/Srajan-Sanjay-Saxena/goRabbit-axon/connection/singleConnection"
    "github.com/Srajan-Sanjay-Saxena/goRabbit-axon/consumer"
    "github.com/Srajan-Sanjay-Saxena/goRabbit-axon/exchange"
    "github.com/Srajan-Sanjay-Saxena/goRabbit-axon/producer"
)

func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer cancel()

    // 1. Create pool (3 connections × 5 channels = 15 pre-warmed channels)
    pool := connPool.NewConnectionPool(
        "amqp://guest:guest@localhost:5672/",
        connPool.PoolOptions{ConnSize: 3, ChanPerConn: 5},
        singleConn.DefaultOptions(),
        nil,
    )
    if err := pool.Connect(ctx); err != nil {
        log.Fatal(err)
    }
    defer pool.Shutdown()

    // 2. Declare infrastructure
    ex := exchange.NewRabbitExchange("events", exchange.Topic, exchange.RabbitExchangeOptions{Durable: true})
    ex.CreateExchange(ctx, pool)
    ex.CreateQueue(ctx, pool, exchange.RabbitQueueConfig{
        Name:       "user.signup",
        BindingKey: "user.signup.#",
        QueueType:  exchange.QuorumQueue,
        Durable:    true,
    })

    // 3. Start consumer
    handler := func(ctx context.Context, msg amqp.Delivery) error {
        fmt.Printf("Processing: %s\n", string(msg.Body))
        return nil
    }
    cons := consumer.NewConsumer("user.signup", 10, handler)
    cons.GetChannel(ctx, pool)
    cons.Consume(ctx)

    // 4. Publish
    pub := producer.NewProducer("events", "user.signup.us")
    pub.GetChannel(ctx, pool)
    pub.Publish(ctx, []byte(`{"user_id": "abc123"}`), producer.RabbitMqPublisherConfig{
        Persistent: true,
    })

    // 5. Wait for shutdown
    <-ctx.Done()
    cons.Stop()
}
```

---

## Producer Channel Modes

| Mode | FireAndForget | Confirms | Mandatory | Behavior |
|------|--------------|----------|-----------|----------|
| `Confirmed` | N/A | ON | ON | Full safety. Default. |
| `Unsafe` | `false` | ON | OFF | Confirms but no routing check. |
| `Unsafe` | `true` | OFF | OFF | Zero overhead fire-and-forget. |

```go
// Fire-and-forget (metrics, logs)
pub.GetChannel(ctx, pool, producer.ProducerChannelOptions{
    Mode: producer.Unsafe,
    UnsafeOptions: producer.UnsafeOptions{FireAndForget: true},
})
```

---

## Project Structure

```
.
├── backoff/
│   └── backoff.go
├── breaker/
│   ├── circuitBreaker.go
│   └── options.go
├── channel/
│   └── channel.go
├── connection/
│   ├── connectionPool/
│   │   └── poolConnectionHandler.go
│   └── singleConnection/
│       ├── singleConnectionHandler.go
│       └── options.go
├── consumer/
│   └── consumer.go
├── exchange/
│   ├── exhchange.go
│   └── options.go
├── helpers/
│   └── helper.go
├── logger/
│   ├── logger.go
│   └── modes.go
├── producer/
│   ├── producer.go
│   └── options.go
├── go.mod
├── go.sum
├── main.go
├── README.md
└── INTERNAL.md
```

---

## License

MIT
