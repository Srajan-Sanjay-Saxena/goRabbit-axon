# RabbitMqWrapper-Service-Go

A production-ready RabbitMQ client library for Go that provides connection pooling, exponential backoff reconnection, quorum queue support, publisher confirms, and graceful shutdown — built to be imported as an internal SDK by microservices.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                  ConnectionPool                       │
│  ┌───────────┐ ┌───────────┐ ┌───────────┐         │
│  │  Conn #1  │ │  Conn #2  │ │  Conn #N  │         │
│  │ (auto-    │ │ (auto-    │ │ (auto-    │         │
│  │ reconnect)│ │ reconnect)│ │ reconnect)│         │
│  └─────┬─────┘ └─────┬─────┘ └─────┬─────┘         │
│        │              │              │               │
│        └──────────────┼──────────────┘               │
│                       │                              │
│              available channel (buffered)             │
└───────────────────────┬─────────────────────────────┘
                        │
          ┌─────────────┼─────────────┐
          │             │             │
     ┌────▼────┐  ┌────▼────┐  ┌────▼────┐
     │Producer │  │Consumer │  │Exchange │
     │(confirms│  │(prefetch│  │(declare │
     │ + back- │  │ + grace-│  │ + queue │
     │pressure)│  │ful stop)│  │ + bind) │
     └─────────┘  └─────────┘  └─────────┘
```

---

## Packages

| Package | File | Responsibility |
|---------|------|----------------|
| `helpers` | `helper.go` | Shared config structs and types |
| `backoff` | `backoff.go` | Exponential backoff with stable window reset |
| `connection` | `connection.go` | Single connection lifecycle + auto-reconnect |
| `connection` | `pool.go` | Connection pooling with async health recovery |
| `exchange` | `exhchange.go` | Exchange/queue declaration and binding |
| `producer` | `producer.go` | Publishing with confirms and backpressure |
| `consumer` | `consumer.go` | Consuming with prefetch, graceful shutdown |

---

## Installation

```bash
go get github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go
```

---

## Usage

### 1. Create a Connection Pool

```go
import (
    "github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/connection"
)

opts := connection.DefaultOptions()
pool := connection.NewConnectionPool("amqp://guest:guest@localhost:5672/", 5, opts)

if err := pool.Init(); err != nil {
    log.Fatal(err)
}
defer pool.Shutdown()
```

### 2. Declare an Exchange and Queue

```go
import (
    "github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/exchange"
    "github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/helpers"
)

conn, _ := pool.Acquire()
defer pool.Release(conn)

ex := exchange.NewRabbitExchange("orders.exchange", exchange.Topic, helpers.RabbitExchangeOptions{
    Durable: true,
})
ex.CreateExchange(conn)

ex.CreateQueue(conn, helpers.RabbitQueueConfig{
    Name:       "orders.created",
    BindingKey: "order.created.#",
    QueueType:  helpers.QuorumQueue,
    Durable:    true,
})
```

### 3. Publish a Message

```go
import (
    "github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/producer"
    "github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/helpers"
)

conn, _ := pool.Acquire()
defer pool.Release(conn)

pub := producer.NewProducer("orders.exchange", "order.created.us")
pub.GetChannel(conn)

err := pub.Publish(ctx, []byte(`{"order_id": "123"}`), conn, helpers.RabbitMqPublisherConfig{
    Persistent: true,
})
```

### 4. Consume Messages

```go
import (
    "github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/consumer"
)

conn, _ := pool.Acquire()

handler := func(ctx context.Context, msg amqp.Delivery) error {
    fmt.Println("Received:", string(msg.Body))
    return nil
}

cons := consumer.NewConsumer("orders.created", 10, handler)
cons.GetChannel(conn)
cons.Consume(ctx)

// On shutdown:
cons.Stop()
pool.Release(conn)
```

---

## Deep Dive: Each Component

---

### `helpers` — Config Structs

All shared types live here to avoid circular imports.

```go
type ConnectionOptions struct {
    AmqpConfig           amqp.Config
    ReconnectInterval    time.Duration
    MaxReconnectAttempts int
}
```

- `AmqpConfig` — passed directly to `amqp.DialConfig`. Controls heartbeat interval, SASL, locale, etc.
- `ReconnectInterval` — base wait time between reconnect attempts.
- `MaxReconnectAttempts` — hard cap on retries before giving up.

```go
type QueueType string

const (
    ClassicQueue QueueType = "classic"
    QuorumQueue  QueueType = "quorum"
)

type RabbitQueueConfig struct {
    Name       string
    BindingKey string
    QueueType  QueueType
    Durable    bool
    AutoDelete bool
    Exclusive  bool
    NoWait     bool
    Args       amqp.Table
}
```

- `QueueType` — when set to `QuorumQueue`, the library injects `"x-queue-type": "quorum"` into the args table automatically.
- Quorum queues **must** be durable, **cannot** be exclusive or auto-delete (RabbitMQ enforces this).

```go
type RabbitMqPublisherConfig struct {
    Persistent    bool
    Priority      uint8
    Expiration    string
    ContentType   *string
    Headers       amqp.Table
    FireAndForget bool
}
```

- `Persistent` — sets `DeliveryMode` to `amqp.Persistent` (survives broker restart).
- `Priority` — 0-9, used with priority queues.
- `Expiration` — per-message TTL as a string (e.g., `"60000"` for 60s).
- `ContentType` — defaults to `"application/json"` if nil.
- `FireAndForget` — when `true`, publishes the message and returns immediately without waiting for broker confirm. No delivery guarantee. Use for metrics, logs, non-critical events. Never use for CDC or saga.

---

### `backoff` — Exponential Backoff

```go
type BackoffDelay struct {
    delay        time.Duration  // current delay
    baseDelay    time.Duration  // minimum delay (reset target)
    maxDelay     time.Duration  // cap before reset
    stableWindow time.Duration  // if stable this long, reset delay
    timerStart   time.Time      // when stability timer started
}
```

**How it works:**

1. Call `StartTimer()` when a connection is established.
2. On failure, call `Wait()` — it sleeps for `delay`, then doubles it.
3. If `delay` exceeds `maxDelay`, it resets to `baseDelay`.
4. If the connection has been stable longer than `stableWindow` since `StartTimer()`, it resets to `baseDelay` (the system recovered, no need for aggressive backoff).

**Why stable window matters:**
Without it, after a long period of stability followed by a brief blip, you'd start at whatever the last escalated delay was. The stable window ensures you always start fresh after sustained health.

```
Attempt 1: sleep 1s
Attempt 2: sleep 2s
Attempt 3: sleep 4s
Attempt 4: sleep 8s
Attempt 5: sleep 16s (hits maxDelay → resets to 1s)
Attempt 6: sleep 1s
...
```

---

### `connection` — Single Connection

```go
type RabbitMqConnectionClass struct {
    Connection           *amqp.Connection
    rabbitConnString     string
    options              helpers.ConnectionOptions
    reconnectAttempts    int
    isShuttingDown       bool
    onReconnectCallbacks []func() error
    mu                   sync.Mutex
}
```

**Lifecycle:**

1. `Connect()` — dials RabbitMQ with the provided config, stores the connection, spawns a disconnect watcher goroutine.
2. `handleDisconnect()` — listens on `NotifyClose`. When the connection drops, it logs and triggers `reconnect()` (unless shutting down).
3. `reconnect()` — retries `Connect()` up to `MaxReconnectAttempts` with `ReconnectInterval` sleep between attempts. On success, fires all registered `onReconnectCallbacks`.
4. `Shutdown()` — sets `isShuttingDown = true` (prevents reconnect loop from firing) and closes the connection.

**Why `isShuttingDown` flag?**
When you intentionally close a connection, `NotifyClose` still fires. Without this flag, the library would try to reconnect a connection you deliberately closed.

**Thread safety:**
`mu` protects `Connect()` and `isShuttingDown`. The reconnect loop runs in a single goroutine so it doesn't race with itself.

---

### `connection/pool` — Connection Pool

```go
type ConnectionPool struct {
    connections []*RabbitMqConnectionClass  // ownership registry
    available   chan *RabbitMqConnectionClass // idle connections
    connString  string
    options     helpers.ConnectionOptions
    size        int
    mu          sync.Mutex
}
```

**Two storage locations — why?**

- `connections` slice — knows ALL connections that exist. Used by `Shutdown()` to close every connection, including ones currently checked out by callers.
- `available` channel — buffered channel acting as a lock-free queue of idle connections. `Acquire()` takes from it, `Release()` puts back.

```
connections = [A, B, C, D, E]     // all 5 exist
available   = [C, D, E]           // 3 idle; A and B are in use
```

**Acquire:**
```go
func (p *ConnectionPool) Acquire() (*RabbitMqConnectionClass, error)
```
- Non-blocking select on the channel.
- If a connection is available, checks `IsClosed()` as a safety net and reconnects if needed.
- If pool is exhausted, returns error immediately (no blocking).

**Release:**
```go
func (p *ConnectionPool) Release(conn *RabbitMqConnectionClass)
```
- If connection is healthy → put it back immediately.
- If connection is dead → fire a **goroutine** (`reconnectAndRelease`) so the caller isn't blocked.

**Why async reconnect on Release?**
If reconnect takes 5-30 seconds with backoff retries, you don't want the caller waiting. The goroutine handles it in the background and puts the connection back when ready.

**`reconnectAndRelease` flow:**
1. Try to reconnect with exponential backoff up to `MaxReconnectAttempts`.
2. If reconnect succeeds → put connection back in `available`.
3. If all retries fail → create a brand new connection, swap it in the `connections` slice, put the new one in `available`.

This makes the pool **self-healing** — dead connections are automatically replaced without any caller intervention.

**Shutdown:**
Iterates ALL connections (including checked-out ones) and closes them. Closes the channel.

---

### `exchange` — Exchange & Queue Declaration

```go
type RabbitExchangeClass struct {
    ExchangeName    string
    exchangeType    ExchangeTopic  // Topic | Direct | Fanout | Headers
    exchangeOptions helpers.RabbitExchangeOptions
}
```

**Exchange Types:**

| Type | Routing Behavior |
|------|-----------------|
| `Direct` | Exact match on routing key |
| `Topic` | Pattern match with `*` (one word) and `#` (zero or more words) |
| `Fanout` | Broadcasts to all bound queues, ignores routing key |
| `Headers` | Routes based on message headers instead of routing key |

**`CreateExchange`** — opens a temporary channel, declares the exchange, closes the channel. Uses a short-lived channel because exchange declaration is a one-time setup operation.

**`CreateQueue`** — declares a queue and binds it to the exchange:
1. If `QueueType` is set (e.g., `QuorumQueue`), injects `"x-queue-type"` into the args table.
2. Declares the queue with `QueueDeclare`.
3. Binds it to the exchange with the specified `BindingKey`.

**Quorum Queues:**
- Replicated across multiple RabbitMQ nodes using Raft consensus.
- Survive node failures without message loss.
- Must be durable, cannot be exclusive or auto-delete.
- Replication is handled server-side — the client just sets `x-queue-type: quorum` and connects to any cluster node.

---

### `producer` — Publishing with Confirms

```go
type RabbitMqProducer struct {
    exchangeName string
    routingKey   string
    channel      *amqp.Channel
}
```

**Channel setup:**
- Opens a channel from the connection.
- Enables **confirm mode** (`ch.Confirm(false)`) — the broker will ack/nack every published message.
- Registers three notification channels **once** (not per-publish, see bug fix section below):

```go
rProd.confirmCh = ch.NotifyPublish(make(chan amqp.Confirmation, 1))
rProd.returnCh = ch.NotifyReturn(make(chan amqp.Return, 1))
rProd.blockedCh = rabbit.Connection.NotifyBlocked(make(chan amqp.Blocking, 1))
```

**The Three Notification Channels — What They Are and Why:**

| Channel | Scope | Fires When | Why You Need It |
|---------|-------|------------|----------------|
| `NotifyPublish` | Per AMQP channel | Broker confirms (ack/nack) a published message | Without it, you only know the message hit the TCP buffer — not that the broker persisted it |
| `NotifyReturn` | Per AMQP channel | Message couldn't be routed to any queue | Without it, unroutable messages silently disappear |
| `NotifyBlocked` | Per connection | Broker hits memory/disk resource alarm | Tells you the broker is overwhelmed — all publishing on this connection is paused |

**Why `NotifyPublish` is on the channel, not the connection:**
Each AMQP channel has its own independent confirm sequence (delivery tags 1, 2, 3...). Confirms are scoped to the channel that published the message.

**Why `NotifyReturn` is on the channel, not the connection:**
Returns are tied to the specific channel that published the unroutable message.

**Why `NotifyBlocked` is on the connection, not the channel:**
When RabbitMQ hits a resource alarm (memory > threshold, disk full), it blocks **all** publishing across the entire TCP connection — not just one channel. It's a connection-wide event.

**Buffer size 1 — why?**
```go
make(chan amqp.Confirmation, 1)  // buffer size 1
```
The broker's confirm might arrive slightly before your `select` is ready to read. With buffer 0 (unbuffered), the library's internal dispatch goroutine would block until your code reaches `<-confirmCh`. With buffer 1, the confirm can land in the channel and wait — no blocking on the library side.

**Publish flow:**

1. Builds the message from config (delivery mode, headers, TTL, etc.).

2. Publishes with `mandatory: true` — ensures the message reaches at least one queue, otherwise it's returned.

3. Waits on a select:
   - **Confirm received** → if acked, success. If nacked, return error.
   - **Return received** → message was unroutable, return error with reason (e.g., `"NO_ROUTE"`).
   - **Blocked** → broker is under pressure. Waits for the confirm to eventually arrive (backpressure handling).
   - **Context cancelled** → caller timed out or cancelled.

**Why publisher confirms matter:**
Without confirms, `Publish` returns success as soon as the message hits the TCP buffer. The broker might reject it, lose it, or fail before persisting. Confirms give you a guarantee that the broker accepted responsibility for the message.

**Why `mandatory: true`?**
If you publish to a routing key that no queue is bound to, the message silently disappears. With mandatory, the broker returns it so you know delivery failed.

---

### `consumer` — Consuming with Graceful Shutdown

```go
type RabbitMqConsumer struct {
    queueName   string
    prefetch    int
    autoAck     bool
    channel     *amqp.Channel
    handler     MessageHandler
    consumerTag string
    wg          sync.WaitGroup
}
```

**Prefetch (`Qos`):**
Controls how many unacknowledged messages the broker sends to this consumer. Prevents a fast producer from overwhelming a slow consumer.
- `prefetch: 1` — one at a time (safe but slow).
- `prefetch: 10-50` — good balance for most workloads.
- `prefetch: 0` — unlimited (dangerous, can OOM).

**Consume flow:**
1. Starts a goroutine that reads from the delivery channel.
2. For each message, spawns a goroutine to handle it (concurrent processing).
3. Uses `sync.WaitGroup` to track in-flight messages.
4. On success → `Ack`. On handler error → `Nack` with requeue.

**Graceful shutdown (`Stop`):**
1. `Cancel` — tells the broker to stop sending new messages.
2. `wg.Wait()` — waits for all in-flight messages to finish processing.
3. `channel.Close()` — closes the channel cleanly.

This ensures no messages are lost or left half-processed during shutdown.

**Context cancellation:**
The consume loop also listens on `ctx.Done()`. When the parent context is cancelled (e.g., SIGTERM handler), the loop exits. Combined with `Stop()`, this gives you a clean two-phase shutdown:
1. Context cancels → loop stops picking up new messages.
2. `Stop()` → waits for in-flight, then closes.

---

## Bug Fix: NotifyPublish Listener Accumulation (Producer Deadlock)

### The Problem

The original `Publish()` method registered new notification channels on **every single publish call**:

```go
// ❌ BROKEN: Old code in Publish()
func (rProd *RabbitMqProducer) Publish(...) error {
    confirmCh := rProd.channel.NotifyPublish(make(chan amqp.Confirmation, 1))  // NEW listener every time
    returnCh := rProd.channel.NotifyReturn(make(chan amqp.Return, 1))          // NEW listener every time
    blockedCh := rabbit.Connection.NotifyBlocked(make(chan amqp.Blocking, 1))  // NEW listener every time
    // ... publish and wait on confirmCh ...
}
```

This caused a **deadlock on the 3rd publish** when reusing the same producer instance.

### How `NotifyPublish` Works Internally

The `amqp091-go` library maintains an internal list of listener channels:

```go
// Inside the amqp library (simplified)
type confirms struct {
    listeners []chan amqp.Confirmation  // append-only, never removes
}

func (c *confirms) One(confirm Confirmation) {
    for _, listener := range c.listeners {
        listener <- confirm  // BLOCKS if channel is full, sends to ALL
    }
}
```

Key behaviors:
- `NotifyPublish(ch)` **appends** `ch` to the listeners list. It never replaces.
- There is **no `UnnotifyPublish`** method. Once registered, a listener stays forever.
- When a confirm arrives, the library sends to **every** listener **sequentially**.
- If any listener channel is full and nobody reads from it, the dispatch **blocks** — it cannot skip.

### Step-by-Step Deadlock Trace

```
═══ PUBLISH #1 ═══
  NotifyPublish(confirmCh_1)       → listeners = [confirmCh_1]
  Publish message #1
  Broker sends confirm #1
  Library dispatch:
    confirmCh_1 <- confirm         ✓ buffer empty, fits
  Your select: <-confirmCh_1       ✓ reads it, buffer now empty
  Return success

═══ PUBLISH #2 ═══
  NotifyPublish(confirmCh_2)       → listeners = [confirmCh_1, confirmCh_2]
  Publish message #2
  Broker sends confirm #2
  Library dispatch:
    confirmCh_1 <- confirm         ✓ buffer was empty (you read from it in publish #1), fits
    confirmCh_2 <- confirm         ✓ buffer empty, fits
  Your select: <-confirmCh_2       ✓ reads it
  Return success
  ⚠️  But confirmCh_1 now has confirm #2 sitting in it. Nobody will ever read it.

═══ PUBLISH #3 ═══
  NotifyPublish(confirmCh_3)       → listeners = [confirmCh_1, confirmCh_2, confirmCh_3]
  Publish message #3
  Broker sends confirm #3
  Library dispatch:
    confirmCh_1 <- confirm         ✗ BLOCKS! Buffer size 1, already has confirm #2 in it.
                                      Nobody is reading confirmCh_1.
    confirmCh_2 <- confirm         ← never reached
    confirmCh_3 <- confirm         ← never reached
  Your select: <-confirmCh_3       ← waiting forever, never receives
  Context deadline exceeded. DEADLOCK.
```

### Why It Fails on the 3rd Publish (Not the 2nd)

After publish #1, you read from `confirmCh_1`, emptying its buffer. So when publish #2's confirm arrives, `confirmCh_1` has space — it accepts the confirm. But **nobody reads that confirm from confirmCh_1** after publish #2 (you're reading from `confirmCh_2` now).

So after publish #2:
- `confirmCh_1` = full (has unread confirm #2)
- `confirmCh_2` = empty (you read from it)

On publish #3, the library tries to send to `confirmCh_1` first → blocked → deadlock.

### The Fix

Register notification channels **once** in `GetChannel()`, reuse them across all publishes:

```go
// ✅ FIXED: Register once
func (rProd *RabbitMqProducer) GetChannel(rabbit *connection.RabbitMqConnectionClass) error {
    ch, _ := rabbit.Connection.Channel()
    ch.Confirm(false)
    rProd.channel = ch
    rProd.confirmCh = ch.NotifyPublish(make(chan amqp.Confirmation, 1))   // once
    rProd.returnCh = ch.NotifyReturn(make(chan amqp.Return, 1))           // once
    rProd.blockedCh = rabbit.Connection.NotifyBlocked(make(chan amqp.Blocking, 1)) // once
    return nil
}

func (rProd *RabbitMqProducer) Publish(...) error {
    // Just publish and read from the existing channels
    rProd.channel.PublishWithContext(...)
    select {
    case confirm := <-rProd.confirmCh:  // same channel every time
        ...
    }
}
```

Now there's exactly **one** listener in the list. Every confirm goes to that one channel. You read from it on every publish. No accumulation, no deadlock.

### The Stack Trace Explained

```
goroutine 465 [chan send]:
github.com/rabbitmq/amqp091-go.(*confirms).confirm(...)
    confirms.go:65
github.com/rabbitmq/amqp091-go.(*confirms).One(...)
    confirms.go:91
```

This is the library's internal goroutine stuck on `listener <- confirm` — trying to send to an abandoned channel that nobody reads from. It blocks the entire confirm dispatch pipeline, starving all subsequent listeners.

---

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| Buffered channel for pool | Lock-free acquire/release in the happy path. No mutex contention under normal load. |
| Async reconnect on Release | Callers shouldn't block on infrastructure recovery. Fire-and-forget with self-healing. |
| Separate connection slice + channel | Slice for ownership tracking (shutdown all). Channel for availability signaling. |
| Short-lived channels for declaration | Exchange/queue declaration is idempotent and infrequent. No need to keep channels open. |
| Confirm mode on producer channel | At-least-once delivery guarantee. Critical for CDC/saga where message loss = data inconsistency. |
| WaitGroup in consumer | Tracks in-flight messages for graceful shutdown without losing work. |
| Stable window in backoff | Prevents stale escalated delays after long periods of health. |

---

## Quorum Queues & Clustering

Quorum queues use **Raft consensus** to replicate messages across cluster nodes. This is entirely server-side:

```
Node 1 (leader)  ←──Raft──→  Node 2 (follower)  ←──Raft──→  Node 3 (follower)
```

**From the client's perspective:**
- Connect to any node (or a load balancer).
- Declare the queue with `QueueType: QuorumQueue`.
- RabbitMQ handles replication automatically.
- If the leader node dies, a follower is elected as the new leader. Clients reconnect (handled by our auto-reconnect) and continue.

**Clustering is infrastructure config, not application code:**
```bash
# On node 2:
rabbitmqctl stop_app
rabbitmqctl join_cluster rabbit@node1
rabbitmqctl start_app
```

---

## Error Handling Philosophy

| Scenario | Behavior |
|----------|----------|
| Connection drops | Auto-reconnect with backoff, fire callbacks |
| Pool connection dies on Release | Async reconnect, replace if unrecoverable |
| Pool exhausted on Acquire | Immediate error (non-blocking) |
| Publish nacked by broker | Error returned to caller |
| Message unroutable | Error returned via mandatory return |
| Broker blocked (resource alarm) | Wait for confirm (backpressure) |
| Consumer handler fails | Nack + requeue |
| Shutdown during consumption | Wait for in-flight, then close |

---

## Project Structure

```
.
├── backoff/
│   └── backoff.go          # Exponential backoff with stable window
├── connection/
│   ├── connection.go       # Single connection + auto-reconnect
│   └── pool.go             # Connection pooling + async recovery
├── consumer/
│   └── consumer.go         # Consumer with prefetch + graceful shutdown
├── exchange/
│   └── exhchange.go        # Exchange/queue declaration + quorum support
├── helpers/
│   └── helper.go           # Shared config types
├── producer/
│   └── producer.go         # Publisher with confirms + backpressure
├── go.mod
├── go.sum
├── main.go
└── README.md
```

---

## Examples

### Full Application Example

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
    "github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/connection"
    "github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/consumer"
    "github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/exchange"
    "github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/helpers"
    "github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/producer"
)

func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer cancel()

    // 1. Create pool
    pool := connection.NewConnectionPool("amqp://guest:guest@localhost:5672/", 5, connection.DefaultOptions())
    if err := pool.Init(); err != nil {
        log.Fatal(err)
    }
    defer pool.Shutdown()

    // 2. Declare infrastructure
    conn, _ := pool.Acquire()
    ex := exchange.NewRabbitExchange("events", exchange.Topic, helpers.RabbitExchangeOptions{Durable: true})
    ex.CreateExchange(conn)
    ex.CreateQueue(conn, helpers.RabbitQueueConfig{
        Name:       "user.signup",
        BindingKey: "user.signup.#",
        QueueType:  helpers.QuorumQueue,
        Durable:    true,
    })
    pool.Release(conn)

    // 3. Start consumer
    consConn, _ := pool.Acquire()
    handler := func(ctx context.Context, msg amqp.Delivery) error {
        fmt.Printf("Processing: %s\n", string(msg.Body))
        return nil
    }
    cons := consumer.NewConsumer("user.signup", 10, handler)
    cons.GetChannel(consConn)
    cons.Consume(ctx)

    // 4. Publish a message
    pubConn, _ := pool.Acquire()
    pub := producer.NewProducer("events", "user.signup.us")
    pub.GetChannel(pubConn)
    pub.Publish(ctx, []byte(`{"user_id": "abc123", "email": "user@example.com"}`), pubConn, helpers.RabbitMqPublisherConfig{
        Persistent: true,
    })
    pool.Release(pubConn)

    // 5. Wait for shutdown signal
    <-ctx.Done()
    cons.Stop()
    pool.Release(consConn)
}
```

---

### Backoff — Standalone Usage

```go
import (
    "time"
    "github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/backoff"
)

bo := backoff.NewBackoffDelay(
    1*time.Second,   // initial delay
    1*time.Second,   // base delay (reset target)
    16*time.Second,  // max delay before reset
    30*time.Second,  // stable window
)
bo.StartTimer()

for {
    err := doSomethingUnreliable()
    if err == nil {
        break
    }
    bo.Wait() // sleeps 1s, 2s, 4s, 8s, 16s, resets to 1s...
}
```

---

### Connection — Single Connection with Reconnect Callbacks

```go
import (
    "github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/connection"
)

opts := connection.DefaultOptions()
conn := connection.NewRabbitMqConnectionClass("amqp://guest:guest@localhost:5672/", opts)

if err := conn.Connect(); err != nil {
    log.Fatal(err)
}
defer conn.Shutdown()

// Connection auto-reconnects on failure.
// handleDisconnect() goroutine watches NotifyClose and triggers reconnect().
```

---

### Pool — Acquire, Use, Release Pattern

```go
pool := connection.NewConnectionPool("amqp://guest:guest@localhost:5672/", 3, connection.DefaultOptions())
pool.Init()
defer pool.Shutdown()

// Acquire is non-blocking — returns error if pool exhausted
conn, err := pool.Acquire()
if err != nil {
    log.Println("Pool exhausted, try again later")
    return
}

// Use the connection...
ch, _ := conn.Connection.Channel()
ch.Close()

// Release back to pool
// If connection died, pool auto-reconnects in background
pool.Release(conn)
```

---

### Exchange — Different Exchange Types

```go
import (
    "github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/exchange"
    "github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/helpers"
)

conn, _ := pool.Acquire()
defer pool.Release(conn)

// Topic exchange — pattern-based routing
topicEx := exchange.NewRabbitExchange("events.topic", exchange.Topic, helpers.RabbitExchangeOptions{Durable: true})
topicEx.CreateExchange(conn)
topicEx.CreateQueue(conn, helpers.RabbitQueueConfig{
    Name:       "order.events",
    BindingKey: "order.*.us",  // matches order.created.us, order.cancelled.us
    QueueType:  helpers.QuorumQueue,
    Durable:    true,
})

// Direct exchange — exact routing key match
directEx := exchange.NewRabbitExchange("notifications.direct", exchange.Direct, helpers.RabbitExchangeOptions{Durable: true})
directEx.CreateExchange(conn)
directEx.CreateQueue(conn, helpers.RabbitQueueConfig{
    Name:       "email.notifications",
    BindingKey: "send.email",  // only exact match
    Durable:    true,
})

// Fanout exchange — broadcasts to all bound queues
fanoutEx := exchange.NewRabbitExchange("broadcast", exchange.Fanout, helpers.RabbitExchangeOptions{Durable: true})
fanoutEx.CreateExchange(conn)
fanoutEx.CreateQueue(conn, helpers.RabbitQueueConfig{
    Name:       "audit.log",
    BindingKey: "",  // ignored for fanout
    Durable:    true,
})
```

---

### Producer — Publishing with TTL and Priority

```go
import (
    "github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/producer"
    "github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/helpers"
)

conn, _ := pool.Acquire()
defer pool.Release(conn)

pub := producer.NewProducer("events.topic", "order.created.us")
pub.GetChannel(conn)

// Basic persistent publish (waits for broker confirm)
pub.Publish(ctx, []byte(`{"order_id": "456"}`), conn, helpers.RabbitMqPublisherConfig{
    Persistent: true,
})

// Fire and forget (no confirm, no mandatory routing, fastest)
pub.Publish(ctx, []byte(`{"metric": "page_view", "count": 1}`), conn, helpers.RabbitMqPublisherConfig{
    FireAndForget: true,
})

// With TTL (message expires after 60 seconds if not consumed)
pub.Publish(ctx, []byte(`{"otp": "1234"}`), conn, helpers.RabbitMqPublisherConfig{
    Persistent: true,
    Expiration: "60000",
})

// With priority (0-9, higher = more important)
pub.Publish(ctx, []byte(`{"alert": "critical"}`), conn, helpers.RabbitMqPublisherConfig{
    Persistent: true,
    Priority:   9,
})

// With custom content type and headers
contentType := "text/plain"
pub.Publish(ctx, []byte("raw event data"), conn, helpers.RabbitMqPublisherConfig{
    Persistent:  true,
    ContentType: &contentType,
    Headers:     amqp.Table{"x-source": "cdc-relay", "x-version": "1.0"},
})
```

---

### Consumer — With Error Handling and Graceful Shutdown

```go
import (
    "github.com/Srajan-Sanjay-Saxena/RabbitMqWrapper-Service-Go/consumer"
)

conn, _ := pool.Acquire()

handler := func(ctx context.Context, msg amqp.Delivery) error {
    var order Order
    if err := json.Unmarshal(msg.Body, &order); err != nil {
        // Returning error → message gets Nack'd and requeued
        return fmt.Errorf("invalid payload: %w", err)
    }

    if err := processOrder(ctx, order); err != nil {
        return err  // requeued for retry
    }

    // Returning nil → message gets Ack'd
    return nil
}

// prefetch=20 means broker sends up to 20 unacked messages at a time
cons := consumer.NewConsumer("order.events", 20, handler)
cons.GetChannel(conn)
cons.Consume(ctx)

// Later, on SIGTERM:
// 1. Cancel stops new deliveries
// 2. WaitGroup waits for in-flight messages to finish
// 3. Channel closes cleanly
cons.Stop()
pool.Release(conn)
```

---

### CDC Relay Pattern (Real-World Usage)

```go
// CDC captures change events from MongoDB/PostgreSQL
// and publishes them reliably via this library

func relayCDCEvent(ctx context.Context, pool *connection.ConnectionPool, event CDCEvent) error {
    conn, err := pool.Acquire()
    if err != nil {
        return fmt.Errorf("pool exhausted: %w", err)
    }
    defer pool.Release(conn)

    pub := producer.NewProducer("cdc.events", event.Table+"."+event.Operation)
    if err := pub.GetChannel(conn); err != nil {
        return err
    }

    payload, _ := json.Marshal(event)
    return pub.Publish(ctx, payload, conn, helpers.RabbitMqPublisherConfig{
        Persistent: true,
        Headers: amqp.Table{
            "x-source-table": event.Table,
            "x-operation":    event.Operation,
            "x-timestamp":    event.Timestamp.String(),
        },
    })
}
```

---

### Distributed Saga Pattern (Real-World Usage)

```go
// Each saga step publishes to the next step's queue
// On failure, publishes a compensation event

func sagaStep(ctx context.Context, pool *connection.ConnectionPool, msg amqp.Delivery) error {
    conn, err := pool.Acquire()
    if err != nil {
        return err
    }
    defer pool.Release(conn)

    pub := producer.NewProducer("saga.events", "")
    pub.GetChannel(conn)

    err = executeBusinessLogic(ctx, msg.Body)
    if err != nil {
        // Publish compensation event
        pub.routingKey = "order.compensate"
        pub.Publish(ctx, msg.Body, conn, helpers.RabbitMqPublisherConfig{Persistent: true})
        return err
    }

    // Publish next step
    pub.routingKey = "payment.process"
    return pub.Publish(ctx, msg.Body, conn, helpers.RabbitMqPublisherConfig{Persistent: true})
}
```

---

## License

MIT
