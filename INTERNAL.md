# INTERNAL.md — Design Decisions & Deep Dives

This document captures every architectural discussion, design decision, bug fix, and rationale behind the goRabbit-axon library.

---

## Table of Contents

1. [Circuit Breaker Design](#circuit-breaker-design)
2. [Why No `reconnectAttempts` — Breaker Owns State](#why-no-reconnectattempts)
3. [Logger Integration — Name Shadowing Pitfall](#logger-integration)
4. [Context Passing — Why Not Store in Struct](#context-passing)
5. [Channel Handler — OnChannelClose Callback](#channel-handler)
6. [Pool Architecture — Why Fixed Channel Pool](#pool-architecture)
7. [Buffered Channel vs Struct with Status](#buffered-channel-vs-struct)
8. [Two-Layer Callback System](#two-layer-callback-system)
9. [ReleaseChannel — The Wrong Way and The Fix](#releasechannel-fix)
10. [IRabbitConnection Interface Evolution](#interface-evolution)
11. [NotifyPublish Deadlock Bug Fix](#notifypublish-deadlock)
12. [GetChannel Round-Robin Logic](#getchannel-round-robin)
13. [Why Exchange Passes nil for OnClose](#exchange-nil-onclose)
14. [Producer OnClose — Auto-Nil Cached Channel](#producer-onclose)
15. [Consumer OnClose — WaitGroup Then Nil](#consumer-onclose)

---

## Circuit Breaker Design

### The Problem

We needed reconnection logic that doesn't hammer the broker during outages but recovers fast when the broker is back.

### The Solution

A circuit breaker with three states:

```
Closed → (failures >= threshold) → Open → (probe timeout) → HalfOpen → (success) → Closed
                                                                      → (failure) → Open
```

### Key Decisions

**Why `orDefault[T comparable]` generic helper?**

Go doesn't have `??` or default parameter syntax. We needed a clean way to provide defaults for `CircuitBreakerOptions`:

```go
func orDefault[T comparable](val, fallback T) T {
    var zero T
    if val == zero {
        return fallback
    }
    return val
}
```

This lets us write:
```go
opts := CircuitBreakerOptions{
    threshold:        orDefault(options.threshold, 5),
    baseResetTimeout: orDefault(options.baseResetTimeout, 5*time.Second),
    maxResetTimeout:  orDefault(options.maxResetTimeout, 60*time.Second),
}
```

**Why `orDefault` doesn't work with pointers:**

`orDefault` requires `comparable`. Pointers are comparable in Go, but nil pointer vs non-nil pointer comparison works differently. For `*logger.Logger`, we use a simple nil check instead:

```go
if log == nil {
    log = logger.New(logger.Production)
}
```

### GetBackoffDelay — Exponential with Jitter

```go
func (cb *CircuitBreaker) GetBackoffDelay(maxInterval time.Duration) time.Duration {
    delay := time.Duration(1<<cb.failuresCount) * time.Second  // 2^n seconds
    delay = min(delay, maxInterval)                             // cap it
    return time.Duration(rand.Float64() * float64(delay))       // jitter
}
```

- `1<<cb.failuresCount` = bit shift for 2^n (not `2**n` which is invalid Go)
- Jitter prevents thundering herd when multiple connections reconnect simultaneously
- `maxInterval` cap prevents absurdly long waits

### Probe — Exponential Reset Timeout Escalation

```go
func (cb *CircuitBreaker) Probe(ctx context.Context, afterWaitCb AfterProbeCb) error {
    select {
    case <-time.After(cb.currentResetTimeout):
        cb.state = HalfOpen
        cb.currentResetTimeout = min(cb.currentResetTimeout*2, cb.options.maxResetTimeout)
        afterWaitCb()
        return nil
    case <-ctx.Done():
        return ctx.Err()
    }
}
```

Each failed probe doubles the wait time. This prevents rapid probe-fail-probe-fail cycles when the broker is truly down for extended periods.

---

## Why No `reconnectAttempts`

### Before

```go
type RabbitMqConnectionClass struct {
    reconnectAttempts int
    // ...
}
```

### The Insight

The circuit breaker already manages:
- `failuresCount` — how many times we've failed
- `threshold` — when to stop trying (trip open)
- `state` — whether we should even attempt

Having `reconnectAttempts` on the connection is redundant bookkeeping. The breaker IS the reconnection policy.

### After

```
handleDisconnect → RecordFailure() → reconnect()
                                         │
                    breaker closed?  ──── yes → backoff + try
                    breaker open?   ──── yes → Probe(wait) → retry later
```

The connection just reacts to the breaker's decisions. Zero duplicate state.

---

## Logger Integration

### The Name Shadowing Problem

```go
// ❌ BROKEN
func NewCircuitBreaker(options CircuitBreakerOptions, logger *logger.Logger) *CircuitBreaker {
    return &CircuitBreaker{
        logger: orDefault(logger, logger.New(logger.Production)),
        //                        ^^^^^^ this is the PARAMETER, not the PACKAGE
    }
}
```

When a parameter is named `logger` and you import a package called `logger`, the parameter shadows the package. `logger.New()` tries to call `.New()` on the pointer, not the package function.

### The Fix

Name the field/parameter `log`:

```go
func NewCircuitBreaker(options CircuitBreakerOptions, log *logger.Logger) *CircuitBreaker {
    if log == nil {
        log = logger.New(logger.Production)  // "logger" is the package here
    }
}
```

---

## Context Passing

### The Anti-Pattern

```go
// ❌ Storing ctx in struct
type ChannelHandler struct {
    ctx context.Context
}
```

From Go docs: "Do not store Contexts inside a struct type."

**Why it's bad:**
- A struct outlives any single operation, but a context is scoped to one
- You can't cancel different operations independently
- Testing becomes harder (can't pass different contexts per test case)

### The Fix

Pass context through method calls:

```go
func (ch *ChannelHandler) GetChannel(ctx context.Context, conn *amqp.Connection, onClose OnChannelClose) (*amqp.Channel, error)
```

For the single connection handler, context flows from:
`Connect(ctx)` → `handleDisconnect(ctx)` → `reconnect(ctx)` → `Probe(ctx, ...)`

Shutdown cancels naturally because closing the connection fires `NotifyClose`, and the goroutine checks `shutDownInitiated` before reconnecting.

---

## Channel Handler

### What It Does

```go
type OnChannelClose func(conn *amqp.Connection)

type ChannelHandler struct {
    logger *logger.Logger
}
```

Opens a channel, spawns a goroutine to watch for its death via `NotifyClose`, and fires the caller's callback when it dies.

### Why `OnChannelClose` Only Has `conn`

Originally it was `func(deadCh *amqp.Channel, conn *amqp.Connection)`.

The dead channel is useless:
- The pool doesn't search for it (it's already out of the buffer)
- The producer doesn't need it (it just nils its reference)
- Nobody does anything with the dead channel pointer

All we need is `conn` to open a replacement on the same connection.

### HandleChannelClose — ctx.Done() Guard

```go
func (ch *ChannelHandler) HandleChannelClose(ctx context.Context, channel *amqp.Channel, conn *amqp.Connection, onClose OnChannelClose) {
    closeCh := channel.NotifyClose(make(chan *amqp.Error, 1))
    select {
    case err := <-closeCh:
        if err != nil {
            ch.logger.Warn("channel closed unexpectedly", "error", err)
        }
        if onClose != nil {
            onClose(conn)
        }
    case <-ctx.Done():
        return
    }
}
```

Without `ctx.Done()`, if shutdown cancels a context without explicitly closing channels (async shutdown), the goroutine leaks forever waiting on `closeCh`.

### Why `OnChannelClose` is Per-Call, Not Per-Handler

Different callers need different reactions:
- Pool → replace dead channel in buffer
- Producer → nil the cached channel
- Consumer → wait for in-flight, then nil
- Exchange → doesn't care (passes nil)

If it were per-handler, you'd need separate handler instances for each use case, or complex dispatch logic.

---

## Pool Architecture

### The Problem With the Old Pool

The original pool used `Acquire()`/`Release()` on raw connections:

```go
conn, _ := pool.Acquire()
defer pool.Release(conn)
// ... use conn directly
```

This is useless for load balancing because every producer/consumer just grabs one connection and holds it forever. You end up with N connections but only 1 being used.

### Why Fixed Channel Pool Per Connection

| Approach | Problem |
|----------|---------|
| Unlimited channels on demand | Channel bomb, broker OOM |
| Single channel per connection | Bottleneck, no concurrency |
| Least-loaded tracking | Complexity for no gain at fixed size |
| Channel-per-publish | Latency overhead, GC pressure |
| **Fixed pool (our approach)** | Bounded, pre-warmed, backpressure, simple |

### The Design

```
ConnectionPool
├── Conn #0 → buf chan (cap=5): [ch, ch, ch, ch, ch]  (buffered channel)
├── Conn #1 → buf chan (cap=5): [ch, ch, ch, ch, ch]
└── Conn #2 → buf chan (cap=5): [ch, ch, ch, ch, ch]
```

- `Connect()` — creates N connections, pre-warms M channels per connection
- `GetChannel()` — round-robin picks a connection, pops a channel from its buffer
- Channel dies → `replaceDeadChannel` fires → opens replacement → pushes into buffer
- `Shutdown()` — drains all buffers, closes channels, closes connections

### Why Buffered Channel As the Buffer

The Go channel IS the concurrency primitive:
- Pop = `<-buf` (non-blocking with `select`)
- Push = `buf <- ch`
- Thread-safe by design
- Presence in buffer = free, absence = acquired (no status field needed)
- Exhausted = empty channel → `select default` → immediate error (backpressure)

### PoolOptions — Tunable Per Service

```go
type PoolOptions struct {
    ConnSize    int  // default 3
    ChanPerConn int  // default 5
}
```

High-throughput publisher service: `ChanPerConn: 20`
Simple consumer: `ChanPerConn: 2`

---

## Buffered Channel vs Struct with Status

### The Struct Approach (Rejected)

```go
type PooledChannel struct {
    ch     *amqp.Channel
    status string  // "acquired" / "free"
}
```

Problems:
- Needs mutex to read/write status safely
- `GetChannel()` must iterate whole slice — O(n)
- Race condition if two goroutines see same "free" channel

### The Buffered Channel Approach (Chosen)

```go
available chan *amqp.Channel  // buffered, size = chanPerConn
```

- `GetChannel()` = pop → O(1), lock-free, inherently thread-safe
- `ReleaseChannel()` = push → O(1)
- No status field — presence = free, absence = acquired
- Exhausted pool = empty channel → immediate error

The Go channel IS the status tracking.

---

## Two-Layer Callback System

When a channel dies, TWO things need to happen:

### Layer 1: Pool Self-Healing (Infrastructure)

```go
// replaceDeadChannel — registered during pre-warm
func (p *Pool) replaceDeadChannel(conn *amqp.Connection) {
    // find buffer → open new channel → push into buffer
    // pool stays at full capacity automatically
}
```

### Layer 2: Caller State Cleanup (Application)

```go
// Producer's onClose — registered during GetChannel
func(_ *amqp.Connection) {
    rProd.channel = nil
    rProd.confirmCh = nil
    rProd.returnCh = nil
}

// Consumer's onClose
func(_ *amqp.Connection) {
    c.wg.Wait()   // let in-flight finish
    c.channel = nil
}
```

### How They Coexist

During `Connect()`, pre-warmed channels get `replaceDeadChannel` as their watcher callback.

During `GetChannel()`, if the caller provides an `onClose`, a SECOND watcher goroutine is spawned for that channel with the caller's callback.

Both fire independently when the channel dies:
1. `replaceDeadChannel` refills the buffer (but the channel was already out since the caller had it)
2. Caller's `onClose` cleans up the caller's state

Wait — actually there's a subtlety. The pre-warm watcher fires `replaceDeadChannel`. But the channel was popped out of the buffer and given to a caller. So when it dies:
- The pre-warm watcher's `replaceDeadChannel` fires → puts a NEW channel into the buffer ✓
- The caller's `onClose` fires → nils their reference ✓

Both work. Buffer stays full. Caller knows their channel is dead.

---

## ReleaseChannel Fix

### The Broken Version

```go
func (p *Pool) ReleaseChannel(ch *amqp.Channel) {
    for _, conn := range p.connections {
        buf := p.chanPool[conn]
        select {
        case buf <- ch:  // shoves into ANY buffer with space
            return
        default:
            continue
        }
    }
}
```

This is wrong — it pushes the channel into whichever buffer has space, not the one that owns it. A channel opened on Connection A could end up in Connection B's buffer.

### The Fix

Caller tells us which connection it belongs to:

```go
func (p *Pool) ReleaseChannel(targetConn *amqp.Connection, ch *amqp.Channel) {
    for _, conn := range p.connections {
        if conn.Connection != targetConn {
            continue
        }
        select {
        case p.chanPool[conn] <- ch:
            return
        default:
            p.log.Warn("buffer full, closing channel")
            ch.Close()
            return
        }
    }
    p.log.Warn("connection not found in pool, closing orphaned channel")
    ch.Close()
}
```

Once we find the matching connection: push or close. No more iteration after the match.

### Do We Even Need ReleaseChannel?

For most use cases, no:
- Producer holds channel for its lifetime → channel dies → onClose fires → pool replaces
- Consumer holds channel for its lifetime → same flow
- Exchange uses short-lived channels → deferred close → onClose fires → pool replaces

`ReleaseChannel` is only for cases where a caller explicitly wants to give back a still-healthy channel (e.g., dynamic scaling down of producers).

---

## Interface Evolution

### V1 — Tied to Concrete Types

```go
func (pub *Producer) GetChannel(conn *connection.RabbitMqConnectionClass) error
```

Problem: producer is coupled to single connection. Can't use with pool.

### V2 — Simple Interface

```go
type IRabbitConnection interface {
    GetChannel() (*amqp.Channel, error)
    Shutdown() error
}
```

Problem: no way to pass `onClose` callback or context.

### V3 — Final Interface

```go
type IRabbitConnection interface {
    GetChannel(ctx context.Context, onClose channel.OnChannelClose) (*amqp.Channel, error)
    Shutdown() error
}
```

Both `SingleConnectionHandler` and `ConnectionPool` implement this. Producer, consumer, and exchange all accept the interface. Pass either and it works:

```go
pub.GetChannel(ctx, pool)   // ✓ round-robin, pre-warmed
pub.GetChannel(ctx, conn)   // ✓ direct channel from single connection
```

---

## NotifyPublish Deadlock Bug Fix

### The Original Problem

Registering `NotifyPublish` on every `Publish()` call caused a deadlock on the 3rd publish.

### How `NotifyPublish` Works Internally

The `amqp091-go` library maintains an append-only list of listener channels:

```go
type confirms struct {
    listeners []chan amqp.Confirmation  // append-only, never removes
}

func (c *confirms) One(confirm Confirmation) {
    for _, listener := range c.listeners {
        listener <- confirm  // BLOCKS if full, sends to ALL
    }
}
```

### The Deadlock Trace

```
Publish #1: listeners = [ch1]         → ch1 gets confirm → read ✓
Publish #2: listeners = [ch1, ch2]    → ch1 gets confirm (unread!), ch2 gets confirm → read ch2 ✓
Publish #3: listeners = [ch1, ch2, ch3]
    → try send to ch1 → BLOCKS (ch1 has unread confirm from #2)
    → ch2 never reached
    → ch3 never reached
    → your select on ch3 waits forever
    → DEADLOCK
```

### The Fix

Register once in `GetChannel`, reuse across all publishes:

```go
func (rProd *RabbitMqProducer) GetChannel(ctx context.Context, conn helpers.IRabbitConnection, ...) error {
    ch, err := conn.GetChannel(ctx, onClose)
    ch.Confirm(false)
    rProd.confirmCh = ch.NotifyPublish(make(chan amqp.Confirmation, 1))  // ONCE
    rProd.returnCh = ch.NotifyReturn(make(chan amqp.Return, 1))          // ONCE
    rProd.channel = ch
}
```

One listener. Every confirm goes to that one channel. Read it on every publish. No accumulation.

---

## GetChannel Round-Robin Logic

```go
func (p *Pool) GetChannel(ctx context.Context, onClose channel.OnChannelClose) (*amqp.Channel, error) {
    p.mu.Lock()
    if len(p.connections) == 0 {
        p.mu.Unlock()
        return nil, errors.New("pool not initialized")
    }
    startIdx := p.connIdx
    p.connIdx = (p.connIdx + 1) % len(p.connections)
    p.mu.Unlock()

    for i := 0; i < len(p.connections); i++ {
        conn := p.connections[(startIdx+i)%len(p.connections)]
        // try pop from this conn's buffer...
    }
}
```

**Why `startIdx` outside and loop inside?**

- `startIdx` = round-robin starting point (advances by 1 each call for distribution)
- Loop = fallback if that connection is dead or buffer empty

**Why length check BEFORE modulo?**

Divide by zero protection. If pool isn't initialized, `% len(p.connections)` panics.

**Why mutex only for `connIdx` update?**

The `connections` slice and `chanPool` map are only written during `Connect()` (which holds the lock). After initialization, they're read-only. The buffered channel operations (`<-buf`) are inherently thread-safe.

---

## Why Exchange Passes nil for OnClose

Exchange operations are one-shot:

```go
func (rbEx *RabbitExchangeClass) CreateExchange(ctx context.Context, conn helpers.IRabbitConnection) error {
    ch, err := conn.GetChannel(ctx, nil)  // nil = no callback
    defer ch.Close()
    return ch.ExchangeDeclare(...)
}
```

The channel lives for microseconds. It opens, declares, closes. If it dies mid-declare, the error propagates via the return value. No state to clean up. No callback needed.

---

## Producer OnClose — Auto-Nil Cached Channel

```go
func (rProd *RabbitMqProducer) GetChannel(ctx context.Context, conn helpers.IRabbitConnection, ...) error {
    ch, err := conn.GetChannel(ctx, func(_ *amqp.Connection) {
        rProd.channel = nil
        rProd.confirmCh = nil
        rProd.returnCh = nil
    })
    // ...
}
```

When the channel dies:
1. `onClose` fires → all references nil'd
2. Next `Publish()` call → `rProd.channel == nil` → returns error
3. Caller checks `pub.IsChannelValid()` → false → calls `GetChannel()` again

The producer never uses a dead channel. It fails fast and tells the caller to re-acquire.

---

## Consumer OnClose — WaitGroup Then Nil

```go
func (c *RabbitMqConsumer) GetChannel(ctx context.Context, conn helpers.IRabbitConnection) error {
    ch, err := conn.GetChannel(ctx, func(_ *amqp.Connection) {
        c.wg.Wait()    // wait for in-flight messages to finish
        c.channel = nil
    })
    // ...
}
```

When the channel dies:
1. The delivery channel (`msgs`) closes → consume goroutine exits via `!ok`
2. `onClose` fires → `wg.Wait()` blocks until all in-flight handlers return
3. `c.channel = nil` → consumer knows it's dead

No message is left half-processed. No goroutine leaks.

---

## AMQP Channel Thread Safety

**AMQP channels are NOT thread-safe.** Two goroutines publishing on the same channel = corrupted frames = broker closes the channel.

This is why the pool uses a checkout model:
- Each `GetChannel()` pops a channel from the buffer → exclusive ownership
- The caller (producer/consumer) holds it for its lifetime
- Nobody else can touch it until it's released or dies

This also why confirm mode delivery tags work — they're scoped to a single channel's sequence (1, 2, 3...). If two producers shared a channel, their confirms would interleave and you couldn't match ack to message.

---

## replaceDeadChannel Flow

```
Pre-warm: channelHandler.GetChannel(ctx, conn, replaceDeadChannel)
    → channel created
    → HandleChannelClose goroutine spawned with replaceDeadChannel callback
    → channel pushed into buffer

Later: channel dies (broker closes it, network blip, etc.)
    → NotifyClose fires
    → HandleChannelClose receives the error
    → Calls replaceDeadChannel(conn)

replaceDeadChannel:
    → Lock pool mutex
    → Find which buffer belongs to this connection
    → Check connection is still alive
    → Open new channel on same connection
    → Spawn new HandleChannelClose watcher for replacement
    → Push replacement into buffer (non-blocking)
    → Unlock

Result: pool is back to full capacity, no caller intervention needed
```

---

## Error Handling Philosophy

| Scenario | Behavior |
|----------|----------|
| Connection drops | Circuit breaker RecordFailure → reconnect with backoff |
| Circuit opens | Probe waits → HalfOpen → retry |
| Pool channel dies | Auto-replaced via replaceDeadChannel callback |
| Pool exhausted | Immediate error (backpressure, non-blocking) |
| Publish with dead channel | Error returned, caller re-acquires |
| Publish nacked | Error returned to caller |
| Message unroutable | Error returned via mandatory return |
| Consumer handler fails | Nack + requeue |
| Shutdown during consumption | wg.Wait for in-flight → close |
| Context cancelled | All goroutines exit cleanly |
