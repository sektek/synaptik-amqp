# CLAUDE.md — @sektek/synaptik-amqp

RabbitMQ transport adapter for `@sektek/synaptik`. Follows the Gateway/Channel duality pattern: `AmqpGateway` consumes messages, `AmqpChannel` publishes them. `AmqpRpcProcessor` adds request/reply over AMQP.

## Commands

```bash
npm run build        # Compile (tsc -p tsconfig.build.json)
npm test             # Run all tests (mocha + tsx/esm)
npm run test:cover   # Coverage via c8

# Single test file:
npx mocha --import tsx/esm src/path/to/file.spec.ts
```

## Source layout

```
src/
  types/                        # All TypeScript interfaces and component types
  amqp-gateway.ts               # Consumer: receives messages, invokes handler
  amqp-channel.ts               # Producer: serializes events, publishes to exchange
  amqp-rpc-processor.ts         # RPC client: send request, await correlated reply
  default-channel-provider.ts   # Connection + channel lifecycle
  default-event-extractor.ts    # JSON.parse of message.content
  reply-listener.ts             # RPC server helper: sends replies from gateway results
  *.spec.ts                     # Tests co-located with source
```

## Classes

### `AmqpGateway<T, R>`

Consumes messages from a queue, extracts an `Event`, and invokes a handler.

**Key options (`AmqpGatewayOptions`):**

| Option | Default | Purpose |
|--------|---------|---------|
| `queueName` | required | AMQP queue to consume |
| `handler` | required | Event handler to invoke |
| `channelProvider` / `channel` | — | Channel source |
| `eventExtractor` | `defaultEventExtractor` (JSON.parse) | Extracts `Event` from `ConsumeMessage` |
| `requeueOnError` | `true` | Nack + requeue on handler error |
| `prefetch` | `0` | Channel prefetch count |
| `consumeOptions` | — | Passed to `channel.consume()` |
| `queueOptions` | — | Passed to `channel.assertQueue()` |
| `connection` / `connectionOptions` | — | AMQP connection |

**Processing flow:**
```
message → emit message:received
        → eventExtractor → emit event:received
        → handler(event)
        → success: ack, emit event:processed + message:processed + message:acknowledged
        → error:   nack (requeueOnError), emit event:error + message:error
```

**Methods:** `start()`, `stop()`

**Events emitted:**

| Event | Payload |
|-------|---------|
| `gateway:started` | `(channel, consumer)` |
| `gateway:stopped` | — |
| `message:received` | `(message)` |
| `message:processed` | `(message, event, result)` |
| `message:acknowledged` | `(channel, message, event)` |
| `message:error` | `(error, message)` |
| `event:received` | `(event)` |
| `event:processed` | `(event, result)` |
| `event:error` | `(error, event)` |

If `noAck: true` in `consumeOptions`, ack/nack calls become no-ops.

---

### `AmqpChannel<T>`

Serializes an `Event` and publishes it to an AMQP exchange.

**Key options (`AmqpChannelOptions`):**

| Option | Default | Purpose |
|--------|---------|---------|
| `channelProvider` / `channel` | — | Channel source |
| `exchange` | `''` (default exchange) | AMQP exchange name |
| `routingKeyProvider` / `routingKey` / `queueName` | — | Routing key resolution |
| `eventSerializer` | `JSON.stringify` | Serializes event to buffer |
| `publishOptionsProvider` | adds `contentType` + `type` | `Options.Publish` builder |
| `timeout` | `10000` ms | Drain wait timeout |
| `connection` / `connectionOptions` | — | AMQP connection |

**Processing flow:**
```
send(event) → emit event:received
            → serialize event
            → get routing key
            → channel.publish(exchange, key, buffer, opts)
            → if buffer full: wait for 'drain' event (timeout: 10s)
            → emit message:sent + event:delivered
            → error: emit event:error, rethrow
```

**Events emitted:** `event:received`, `event:delivered`, `event:error`, `message:sent`

---

### `AmqpRpcProcessor<T, R>`

RPC client. Sends a request event with a correlation ID and an exclusive reply queue, then awaits the correlated reply.

**Key options (`AmqpProcessorOptions`):**

| Option | Default | Purpose |
|--------|---------|---------|
| `outboundChannel` | created from opts | `AmqpChannel` for sending requests |
| `correlationIdProvider` | `randomUUID()` | Generates correlation ID per request |
| `extractor` | `defaultEventExtractor` | Extracts `Event` from reply message |
| `timeout` | `0` (no timeout) | Max wait for reply (ms) |

**Processing flow:**
```
process(event) → emit event:received
              → assert exclusive reply queue
              → create PromiseChannel<R> to await reply
              → generate correlationId
              → consume reply queue (noAck, exclusive)
                  → validate correlationId matches
                  → extract R via extractor
                  → PromiseChannel.send(reply)
                  → finally: cancel consumer, delete queue
              → send request via outbound channel (correlationId + replyTo set)
              → return PromiseChannel.get()
```

**Events emitted:** `event:received`, `event:processed`, `event:error`, `message:received`, `message:processed`, `message:error`

---

### `DefaultChannelProvider`

Manages connection and channel lifecycle. Caches both; reconnects on demand.

**Options:** `channel?`, `connection?`, `connectionOptions?` (URL string or `Options.Connect`)

**Methods:** `get(): Promise<Channel>`, `connection(): Promise<ChannelModel>`

**Events:** `connection:created`, `connection:closed`, `connection:error`, `channel:created`, `channel:closed`, `channel:error`

---

### `ReplyListener<T>`

RPC server helper. Listens on `gateway`'s `message:processed` event and sends the result back via `outboundChannel` using the original `correlationId` and `replyTo` queue.

**Options:** `outboundChannel: AmqpChannel<T>`, `gateway: AmqpGateway<Event, T>`

---

### `defaultEventExtractor`

```ts
defaultEventExtractor<T extends Event>(message: ConsumeMessage): T
// JSON.parse(message.content.toString())
```

## Types (`src/types/`)

| Type | Description |
|------|-------------|
| `AmqpSerializerFn<T>` / `AmqpSerializerComponent<T>` | Serializes event → `Uint8Array \| string` via `.serialize` |
| `MessageEventExtractorFn<T>` / `MessageEventExtractorComponent<T>` | Extracts event from `ConsumeMessage` via `.extract` |
| `ChannelProviderFn<T>` / `ChannelProviderComponent<T>` | Provides `Channel` via `.get` |
| `RoutingKeyProviderComponent<T>` | Provides routing key string from event via `.get` |
| `CorrelationIdProviderComponent<T>` | Provides correlation ID string from event via `.get` |
| `PublishOptionsProviderComponent<T>` | Provides `Options.Publish` from event via `.get` |
| `ConnectionOptions` | `Options.Connect & { url?: string }` |
| `AmqpServiceOptions` | `EventServiceOptions & { connection?, connectionOptions? }` |

## Testing

Tests run against a **real RabbitMQ instance** — there is no mock of amqplib.

```bash
# Connection defaults:
AMQP_HOST=localhost  # override via env var
```

**Patterns:**
- Queues are asserted with random UUID names per test run; deleted in `after()`
- `sinon.fake()` / `sinon.stub()` for handlers and providers
- `await new Promise(resolve => setTimeout(resolve, 500))` to allow async AMQP processing
- Spy on `channel.ack()` / `channel.nack()` to assert acknowledgement behaviour
- `sinon.reset()` in `afterEach` clears all call history

## Key constraints

- No new dependencies without explicit approval
- ESM only; imports use `.js` extensions
- Decorators enabled
- Depends on `@sektek/synaptik`, `@sektek/utility-belt`, `amqplib`
