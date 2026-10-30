# Synaptik AMQP

Library to use `@sektek/synaptik` with RabbitMQ. It translates between AMQP messages and synaptik `Event`s:

| Class | Role |
|-------|------|
| `AmqpGateway` | Consumes a queue and hands each message to an event handler |
| `AmqpChannel` | Publishes events to an exchange / queue |
| `AmqpRpcProcessor` | Request/reply over AMQP: publishes an event and resolves with the reply |
| `ReplyListener` | Server side of RPC: publishes a gateway's result back to the caller |
| `DefaultConnectionProvider` / `DefaultChannelProvider` | Lazily create and manage the `amqplib` connection and channel |

## Installation

```sh
npm install @sektek/synaptik-amqp
```

## Connecting

Every class accepts the same connection options. Provide one of:

- `connectionOptions` – an `amqplib` connect options object, optionally with a `url`. The connection is created lazily on first use and closed on stop.
- `connection` – an existing `ChannelModel`. It is used as-is and is **not** closed on stop.
- `channel` (channels and gateways) – an existing `Channel`. It is used as-is and is **not** closed on stop.
- `channelProvider` – a custom provider for the `Channel` (see [Pluggable components](#pluggable-components)).

```ts
const connectionOptions = { url: 'amqp://guest:guest@localhost:5672' };
```

If you build several components, share one connection instead of letting each open its own:

```ts
import { connect } from 'amqplib';

const connection = await connect('amqp://localhost');

const gateway = new AmqpGateway({ connection, queueName: 'orders', handler });
const channel = new AmqpChannel({ connection, queueName: 'shipments' });
```

## Consuming messages: `AmqpGateway`

`AmqpGateway` is a `Service`. On `start()` it asserts the queue, applies `prefetch`, and begins consuming. Each message is converted to an `Event`, passed to `handler`, and acknowledged on success.

```ts
import { AmqpGateway } from '@sektek/synaptik-amqp';

const gateway = new AmqpGateway({
  connectionOptions: { url: 'amqp://localhost' },
  queueName: 'orders',
  queueOptions: { durable: true },
  prefetch: 10,
  handler: async event => {
    console.log('received', event.type, event.data);
  },
});

await gateway.start();
// ...
await gateway.stop();
```

| Option | Default | Description |
|--------|---------|-------------|
| `queueName` | required | Queue to consume. It is asserted on start. |
| `handler` | required | Event handler (function or object with `handle`). |
| `queueOptions` | `{}` | `assertQueue` options, e.g. `{ durable: true }`. |
| `consumeOptions` | `{}` | `consume` options. Set `noAck: true` to disable acknowledgements. |
| `prefetch` | `0` (unlimited) | Maximum unacknowledged messages. |
| `requeueOnError` | `true` | Whether a failed message is requeued when nacked. Ignored if `noAck` is set. |
| `eventExtractor` | JSON parse | Converts a `ConsumeMessage` into an `Event`. |

Error handling: if the extractor or handler throws, the message is nacked (requeued unless `requeueOnError: false`) and `message:error` is emitted. `event:error` is also emitted when the event had been extracted.

### Events

`AmqpGateway` emits the standard synaptik handler events (`event:received`, `event:processed`, `event:error`) plus:

| Event | Arguments |
|-------|-----------|
| `gateway:started` | `channel, consumer` |
| `gateway:stopped` | |
| `message:received` | `message` |
| `message:processed` | `message, event, result` |
| `message:acknowledged` | `channel, message, event` |
| `message:error` | `error, message` |

## Publishing events: `AmqpChannel`

`AmqpChannel` serializes an event and publishes it. It implements the synaptik `EventChannel` contract, so it can be used anywhere a channel is accepted.

```ts
import { AmqpChannel } from '@sektek/synaptik-amqp';

const channel = new AmqpChannel({
  connectionOptions: { url: 'amqp://localhost' },
  exchange: 'events',
  routingKey: 'orders.created',
});

await channel.send({ id: '1', type: 'order.created', data: { total: 42 } });
```

| Option | Default | Description |
|--------|---------|-------------|
| `exchange` | `''` (default exchange) | Exchange to publish to. |
| `routingKey` | `queueName`, else `''` | Static routing key. |
| `queueName` | | Convenient with the default exchange, where the routing key is the queue name. |
| `routingKeyProvider` | | Derives the routing key from each event. Takes precedence over `routingKey` / `queueName`. |
| `publishOptionsProvider` | content type `application/json` and `type` set from the event | Builds the `amqplib` publish options for each event. |
| `eventSerializer` | `JSON.stringify` | Converts the event to the message body. |
| `timeout` | `10000` | Milliseconds to wait for the channel to drain when the write buffer is full. |

`send(event, options)` accepts per-call overrides: `exchange`, `routingKey`, and any `amqplib` publish option (such as `correlationId`, `replyTo`, `persistent`, or `headers`). When the underlying channel applies back-pressure, `send` waits for `drain` and rejects with `Failed to publish message` if that does not happen within `timeout`.

`AmqpChannel` emits `event:received`, `event:delivered`, and `event:error`, plus `message:sent` with `exchange, routingKey, publishOptions, message`.

## Request/reply (RPC)

RPC uses two pieces, one on each side of the queue.

### Client: `AmqpRpcProcessor`

`AmqpRpcProcessor` is an `EventProcessor<T, R>`. For each call to `process(event)` it:

1. Creates an exclusive, server-named reply queue.
2. Publishes the event with a `correlationId` and `replyTo` set to that queue.
3. Resolves with the reply event, or rejects if the reply is invalid or `replyTimeout` elapses.
4. Cancels the consumer and deletes the reply queue.

```ts
import { AmqpRpcProcessor } from '@sektek/synaptik-amqp';

const rpc = new AmqpRpcProcessor({
  connectionOptions: { url: 'amqp://localhost' },
  queueName: 'rpc.quote',
  replyTimeout: 5000,
});

const reply = await rpc.process({ id: '1', type: 'quote.request', data: { sku: 'abc' } });
```

It accepts all the `AmqpChannel` options (for the outbound request) plus:

| Option | Default | Description |
|--------|---------|-------------|
| `replyTimeout` | `0` (wait forever) | Milliseconds to wait for the reply. |
| `timeout` | `10000` | Passed to the outbound `AmqpChannel` as its publish drain wait. It does not limit the reply wait. |
| `correlationIdProvider` | `randomUUID()` | Generates the correlation id per request. |
| `extractor` | JSON parse | Converts the reply message into an `Event`. |
| `outboundChannel` | new `AmqpChannel` | Supply a preconfigured `AmqpChannel` for requests. |

A reply whose `correlationId` does not match rejects the call.

> `timeout` and `replyTimeout` are independent: `timeout` only bounds the publish drain wait on the outbound channel, and `replyTimeout` only bounds the reply wait. Callers that used `timeout` to bound the reply must switch to `replyTimeout`.

### Server: `AmqpGateway` + `ReplyListener`

`ReplyListener` attaches to a gateway's `message:processed` event and, when the incoming message has a `replyTo`, publishes the handler's result back to that queue with the original `correlationId`. The gateway handler must return the reply event.

```ts
import { AmqpChannel, AmqpGateway, ReplyListener } from '@sektek/synaptik-amqp';

const connection = await connect('amqp://localhost');

const gateway = new AmqpGateway({
  connection,
  queueName: 'rpc.quote',
  handler: async event => ({
    id: crypto.randomUUID(),
    type: 'quote.response',
    parentId: event.id,
    data: { price: 42 },
  }),
});

new ReplyListener({
  gateway,
  outboundChannel: new AmqpChannel({ connection }),
});

await gateway.start();
```

Messages without a `replyTo` are processed normally and no reply is sent.

## Pluggable components

Following the rest of the `@sektek` libraries, each dependency below can be supplied either as a plain function or as an object with the named method:

| Option | Method | Signature |
|--------|--------|-----------|
| `channelProvider` | `get` | `() => Channel` |
| `routingKeyProvider` | `get` | `(event) => string` |
| `publishOptionsProvider` | `get` | `(event, options?) => Options.Publish` |
| `correlationIdProvider` | `get` | `(event) => string` |
| `eventSerializer` | `serialize` | `(event) => string \| Uint8Array` |
| `extractor` (`AmqpRpcProcessor`) | `get` | `(message) => Event` |

```ts
new AmqpChannel({
  connection,
  exchange: 'events',
  routingKeyProvider: event => `events.${event.type}`,
  eventSerializer: event => JSON.stringify(event),
});
```

`AmqpGateway.eventExtractor` is the exception: it takes a function `(message: ConsumeMessage) => Event | Promise<Event>`.

## Connection and channel lifecycle

`DefaultConnectionProvider` and `DefaultChannelProvider` are used automatically, but can be constructed directly to share a channel between components or to observe lifecycle events.

- `DefaultConnectionProvider` emits `connection:created`, `connection:closed`, and `connection:error`. `get()` rejects after `stop()`.
- `DefaultChannelProvider` emits `channel:created`, `channel:closed`, and `channel:error`. A closed channel is recreated on the next `get()`.
- Each provider closes only what it created. A `connection` or `channel` you pass in is left open for you to manage.
- `DefaultConnectionProvider` registers itself with a `ProcessManager` (yours via `processManager`, otherwise one of its own) from `@sektek/utility-belt`, which manages stopping it.

## Types

All option and component types (`AmqpChannelOptions`, `AmqpGatewayOptions`, `MessageEventExtractorFn`, `ChannelProviderComponent`, ...) are exported from the package root.
