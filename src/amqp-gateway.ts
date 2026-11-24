import {
  AbstractEventComponent,
  EVENT_ERROR,
  EVENT_PROCESSED,
  EVENT_RECEIVED,
  Event,
  EventHandlerEvents,
  EventHandlerFn,
  EventHandlerReturnType,
  getEventHandlerComponent,
} from '@sektek/synaptik';
import { Channel, ConsumeMessage, Options, Replies } from 'amqplib';
import {
  ErrorHandlerFn,
  EventEmittingService,
  SERVICE_STARTED,
  SERVICE_START_ERROR,
  SERVICE_STOPPED,
  SERVICE_STOP_ERROR,
  ServiceEvents,
  getComponent,
  toError,
} from '@sektek/utility-belt';

import {
  AmqpServiceOptions,
  ChannelProviderComponent,
  ChannelProviderFn,
  MessageEventExtractorFn,
} from './types/index.js';
import { DefaultChannelProvider } from './default-channel-provider.js';
import { defaultEventExtractor } from './default-event-extractor.js';

export type AmqpGatewayOptions<
  T extends Event = Event,
  R extends EventHandlerReturnType = unknown,
> = AmqpServiceOptions & {
  channel?: Channel;
  channelProvider?: ChannelProviderComponent;
  consumeOptions?: Options.Consume;
  eventExtractor?: MessageEventExtractorFn<T>;
  handler: EventHandlerFn<T, R>;
  /** Only applies if noAck is false */
  requeueOnError?: boolean;
  prefetch?: number;
  queueName: string;
  queueOptions?: Options.AssertQueue;
};

/**
 * Events emitted by an {@link AmqpGateway}.
 *
 * - `gateway:started` `(channel, consumer)`: consuming has started.
 * - `gateway:stopped`: consuming has stopped. The gateway emits it without
 *   arguments.
 * - `service:started`: the `Service` counterpart of `gateway:started`.
 * - `service:startError` `(error)`: `start()` failed.
 * - `service:stopped`: the `Service` counterpart of `gateway:stopped`.
 * - `service:stopError` `(error)`: `stop()` failed.
 * - `message:received` `(message)`: a message arrived.
 * - `message:processed` `(message, event, result)`: the handler returned.
 * - `message:acknowledged` `(channel, message, event)`: the message was
 *   acknowledged.
 * - `message:error` `(error, message)`: processing the message failed.
 *
 * @template T The event type handled by the gateway.
 * @template R The result type returned by the handler.
 */
export type AmqpGatewayEvents<
  T extends Event = Event,
  R extends EventHandlerReturnType = unknown,
> = EventHandlerEvents<T> &
  ServiceEvents & {
    'gateway:started': (channel: Channel, consumer: Replies.Consume) => void;
    'gateway:stopped': (channel: Channel, consumer: Replies.Consume) => void;
    'message:received': (message: ConsumeMessage) => void;
    'message:processed': (message: ConsumeMessage, event: T, result: R) => void;
    'message:acknowledged': (
      channel: Channel,
      message: ConsumeMessage,
      event: T,
    ) => void;
    'message:error': ErrorHandlerFn<ConsumeMessage>;
  };

type AckFn<T extends Event = Event> = (
  channel: Channel | undefined,
  message: ConsumeMessage,
  event: T,
) => void;

type NackFn = (channel: Channel | undefined, message: ConsumeMessage) => void;

const ackFn = (channel: Channel | undefined, message: ConsumeMessage) => {
  channel?.ack(message);
};

const buildNackFn = (requeueOnError: boolean): NackFn => {
  return (channel, message) => {
    channel?.nack(message, false, requeueOnError);
  };
};

const noAckFn = () => {};

/**
 * The `AmqpGateway` class provides functionality for handling AMQP events.
 *
 * Emits the `Service` lifecycle events; see `start()` and `stop()`. On
 * success, the gateway-specific event (`gateway:started`, `gateway:stopped`)
 * is emitted first, immediately followed by its `service:` counterpart. On
 * failure, only the matching `service:*Error` event is emitted. It carries an
 * `Error`; a non-`Error` thrown value is wrapped with the original as its
 * `cause`, and the original value is rethrown. Emission is synchronous and
 * listener exceptions are not isolated, so they propagate to the caller.
 */
export class AmqpGateway<
  T extends Event = Event,
  R extends EventHandlerReturnType = unknown,
>
  extends AbstractEventComponent
  implements EventEmittingService<AmqpGatewayEvents<T, R>>
{
  #channelProvider: ChannelProviderFn;
  #consume: Replies.Consume | undefined;
  #consumeOptions: Options.Consume;
  #extractor: MessageEventExtractorFn<T>;
  #handler: EventHandlerFn<T, R>;
  #prefetch: number;
  #queueName: string;
  #queueOptions: Options.AssertQueue;
  #running: boolean = false;

  #ackFn: AckFn<T>;
  #nackFn: NackFn;

  constructor(opts: AmqpGatewayOptions<T, R>) {
    super(opts);

    this.#queueName = opts.queueName;
    this.#queueOptions = opts.queueOptions ?? {};
    this.#consumeOptions = opts.consumeOptions ?? {};
    this.#prefetch = opts.prefetch ?? 0;

    this.#channelProvider = getComponent(opts.channelProvider, 'get', {
      name: 'channelProvider',
      defaultProvider: () => new DefaultChannelProvider(opts),
    });

    this.#extractor = getComponent(opts.eventExtractor, 'extract', {
      name: 'eventExtractor',
      default: defaultEventExtractor<T>,
    });
    this.#handler = getEventHandlerComponent(opts.handler);

    if (this.#consumeOptions.noAck) {
      this.#ackFn = noAckFn;
      this.#nackFn = noAckFn;
    } else {
      this.#ackFn = (channel, message, event) => {
        ackFn(channel, message);
        this.emit('message:acknowledged', channel, message, event);
      };

      const requeueOnError = opts.requeueOnError ?? true;
      this.#nackFn = buildNackFn(requeueOnError);
    }
  }

  /**
   * Asserts the queue and starts consuming from it.
   *
   * Once consuming, emits `gateway:started` with `(channel, consumer)`, then
   * `service:started`.
   *
   * If getting the channel, asserting the queue, setting the prefetch, or
   * consuming fails, emits only `service:startError` with an `Error` and
   * rethrows the original thrown value.
   */
  async start() {
    let channel: Channel;

    try {
      channel = await this.#channelProvider();
      await channel.assertQueue(this.#queueName, this.#queueOptions);
      await channel.prefetch(this.#prefetch);
      this.#consume = await channel.consume(
        this.#queueName,
        this.messageHandler,
        this.#consumeOptions,
      );
    } catch (err) {
      this.emit(SERVICE_START_ERROR, toError(err));
      throw err;
    }

    this.emit('gateway:started', channel, this.#consume);
    this.emit(SERVICE_STARTED);
    this.#running = true;
  }

  /**
   * Cancels the consumer.
   *
   * Once the consumer is cancelled, emits `gateway:stopped` without arguments,
   * then `service:stopped`.
   *
   * If the gateway is not running, returns without emitting any event.
   *
   * If getting the channel or cancelling the consumer fails, emits only
   * `service:stopError` with an `Error` and rethrows the original thrown
   * value.
   */
  async stop() {
    if (!this.#running) {
      return;
    }

    try {
      const channel = await this.#channelProvider();
      if (this.#consume) {
        await channel.cancel(this.#consume.consumerTag);
      }
    } catch (err) {
      this.emit(SERVICE_STOP_ERROR, toError(err));
      throw err;
    }

    this.emit('gateway:stopped');
    this.emit(SERVICE_STOPPED);
    this.#running = false;
  }

  async handleMessage(message: ConsumeMessage | null) {
    if (!message) {
      return;
    }
    let channel: Channel | undefined, event: T | undefined;

    try {
      this.emit('message:received', message);
      event = await this.#extractor(message);
      this.emit(EVENT_RECEIVED, event);
      channel = await this.#channelProvider();

      if (!channel) {
        throw new Error('Channel not available');
      }

      const result = await this.#handler(event);
      this.emit(EVENT_PROCESSED, event, result);
      this.emit('message:processed', message, event, result);
      this.#ackFn(channel, message, event);
    } catch (err) {
      this.#nackFn(channel, message);
      if (event) {
        this.emit(EVENT_ERROR, err, event);
      }
      this.emit('message:error', err, message);
    }
  }

  protected get messageHandler() {
    return this.handleMessage.bind(this);
  }
}
