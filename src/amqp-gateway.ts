import {
  AbstractEventService,
  Event,
  EventHandlerEvents,
  EventHandlerFn,
  EventHandlerReturnType,
  getEventHandlerComponent,
} from '@sektek/synaptik';
import { Channel, ConsumeMessage, Options, Replies } from 'amqplib';
import { EventEmittingService, getComponent } from '@sektek/utility-belt';

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

export type AmqpGatewayEvents<
  T extends Event = Event,
  R extends EventHandlerReturnType = unknown,
> = EventHandlerEvents<T> & {
  'gateway:started': (channel: Channel, consumer: Replies.Consume) => void;
  'gateway:stopped': (channel: Channel, consumer: Replies.Consume) => void;
  'message:received': (message: ConsumeMessage) => void;
  'message:processed': (message: ConsumeMessage, event: T, result: R) => void;
  'message:acknowledged': (
    channel: Channel,
    message: ConsumeMessage,
    event: T,
  ) => void;
  'message:error': (event: ConsumeMessage, err: Error) => void;
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
 */
export class AmqpGateway<
    T extends Event = Event,
    R extends EventHandlerReturnType = unknown,
  >
  extends AbstractEventService
  implements EventEmittingService<AmqpGatewayEvents<T, R>>
{
  #channelProvider: ChannelProviderFn;
  #consumeOptions: Options.Consume;
  #extractor: MessageEventExtractorFn<T>;
  #handler: EventHandlerFn<T, R>;
  #prefetch: number;
  #queueName: string;
  #queueOptions: Options.AssertQueue;
  #ackFn: AckFn<T>;
  #nackFn: NackFn;
  #consume: Replies.Consume | undefined;

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

  async start() {
    const channel = await this.#channelProvider();
    await channel.assertQueue(this.#queueName, this.#queueOptions);
    await channel.prefetch(this.#prefetch);
    this.#consume = await channel.consume(
      this.#queueName,
      this.messageHandler,
      this.#consumeOptions,
    );

    this.emit('gateway:started', channel, this.#consume);
  }

  async stop() {
    const channel = await this.#channelProvider();
    if (this.#consume) {
      await channel.cancel(this.#consume.consumerTag);
    }
    this.emit('gateway:stopped');
  }

  async handleMessage(message: ConsumeMessage | null) {
    if (!message) {
      return;
    }
    let channel: Channel | undefined, event: T | undefined;

    try {
      this.emit('message:received', message);
      event = await this.#extractor(message);
      this.emit('event:received', event);
      channel = await this.#channelProvider();

      if (!channel) {
        throw new Error('Channel not available');
      }

      const result = await this.#handler(event);
      this.emit('event:processed', event, result);
      this.emit('message:processed', message, event, result);
      this.#ackFn(channel, message, event);
    } catch (err) {
      this.#nackFn(channel, message);
      if (event) {
        this.emit('event:error', event, err);
      }
      this.emit('message:error', message, err);
    }
  }

  protected get messageHandler() {
    return this.handleMessage.bind(this);
  }
}
