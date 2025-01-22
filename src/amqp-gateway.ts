import { Channel, ConsumeMessage, Options } from 'amqplib';
import {
  Event,
  EventHandlerEvents,
  EventHandlerFn,
  EventHandlerReturnType,
  getEventHandlerComponent,
} from '@sektek/synaptik';
import { EventEmittingService, getComponent } from '@sektek/utility-belt';

import { AmqpServiceOptions, MessageEventExtractorFn } from './types/index.js';
import { AbstractAmqpService } from './abstract-amqp-service.js';
import { AmqpChannel } from './amqp-channel.js';
import { defaultEventExtractor } from './default-event-extractor.js';

type ReplyChannel<T> = T extends Event ? AmqpChannel<T> : never;

export type AmqpGatewayOptions<
  T extends Event = Event,
  R extends EventHandlerReturnType = unknown,
> = AmqpServiceOptions & {
  consumeOptions?: Options.Consume;
  extractor?: MessageEventExtractorFn<T>;
  handler: EventHandlerFn<T, R>;
  noAck?: boolean;
  prefetch?: number;
  queueName: string;
  queueOptions?: Options.AssertQueue;
  replyChannel?: ReplyChannel<R>;
};

export type AmqpGatewayEvents<
  T extends Event = Event,
  R extends EventHandlerReturnType = unknown,
> = EventHandlerEvents<T> & {
  'message:received': (message: ConsumeMessage) => void;
  'message:processed': (message: ConsumeMessage, event: T, result: R) => void;
  'message:acknowledged': (message: ConsumeMessage, event: T) => void;
  'message:error': (event: ConsumeMessage, err: Error) => void;
};

type AckFn = (channel: Channel | undefined, message: ConsumeMessage) => void;

const ackFn = (channel: Channel | undefined, message: ConsumeMessage) => {
  channel?.ack(message);
};

const nackFn = (channel: Channel | undefined, message: ConsumeMessage) => {
  channel?.nack(message);
};

const noAckFn = () => {};

export class AmqpGateway<
    T extends Event = Event,
    R extends EventHandlerReturnType = unknown,
  >
  extends AbstractAmqpService
  implements EventEmittingService<AmqpGatewayEvents<T, R>>
{
  #consumeOptions: Options.Consume;
  #extractor: MessageEventExtractorFn<T>;
  #handler: EventHandlerFn<T, R>;
  #prefetch: number;
  #queueName: string;
  #queueOptions: Options.AssertQueue;
  #replyChannel: ReplyChannel<R> | undefined;
  #ackFn: AckFn;
  #nackFn: AckFn;

  constructor(opts: AmqpGatewayOptions<T, R>) {
    super(opts);

    this.#queueName = opts.queueName;
    this.#queueOptions = opts.queueOptions ?? {};
    this.#consumeOptions = opts.consumeOptions ?? {};
    this.#prefetch = opts.prefetch ?? 0;

    this.#extractor = getComponent(
      opts.extractor,
      'extract',
      defaultEventExtractor<T>,
    );
    this.#handler = getEventHandlerComponent(opts.handler);

    if (this.#consumeOptions.noAck) {
      this.#ackFn = noAckFn;
      this.#nackFn = noAckFn;
    } else {
      this.#ackFn = ackFn;
      this.#nackFn = nackFn;
    }

    this.#replyChannel = opts.replyChannel;
  }

  async start() {
    const channel = await this.channel();
    await channel.assertQueue(this.#queueName, this.#queueOptions);
    await channel.prefetch(this.#prefetch);
    await channel.consume(
      this.#queueName,
      this.messageHandler,
      this.#consumeOptions,
    );
  }

  async handleMessage(message: ConsumeMessage | null) {
    if (!message) {
      return;
    }
    let channel, event;

    try {
      this.emit('message:received', message);
      event = await this.#extractor(message);
      this.emit('event:received', event);
      channel = await this.channel();

      if (!channel) {
        throw new Error('Channel not available');
      }

      const result = await this.#handler(event);
      this.emit('event:processed', event, result);
      this.emit('message:processed', message, event, result);
      this.#ackFn(channel, message);
      this.emit('message:acknowledged', message, event);
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
