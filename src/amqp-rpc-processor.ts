import { randomUUID } from 'crypto';

import {
  AbstractEventService,
  Event,
  EventProcessor,
  PromiseChannel,
} from '@sektek/synaptik';
import { ConsumeMessage, Options } from 'amqplib';
import { EventEmittingService, getComponent } from '@sektek/utility-belt';

import {
  AmqpChannel,
  AmqpChannelFn,
  AmqpChannelOptions,
} from './amqp-channel.js';
import {
  AmqpSerializerReturnType,
  ChannelProviderFn,
  CorrelationIdProviderComponent,
  CorrelationIdProviderFn,
  MessageEventExtractorComponent,
  MessageEventExtractorFn,
} from './types/index.js';
import { DefaultChannelProvider } from './default-channel-provider.js';
import { defaultEventExtractor } from './default-event-extractor.js';

const MESSAGE_SENT_EVENT = 'message:sent';

type AmqpProcessorOptions<
  T extends Event = Event,
  R extends Event = T,
> = AmqpChannelOptions & {
  outboundChannel?: AmqpChannel<T>;
  extractor?: MessageEventExtractorComponent<R>;
  correlationIdProvider?: CorrelationIdProviderComponent<T>;
  /** Timeout in milliseconds */
  timeout?: number;
};

type AmqpRpcProcessorEvents<T extends Event = Event, R extends Event = T> = {
  'event:error': (event: T, err: Error) => void;
  'event:processed': (event: T, result: R) => void;
  'event:received': (event: T) => void;
  'message:received': (message: ConsumeMessage) => void;
  [MESSAGE_SENT_EVENT]: (
    exchange: string,
    routingKey: string,
    options: Options.Publish,
    message: AmqpSerializerReturnType,
  ) => void;
  'message:processed': (message: ConsumeMessage, event: T, result: R) => void;
  'message:error': (event: ConsumeMessage, err: Error) => void;
};

export class AmqpRpcProcessor<T extends Event = Event, R extends Event = T>
  extends AbstractEventService
  implements
    EventProcessor<T, R>,
    EventEmittingService<AmqpRpcProcessorEvents<T, R>>
{
  #outboundChannel: AmqpChannelFn<T>;
  #channelProvider: ChannelProviderFn;
  #correlationIdProvider: CorrelationIdProviderFn<T>;
  #extractor: MessageEventExtractorFn<R>;
  #timeout: number;

  constructor(opts: AmqpProcessorOptions<T, R>) {
    super(opts);

    const outboundChannel = opts.outboundChannel ?? new AmqpChannel(opts);
    outboundChannel.on(
      MESSAGE_SENT_EVENT,
      (
        exchange: string,
        routingKey: string,
        options: Options.Publish,
        message: AmqpSerializerReturnType,
      ) => {
        this.emit(MESSAGE_SENT_EVENT, exchange, routingKey, options, message);
      },
    );

    this.#outboundChannel = outboundChannel.send.bind(outboundChannel);

    this.#channelProvider = getComponent(opts.channelProvider, 'get', {
      name: 'channelProvider',
      defaultProvider: () => new DefaultChannelProvider(opts),
    });
    this.#timeout = opts.timeout ?? 0;
    this.#correlationIdProvider = getComponent(
      opts.correlationIdProvider,
      'get',
      {
        name: 'correlationIdProvider',
        default: () => randomUUID(),
      },
    );

    this.#extractor = getComponent(opts.extractor, 'get', {
      name: 'extractor',
      default: defaultEventExtractor<R>,
    });
  }

  async process(event: T): Promise<R> {
    this.emit('event:received', event);
    const channel = await this.#channelProvider();
    const replyQueue = await channel.assertQueue('', { exclusive: true });
    const promiseChannel = new PromiseChannel<R>({ timeout: this.#timeout });
    const correlationId = await this.#correlationIdProvider(event);

    const consumer = await channel.consume(
      replyQueue.queue,
      async message => {
        if (!message) {
          return;
        }
        try {
          this.emit('message:received', message);
          if (message.properties.correlationId !== correlationId) {
            throw new Error(
              `Expected correlationId ${correlationId} but got ${message.properties.correlationId}`,
            );
          }
          const result = await this.#extractor(message);
          promiseChannel.send(result);
          this.emit('message:processed', message, event, result);
          this.emit('event:processed', event, result);
        } catch (err) {
          this.emit('message:error', message, err);
          this.emit('event:error', event, err);
          promiseChannel.send(err);
        } finally {
          await channel.cancel(consumer.consumerTag);
          await channel.deleteQueue(replyQueue.queue);
        }
      },
      { noAck: true, exclusive: true },
    );

    if (this.#timeout > 0) {
      setTimeout(async () => {
        await channel.cancel(consumer.consumerTag);
        await channel.deleteQueue(replyQueue.queue);
      }, this.#timeout);
    }

    await this.#outboundChannel(event, {
      correlationId,
      replyTo: replyQueue.queue,
    });

    return promiseChannel.get();
  }
}
