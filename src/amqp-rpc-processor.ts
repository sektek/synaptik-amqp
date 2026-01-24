import { randomUUID } from 'crypto';

import {
  AbstractEventService,
  EVENT_ERROR,
  EVENT_PROCESSED,
  EVENT_RECEIVED,
  Event,
  EventHandlerEvents,
  EventProcessor,
  PromiseChannel,
} from '@sektek/synaptik';
import { Channel, ConsumeMessage, Options } from 'amqplib';
import {
  ErrorHandlerFn,
  EventEmittingService,
  getComponent,
} from '@sektek/utility-belt';

import {
  AmqpChannel,
  AmqpChannelFn,
  AmqpChannelOptions,
} from './amqp-channel.js';
import {
  AmqpSerializerReturnType,
  ChannelProviderComponent,
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
  channel?: Channel;
  channelProvider?: ChannelProviderComponent<T>;
  outboundChannel?: AmqpChannel<T>;
  extractor?: MessageEventExtractorComponent<R>;
  correlationIdProvider?: CorrelationIdProviderComponent<T>;
  /** Timeout in milliseconds */
  timeout?: number;
};

type AmqpRpcProcessorEvents<
  T extends Event = Event,
  R extends Event = T,
> = EventHandlerEvents<T, R> & {
  'message:received': (message: ConsumeMessage) => void;
  [MESSAGE_SENT_EVENT]: (
    exchange: string,
    routingKey: string,
    options: Options.Publish,
    message: AmqpSerializerReturnType,
  ) => void;
  'message:processed': (message: ConsumeMessage, event: T, result: R) => void;
  'message:error': ErrorHandlerFn<ConsumeMessage>;
};

export class AmqpRpcProcessor<T extends Event = Event, R extends Event = T>
  extends AbstractEventService
  implements
    EventProcessor<T, R>,
    EventEmittingService<AmqpRpcProcessorEvents<T, R>>
{
  #outboundChannel: AmqpChannelFn<T>;
  #channelProvider: ChannelProviderFn<Event>;
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
    this.emit(EVENT_RECEIVED, event);
    const channel = await this.#channelProvider(event);
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
          this.emit(EVENT_PROCESSED, event, result);
        } catch (err) {
          this.emit('message:error', err, message);
          this.emit(EVENT_ERROR, err, event);
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
