import { randomUUID } from 'crypto';

import { Event, EventProcessor, PromiseChannel } from '@sektek/synaptik';
import { getComponent } from '@sektek/utility-belt';

import {
  AmqpServiceOptions,
  CorrelationIdProviderComponent,
  CorrelationIdProviderFn,
  MessageEventExtractorComponent,
  MessageEventExtractorFn,
} from './types/index.js';
import { AbstractAmqpService } from './abstract-amqp-service.js';
import { AmqpChannel } from './amqp-channel.js';
import { defaultEventExtractor } from './default-event-extractor.js';

type AmqpProcessorOptions<
  T extends Event = Event,
  R extends Event = T,
> = AmqpServiceOptions & {
  amqpChannel: AmqpChannel<T>;
  extractor?: MessageEventExtractorComponent<R>;
  correlationIdProvider?: CorrelationIdProviderComponent<T>;
  /** Timeout in milliseconds */
  timeout?: number;
};

export class AmqpProcessor<T extends Event = Event, R extends Event = T>
  extends AbstractAmqpService
  implements EventProcessor<T, R>
{
  #amqpChannel: AmqpChannel<T>;
  #correlationIdProvider: CorrelationIdProviderFn<T>;
  #extractor: MessageEventExtractorFn<R>;
  #timeout: number;

  constructor(opts: AmqpProcessorOptions<T, R>) {
    super(opts);

    this.#amqpChannel = opts.amqpChannel;
    this.#timeout = opts.timeout ?? 0;
    this.#correlationIdProvider = getComponent(
      opts.correlationIdProvider,
      'get',
      () => randomUUID(),
    );

    this.#extractor = getComponent(
      opts.extractor,
      'get',
      defaultEventExtractor<R>,
    );
  }

  async process(event: T): Promise<R> {
    const channel = await this.channel();
    const replyQueue = await channel.assertQueue('', { exclusive: true });
    const promiseChannel = new PromiseChannel<R>({ timeout: this.#timeout });
    const correlationId = await this.#correlationIdProvider(event);

    const consumer = await channel.consume(
      replyQueue.queue,
      async message => {
        if (message && message.properties.correlationId === correlationId) {
          promiseChannel.send(await this.#extractor(message));
          await channel.cancel(consumer.consumerTag);
        }
      },
      { noAck: true, exclusive: true },
    );

    if (this.#timeout > 0) {
      setTimeout(async () => {
        await channel.cancel(consumer.consumerTag);
        promiseChannel.sendError(new Error('Timeout'));
      }, this.#timeout);
    }


    await this.#amqpChannel.send(event, {
      correlationId,
      replyTo: replyQueue.queue,
    });

    return promiseChannel.get();
  }
}
