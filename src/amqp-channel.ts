import {
  AbstractEventService,
  Event,
  EventChannelEvents,
} from '@sektek/synaptik';
import {
  Component,
  EventEmittingService,
  getComponent,
} from '@sektek/utility-belt';
import { Options } from 'amqplib';

import {
  AmqpSerializerComponent,
  AmqpSerializerFn,
  AmqpSerializerReturnType,
  AmqpServiceOptions,
  ChannelProviderFn,
  PublishOptionsProviderComponent,
  PublishOptionsProviderFn,
  RoutingKeyProviderComponent,
  RoutingKeyProviderFn,
} from './types/index.js';
import { DefaultChannelProvider } from './default-channel-provider.js';

const DEFAULT_EXCHANGE = '';
const DEFAULT_PUBLISH_OPTIONS_PROVIDER = (event: Event, options = {}) => ({
  contentType: 'application/json',
  type: event.type,
  ...options,
});
const DEFAULT_SERIALIZER = (event: Event) => JSON.stringify(event);

type SendOptions = Options.Publish & {
  exchange?: string;
  routingKey?: string;
};

export type AmqpChannelFn<T extends Event = Event> = (
  event: T,
  options?: SendOptions,
) => Promise<void>;

export type AmqpChannelComponent<T extends Event = Event> = Component<
  AmqpChannel<T>,
  'send'
>;

export type AmqpChannelEvents<T extends Event = Event> =
  EventChannelEvents<T> & {
    'message:sent': (
      exchange: string,
      routingKey: string,
      options: Options.Publish,
      message: AmqpSerializerReturnType,
    ) => void;
  };

export type AmqpChannelOptions<T extends Event = Event> = AmqpServiceOptions & {
  exchange?: string;
  publishOptionsProvider?: PublishOptionsProviderComponent<T>;
  queueName?: string;
  routingKey?: string;
  routingKeyProvider?: RoutingKeyProviderComponent<T>;
  serializer?: AmqpSerializerComponent<T>;
  timeout?: number;
};

const DEFAULT_TIMEOUT = 10000;

export class AmqpChannel<T extends Event = Event>
  extends AbstractEventService
  implements EventEmittingService<AmqpChannelEvents<T>>
{
  #channelProvider: ChannelProviderFn;
  #eventSerializer: AmqpSerializerFn<T>;
  #exchange: string;
  #publishOptionsProvider: PublishOptionsProviderFn<T>;
  #routingKeyProvider: RoutingKeyProviderFn<T>;
  #timeout: number;

  constructor(opts: AmqpChannelOptions<T>) {
    super(opts);

    this.#timeout = opts.timeout ?? DEFAULT_TIMEOUT;

    this.#channelProvider = getComponent(
      opts.channelProvider,
      'get',
      new DefaultChannelProvider(opts),
    );

    this.#exchange = opts.exchange ?? DEFAULT_EXCHANGE;

    this.#eventSerializer = getComponent(
      opts.serializer,
      'serialize',
      DEFAULT_SERIALIZER,
    );

    this.#publishOptionsProvider = getComponent(
      opts.publishOptionsProvider,
      'get',
      DEFAULT_PUBLISH_OPTIONS_PROVIDER,
    );

    this.#routingKeyProvider = getComponent(
      opts.routingKeyProvider,
      'get',
      () => opts.routingKey ?? opts.queueName ?? '',
    );
  }

  async send(event: T, options: SendOptions = {}) {
    this.emit('event:received', event);

    try {
      const channel = await this.#channelProvider();
      const serialized = await this.#eventSerializer(event);
      const publishOptions = await this.#publishOptionsProvider(event, options);
      const exchange = options.exchange ?? this.#exchange;
      const routingKey =
        options.routingKey ?? (await this.#routingKeyProvider(event));
      const success = channel.publish(
        exchange,
        routingKey,
        Buffer.from(serialized),
        publishOptions,
      );

      if (!success) {
        await new Promise<void>((resolve, reject) => {
          // eslint-disable-next-line prefer-const, no-undef
          let timeout: NodeJS.Timeout | undefined;
          const onDrain = () => {
            clearTimeout(timeout);
            if (
              channel.publish(
                exchange,
                routingKey,
                Buffer.from(serialized),
                publishOptions,
              )
            ) {
              resolve();
            }

            reject(new Error('Failed to publish message'));
          };

          timeout = setTimeout(() => {
            channel.removeListener('drain', onDrain);
            reject(new Error('Failed to publish message'));
          }, this.#timeout);

          channel.once('drain', onDrain);
        });
      }

      this.emit(
        'message:sent',
        exchange,
        routingKey,
        publishOptions,
        serialized,
      );
      this.emit('event:delivered', event);
    } catch (err) {
      this.emit('event:error', event, err);

      throw err;
    }
  }
}
