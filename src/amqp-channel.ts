import { Event, EventChannelEvents } from '@sektek/synaptik';
import { EventEmittingService, getComponent } from '@sektek/utility-belt';

import { Options } from 'amqplib';

import {
  AmqpSerializerComponent,
  AmqpSerializerFn,
  AmqpSerializerReturnType,
  AmqpServiceOptions,
  PublishOptionsProviderComponent,
  PublishOptionsProviderFn,
  RoutingKeyProviderComponent,
  RoutingKeyProviderFn,
} from './types/index.js';
import { AbstractAmqpService } from './abstract-amqp-service.js';

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
};

export class AmqpChannel<T extends Event = Event>
  extends AbstractAmqpService
  implements EventEmittingService<AmqpChannelEvents<T>>
{
  #eventSerializer: AmqpSerializerFn<T>;
  #exchange: string;
  #publishOptionsProvider: PublishOptionsProviderFn<T>;
  #routingKeyProvider: RoutingKeyProviderFn<T>;

  constructor(opts: AmqpChannelOptions<T>) {
    super(opts);

    if (!opts.routingKeyProvider && !opts.routingKey && !opts.queueName) {
      throw new Error('Queue name or routing key provider required');
    }

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
      () => opts.routingKey ?? opts.queueName,
    );
  }

  async send(event: T, options: SendOptions = {}) {
    this.emit('event:received', event);

    try {
      const channel = await this.channel();
      const serialized = await this.#eventSerializer(event);
      const publishOptions = await this.#publishOptionsProvider(event, options);
      const exchange = options.exchange ?? this.#exchange;
      const routingKey =
        options.routingKey ?? (await this.#routingKeyProvider(event));
      channel.publish(
        exchange,
        routingKey,
        Buffer.from(serialized),
        publishOptions,
      );
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
    }
  }
}
