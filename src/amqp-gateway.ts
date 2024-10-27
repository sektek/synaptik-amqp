import { Event, EventHandlerEvents, EventHandlerFn } from '@sektek/synaptik';
import { ConsumeMessage } from 'amqplib';

import { AmqpServiceOptions, MessageEventExtractorFn } from './types/index.js';
import { AbstractAmqpService } from './abstract-amqp-service.js';

export type AmqpGatewayOptions<T extends Event = Event> = AmqpServiceOptions & {
  extractor: MessageEventExtractorFn<T>;
  handler: EventHandlerFn<T>;
};

export interface AmqpGatewayEvents<T extends Event = Event>
  extends EventHandlerEvents<T> {
  'message:received': (event: T) => void;
  'message:acknowledged': (event: T) => void;
  'message:error': (event: T, err: Error) => void;
}

export class AmqpGateway<T extends Event = Event> extends AbstractAmqpService {
  #extractor: MessageEventExtractorFn<T>;
  #handler: EventHandlerFn<T>;

  constructor(options: AmqpGatewayOptions<T>) {
    super(options);

    this.#extractor = options.extractor;
    this.#handler = options.handler;
  }

  async start() {
    const channel = await this.channel();
    await channel.consume(this.queue, this.handleMessage.bind(this));
  }

  async handleMessage(message: ConsumeMessage | null) {
    if (!message) {
      return;
    }

    try {
      const event = this.#extractor(message);
      this.emit('event:received', event);

      const result = await this.#handler(event);
      this.emit('event:processed', event, result);
      (await this.channel()).ack(message);
      this.emit('message:acknowledged', event);
    } catch (err) {
      (await this.channel()).nack(message);
      this.emit('message:error', err);
    }
  }
}
