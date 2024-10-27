import { Event } from '@sektek/synaptik';

import { AmqpSerializerFn, AmqpServiceOptions } from './types/index.js';
import { AbstractAmqpService } from './abstract-amqp-service.js';

export type AmqpChannelOptions<T extends Event = Event> = AmqpServiceOptions & {
  serializer: AmqpSerializerFn<T>;
};

export class AmqpChannel<T extends Event = Event> extends AbstractAmqpService {
  #eventSerializer: AmqpSerializerFn<T>;

  constructor(options: AmqpChannelOptions<T>) {
    super(options);

    this.#eventSerializer = options.serializer;
  }

  async send(event: T) {
    this.emit('event:received', event);
    try {
      const channel = await this.channel();
      const serialized = await this.#eventSerializer(event);
      channel.sendToQueue(this.queue, Buffer.from(serialized));
      this.emit('event:delivered', event);
    } catch (err) {
      this.emit('event:error', event, err);
    }
  }
}
