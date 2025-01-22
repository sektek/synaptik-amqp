import { ConsumeMessage } from 'amqplib';
import { Event } from '@sektek/synaptik';

import { AmqpChannel } from './amqp-channel.js';
import { AmqpGateway } from './amqp-gateway.js';

export class ReplyListener<T extends Event = Event> {
  #channel: AmqpChannel<T>;
  #gateway: AmqpGateway<Event, T>;

  constructor(channel: AmqpChannel<T>, gateway: AmqpGateway<Event, T>) {
    this.#channel = channel;
    this.#gateway = gateway;

    this.#gateway.on('message:processed', this.messageProcessed.bind(this));
  }

  async messageProcessed(message: ConsumeMessage, _event: Event, result: T) {
    if (message.properties.replyTo) {
      this.#channel.send(result, {
        correlationId: message.properties.correlationId,
      });
    }
  }
}
