import { ConsumeMessage } from 'amqplib';
import { Event } from '@sektek/synaptik';

import { AmqpChannel } from './amqp-channel.js';
import { AmqpGateway } from './amqp-gateway.js';

/**
 * ReplyListener is responsible for sending a reply message when a message is processed.
 * It listens to the 'message:processed' event from the AmqpGateway and sends the reply
 * using the AmqpChannel.
 */
export class ReplyListener<T extends Event = Event> {
  #outboundChannel: AmqpChannel<T>;
  #gateway: AmqpGateway<Event, T>;

  constructor(channel: AmqpChannel<T>, gateway: AmqpGateway<Event, T>) {
    this.#outboundChannel = channel;
    this.#gateway = gateway;

    this.#gateway.on('message:processed', this.messageProcessed.bind(this));
  }

  async messageProcessed(message: ConsumeMessage, _event: Event, result: T) {
    if (message.properties.replyTo) {
      this.#outboundChannel.send(result, {
        correlationId: message.properties.correlationId,
        routingKey: message.properties.replyTo,
      });
    }
  }
}
