import { ConsumeMessage } from 'amqplib';
import { Event } from '@sektek/synaptik';

import { AmqpChannel } from './amqp-channel.js';
import { AmqpGateway } from './amqp-gateway.js';

export type ReplyListenerOptions<T extends Event> = {
  /**
   * The channel used to send the reply message.
   */
  outboundChannel: AmqpChannel<T>;

  /**
   * The gateway that processes the incoming message and emits the 'message:processed' event.
   */
  gateway: AmqpGateway<Event, T>;
};

/**
 * ReplyListener is responsible for sending a reply message when a message is processed.
 * It listens to the 'message:processed' event from the AmqpGateway and sends the reply
 * using the AmqpChannel.
 */
export class ReplyListener<T extends Event = Event> {
  #outboundChannel: AmqpChannel<T>;
  #gateway: AmqpGateway<Event, T>;

  constructor(opts: ReplyListenerOptions<T>) {
    this.#outboundChannel = opts.outboundChannel;
    this.#gateway = opts.gateway;

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
