import { Channel, Connection } from 'amqplib';
import { AbstractEventService } from '@sektek/synaptik';

import { AmqpServiceOptions, ConnectionOptions } from './types/index.js';
import { createConnection } from './create-connection.js';

export class AbstractAmqpService extends AbstractEventService {
  #channel?: Channel;
  #connection?: Connection;
  #connectionOptions?: ConnectionOptions;
  #queue: string;

  constructor(opts: AmqpServiceOptions) {
    super(opts);

    this.#connection = opts.connection;
    this.#queue = opts.queue;
  }

  protected async channel() {
    if (!this.#channel) {
      this.#channel = await (await this.connection()).createChannel();
      this.#channel.on('close', () => {
        this.#channel = undefined;
      });
      await this.#channel.assertQueue(this.#queue, { durable: true });
    }
    return this.#channel;
  }

  protected async connection(): Promise<Connection> {
    if (this.#connection) {
      return this.#connection;
    }

    if (!this.#connectionOptions) {
      throw new Error('Connection options not set');
    }

    this.#connection = await createConnection(this.#connectionOptions);
    this.#connection.on('close', () => {
      this.#connection = undefined;
    });

    return this.#connection;
  }

  get queue() {
    return this.#queue;
  }
}
