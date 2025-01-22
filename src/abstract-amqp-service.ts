import { Channel, Connection } from 'amqplib';
import { AbstractEventService } from '@sektek/synaptik';

import { AmqpServiceOptions, ConnectionOptions } from './types/index.js';
import { createConnection } from './create-connection.js';

export class AbstractAmqpService extends AbstractEventService {
  #channel?: Channel;
  #connection?: Connection;
  #connectionOptions?: ConnectionOptions;

  constructor(opts: AmqpServiceOptions) {
    super(opts);

    this.#connection = opts.connection;
  }

  protected async channel() {
    if (!this.#channel) {
      this.#channel = await (await this.connection()).createChannel();
      this.#channel.on('close', () => {
        this.#channel = undefined;
      });
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
}
