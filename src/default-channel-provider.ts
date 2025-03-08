import EventEmitter from 'events';

import { Channel, Connection, connect } from 'amqplib';

import { ConnectionOptions } from './types/connection-options.js';
import { EventEmittingService } from '@sektek/utility-belt';

type ChannelProviderOptions = {
  channel?: Channel;
  connection?: Connection;
  connectionOptions?: ConnectionOptions;
};

type DefaultChannelProviderEvents = {
  'channel:created': (channel: Channel) => void;
  'channel:closed': () => void;
  'channel:error': (error: unknown) => void;
  'connection:created': (connection: Connection) => void;
  'connection:closed': () => void;
  'connection:error': (error: unknown) => void;
};

export class DefaultChannelProvider
  extends EventEmitter
  implements EventEmittingService<DefaultChannelProviderEvents>
{
  #channel?: Channel;
  #connection?: Connection;
  #connectionOptions?: ConnectionOptions;

  constructor(opts: ChannelProviderOptions) {
    super();

    this.#channel = opts.channel;
    this.#connection = opts.connection;
    this.#connectionOptions = opts.connectionOptions;
  }

  async get(): Promise<Channel> {
    if (!this.#channel) {
      const connection = await this.connection();
      this.#channel = await connection.createChannel();
      this.emit('channel:created', this.#channel);
      this.#channel.on('error', error => {
        this.emit('channel:error', error);
      });
      this.#channel.on('close', () => {
        this.#channel = undefined;
        this.emit('channel:closed');
      });
    }

    return this.#channel;
  }

  async connection(): Promise<Connection> {
    if (this.#connection) {
      return this.#connection;
    }

    if (!this.#connectionOptions) {
      throw new Error('Connection options not set');
    }

    this.#connection = await connect(
      this.#connectionOptions?.url ?? this.#connectionOptions,
    );
    this.emit('connection:created', this.#connection);
    this.#connection.on('error', error => {
      this.emit('connection:error', error);
    });
    this.#connection.on('close', () => {
      this.#connection = undefined;
      this.emit('connection:closed');
    });

    return this.#connection;
  }
}
