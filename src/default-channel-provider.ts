import EventEmitter from 'events';

import { Channel, ChannelModel, connect } from 'amqplib';
import { EventEmittingService } from '@sektek/utility-belt';

import { ConnectionOptions, ConnectionProvider } from './types/index.js';

import {
  DefaultConnectionProvider,
  DefaultConnectionProviderOptions,
} from './default-connection-provider.js';

type ChannelProviderOptions<T = void> = DefaultConnectionProviderOptions & {
  channel?: Channel;
  connectionProvider?: ConnectionProvider<T>;
};

type DefaultChannelProviderEvents = {
  'channel:created': (channel: Channel) => void;
  'channel:closed': () => void;
  'channel:error': (error: unknown) => void;
};

export class DefaultChannelProvider<T = void>
  extends EventEmitter
  implements EventEmittingService<DefaultChannelProviderEvents>
{
  #channel?: Channel;
  #closeChannelOnStop = true;
  #connectionProvider?: ConnectionProvider<T>;
  #propegateStop = true;

  constructor(opts: ChannelProviderOptions<T>) {
    super();

    if (opts.channel) {
      this.#channel = opts.channel;
      this.#closeChannelOnStop = false;
      this.#propegateStop = false;
    } else if (opts.connectionProvider) {
      this.#connectionProvider = opts.connectionProvider;
      this.#propegateStop = false;
    } else {
      this.#connectionProvider = new DefaultConnectionProvider(opts);
    }
  }

  async get(arg: T): Promise<Channel> {
    if (!this.#channel) {
      const connection = await this.#connectionProvider!.get(arg);
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

  async stop() {
    try {
      if (this.#closeChannelOnStop) {
        await this.#channel?.close();
      }
    } finally {
      this.#channel = undefined;
    }
    try {
      if (this.#propegateStop) {
        await this.#connectionProvider?.stop();
      }
    } finally {
      this.#connectionProvider = undefined;
    }
  }
}
