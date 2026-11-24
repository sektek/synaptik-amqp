import EventEmitter from 'events';

import {
  EventEmittingService,
  SERVICE_STOPPED,
  SERVICE_STOP_ERROR,
  StoppableEvents,
  isStoppable,
  toError,
} from '@sektek/utility-belt';
import { Channel } from 'amqplib';

import { ConnectionProvider } from './types/index.js';

import {
  DefaultConnectionProvider,
  DefaultConnectionProviderOptions,
} from './default-connection-provider.js';

type ChannelProviderOptions<T = void> = DefaultConnectionProviderOptions & {
  channel?: Channel;
  connectionProvider?: ConnectionProvider<T>;
};

/** Events emitted by a {@link DefaultChannelProvider}. */
export type DefaultChannelProviderEvents = StoppableEvents & {
  'channel:created': (channel: Channel) => void;
  'channel:closed': () => void;
  'channel:error': (error: unknown) => void;
};

/**
 * Provides a single shared AMQP channel, creating it on first use.
 *
 * **Lifecycle events** — `stop()` emits `service:stopped` once the channel is
 * closed and any stop propagated to the connection provider has completed. If
 * stopping fails, it emits `service:stopError` with an `Error` and rethrows the
 * original thrown value. The connection provider's own lifecycle events are
 * not re-emitted by this provider.
 */
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

  /**
   * Closes the channel it created and, if it created its own connection
   * provider, stops that too.
   *
   * Emits `service:stopped` on success, or `service:stopError` and rethrows.
   */
  async stop() {
    try {
      await this.#stop();
    } catch (err) {
      this.emit(SERVICE_STOP_ERROR, toError(err));
      throw err;
    }

    this.emit(SERVICE_STOPPED);
  }

  async #stop() {
    try {
      if (this.#closeChannelOnStop) {
        await this.#channel?.close();
      }
    } catch (error) {
      this.emit('channel:error', error);
    } finally {
      this.#channel = undefined;
    }
    try {
      if (this.#propegateStop && isStoppable(this.#connectionProvider)) {
        await this.#connectionProvider.stop();
      }
    } finally {
      this.#connectionProvider = undefined;
    }
  }
}
