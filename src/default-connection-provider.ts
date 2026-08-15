import EventEmitter from 'events';

import { ChannelModel, connect } from 'amqplib';
import { EventEmittingService, ProcessManager } from '@sektek/utility-belt';

import { ConnectionOptions, ConnectionProvider } from './types/index.js';

type DefaultConnectionProviderEvents = {
  'connection:created': (connection: ChannelModel) => void;
  'connection:closed': () => void;
  'connection:error': (error: unknown) => void;
};

export type DefaultConnectionProviderOptions = {
  connection?: ChannelModel;
  connectionOptions?: ConnectionOptions;
  processManager?: ProcessManager;
};

export class DefaultConnectionProvider
  extends EventEmitter
  implements
    ConnectionProvider,
    EventEmittingService<DefaultConnectionProviderEvents>
{
  #connection: ChannelModel | undefined;
  #connectionErrorHandler: ((error: unknown) => void) | undefined;
  #connectionCloseHandler: (() => void) | undefined;
  #connectionOptions: ConnectionOptions | undefined;
  #running = true;
  #closeConnectionOnStop = true;
  #ownProcessManager: ProcessManager | undefined;

  constructor(opts: DefaultConnectionProviderOptions) {
    super();

    if (!(opts.connection || opts.connectionOptions)) {
      throw new Error(
        'Either connection or connectionOptions must be provided',
      );
    }
    this.#connection = opts.connection;
    this.#connectionOptions = opts.connectionOptions;
    if (opts.connection) {
      this.#closeConnectionOnStop = false;
    }

    if (opts.processManager) {
      opts.processManager.add(this);
    } else {
      this.#ownProcessManager = new ProcessManager({
        name: 'DefaultConnectionProviderProcessManager',
      });
      this.#ownProcessManager.add(this);
    }
  }

  async get(): Promise<ChannelModel> {
    if (!this.#running) {
      throw new Error('Connection provider is stopped');
    }

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
    this.#connectionErrorHandler = (error: unknown) => {
      this.emit('connection:error', error);
    };
    this.#connection.on('error', this.#connectionErrorHandler);
    this.#connectionCloseHandler = () => {
      this.#connection?.removeListener('error', this.#connectionErrorHandler!);
      this.#connectionErrorHandler = undefined;

      this.#connection?.removeListener('close', this.#connectionCloseHandler!);
      this.#connectionCloseHandler = undefined;

      this.#connection = undefined;
      this.emit('connection:closed');
    };
    this.#connection.on('close', this.#connectionCloseHandler);

    return this.#connection;
  }

  async stop(): Promise<void> {
    this.#running = false;

    try {
      if (this.#closeConnectionOnStop) {
        await this.#connection?.close();
      }
      if (this.#connectionErrorHandler) {
        this.#connection?.removeListener('error', this.#connectionErrorHandler);
      }
      if (this.#connectionCloseHandler) {
        this.#connection?.removeListener('close', this.#connectionCloseHandler);
      }
    } catch (error) {
      this.emit('connection:error', error);
    } finally {
      this.#connection = undefined;
      this.#connectionErrorHandler = undefined;
      this.#connectionCloseHandler = undefined;
      this.#ownProcessManager?.remove(this);
      this.#ownProcessManager = undefined;
    }
  }
}
