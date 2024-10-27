import { Connection, Options } from 'amqplib';
import { EventServiceOptions } from '@sektek/synaptik';

import { ConnectionOptions } from './connection-options.js';

export interface AmqpServiceOptions
  extends EventServiceOptions,
    Options.Connect {
  connection?: Connection;
  connectionOptions?: ConnectionOptions;
  queue: string;
}
