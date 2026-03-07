import { ChannelModel } from 'amqplib';
import { EventServiceOptions } from '@sektek/synaptik';

import { ConnectionOptions } from './connection-options.js';

export type AmqpServiceOptions = EventServiceOptions & {
  connection?: ChannelModel;
  connectionOptions?: ConnectionOptions;
};
