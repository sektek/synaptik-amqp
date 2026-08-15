import { ChannelModel } from 'amqplib';
import { EventComponentOptions } from '@sektek/synaptik';

import { ConnectionOptions } from './connection-options.js';

export type AmqpServiceOptions = EventComponentOptions & {
  connection?: ChannelModel;
  connectionOptions?: ConnectionOptions;
};
