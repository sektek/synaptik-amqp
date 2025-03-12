import { Channel, Connection } from 'amqplib';
import { EventServiceOptions } from '@sektek/synaptik';

import { ChannelProviderComponent } from './channel-provider.js';
import { ConnectionOptions } from './connection-options.js';

export type AmqpServiceOptions = EventServiceOptions & {
  channel?: Channel;
  channelProvider?: ChannelProviderComponent;
  connection?: Connection;
  connectionOptions?: ConnectionOptions;
};
