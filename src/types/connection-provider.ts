import { ChannelModel } from 'amqplib';
import { Provider } from '@sektek/utility-belt';

export interface ConnectionProvider<T = void> extends Provider<
  ChannelModel,
  T
> {}
