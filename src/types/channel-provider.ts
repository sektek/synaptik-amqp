import { Provider, ProviderComponent, ProviderFn } from '@sektek/utility-belt';
import { Channel } from 'amqplib';

export type ChannelProviderFn<T = unknown> = ProviderFn<Channel, T>;
export interface ChannelProvider<T = unknown> extends Provider<Channel, T> {}
export type ChannelProviderComponent<T = unknown> = ProviderComponent<
  Channel,
  T
>;
