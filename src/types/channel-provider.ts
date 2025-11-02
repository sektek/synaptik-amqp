import { Provider, ProviderComponent, ProviderFn } from '@sektek/utility-belt';
import { Channel } from 'amqplib';

export type ChannelProviderFn<T = void> = ProviderFn<Channel, T>;
export interface ChannelProvider<T = void> extends Provider<Channel, T> {}
export type ChannelProviderComponent<T = void> = ProviderComponent<Channel, T>;
