import { Provider, ProviderComponent, ProviderFn } from '@sektek/utility-belt';
import { Channel } from 'amqplib';

export type ChannelProviderFn = ProviderFn<Channel>;
export interface ChannelProvider extends Provider<Channel> {}
export type ChannelProviderComponent = ProviderComponent<Channel>;
