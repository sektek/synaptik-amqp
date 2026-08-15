import {
  Event,
  EventBasedProvider,
  EventBasedProviderComponent,
} from '@sektek/synaptik';
import { Options } from 'amqplib';

import { AmqpChannelSendOptions } from './amqp-channel-send-options.js';

export type PublishOptionsProviderFn<T extends Event = Event> = (
  event: T,
  options?: AmqpChannelSendOptions,
) => Options.Publish | PromiseLike<Options.Publish>;

export interface PublishOptionsProvider<
  T extends Event = Event,
> extends EventBasedProvider<Options.Publish, T> {}

export type PublishOptionsProviderComponent<T extends Event = Event> =
  EventBasedProviderComponent<Options.Publish, T>;
