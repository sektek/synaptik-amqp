import {
  Event,
  EventBasedProvider,
  EventBasedProviderComponent,
} from '@sektek/synaptik';
import { Options } from 'amqplib';

export type PublishOptionsProviderFn<T extends Event = Event> = (
  event: T,
  options?: Options.Publish,
) => Options.Publish | PromiseLike<Options.Publish>;

export interface PublishOptionsProvider<T extends Event = Event>
  extends EventBasedProvider<T, Options.Publish> {}

export type PublishOptionsProviderComponent<T extends Event = Event> =
  EventBasedProviderComponent<T, Options.Publish>;
