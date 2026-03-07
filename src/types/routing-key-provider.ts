import {
  Event,
  EventBasedStringProvider,
  EventBasedStringProviderComponent,
  EventBasedStringProviderFn,
} from '@sektek/synaptik';

export type RoutingKeyProviderFn<T extends Event = Event> =
  EventBasedStringProviderFn<T>;

export interface RoutingKeyProvider<
  T extends Event = Event,
> extends EventBasedStringProvider<T> {}

export type RoutingKeyProviderComponent<T extends Event = Event> =
  EventBasedStringProviderComponent<T>;
