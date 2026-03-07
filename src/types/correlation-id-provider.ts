import {
  Event,
  EventBasedStringProvider,
  EventBasedStringProviderComponent,
  EventBasedStringProviderFn,
} from '@sektek/synaptik';

export type CorrelationIdProviderFn<T extends Event = Event> =
  EventBasedStringProviderFn<T>;

export interface CorrelationIdProvider<
  T extends Event = Event,
> extends EventBasedStringProvider<T> {}

export type CorrelationIdProviderComponent<T extends Event = Event> =
  EventBasedStringProviderComponent<T>;
