import { Component } from '@sektek/utility-belt';

export type AmqpSerializerReturnType = Uint8Array | readonly number[] | string;

export type AmqpSerializerFn<T> = (
  event: T,
) => AmqpSerializerReturnType | PromiseLike<AmqpSerializerReturnType>;

export interface AmqpSerializer<T> {
  serialize: AmqpSerializerFn<T>;
}

export type AmqpSerializerComponent<T> = Component<
  AmqpSerializer<T>,
  'serialize'
>;
